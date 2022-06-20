# This program, via the use of dictionaries, automates migrating tables, along with
# their constraints and data, between schemas in the same PostgreSQL database or
# between schemas in different PostgreSQL databases including AWS PostgreSQL databases.

import sys
import os
import pandas as pd
import datetime
import time
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
from timeit import default_timer as timer
from datetime import timedelta

YES = 'Y'
NO = 'N'
CREATE_TABLE = 'CREATE_TABLE'
DROP_TABLE = 'DROP_TABLE'
BULK_INSERT_AMT = 200000

# Main function for migrating data between PostgreSQL databases.
def psgres_to_psgres(migration_info,src_db_info,dst_db_info):
    if confirm_migration_params_set(migration_info) == NO:
        sys.exit()
    if confirm_src_db_params_set(src_db_info) == NO:
        sys.exit()
    if confirm_dst_db_params_set(migration_info,dst_db_info) == NO:
        sys.exit()
    if confirm_src_db_params_valid(src_db_info) == NO:
        sys.exit()
    if confirm_dst_db_params_valid(dst_db_info) == NO:
        sys.exit()
    if migration_info['extract_csv_dir']:
        create_csv_files(migration_info,src_db_info)
    if migration_info['create_tables_only'] == YES or \
       migration_info['create_tables_insert_data'] == YES:
        perform_migration(migration_info,src_db_info,dst_db_info) 

# Confirm migration parameters are set.
def confirm_migration_params_set(migration_info):
    if migration_info['extract_csv_dir']:
        dir_path = migration_info['extract_csv_dir']
        if not os.path.isdir(dir_path):
            print(f"'{dir_path}' is not a valid directory path.")
            return NO
    if migration_info['create_tables_only'] not in [YES,NO]:
        print("Set 'create_tables_only' parameter to 'Y' or 'N'.")
        return NO
    if migration_info['create_tables_insert_data'] not in [YES,NO]:
        print("Set 'create_tables_insert_data' parameter to 'Y' or 'N'.")
        return NO
    if migration_info['create_tables_only'] == YES and \
       migration_info['create_tables_insert_data'] == YES:
        print("For the 'create_tables_only' and 'create_tables_insert_data' parameters set one to 'Y' and the other to 'N'.")
        return NO
    if len(migration_info['extract_csv_dir']) == 0 and \
       migration_info['create_tables_only'] == NO and \
       migration_info['create_tables_insert_data'] == NO:
        print("The migration settings won't produce any results.")
        return NO
    else:
        return YES

# Confirm source database parameters are set.
def confirm_src_db_params_set(src_db_info):
    params_unset = {key for (key,val) in src_db_info.items() if len(val.strip()) == 0}
    if params_unset:
        print("The following source database parameter(s) need to be set: " + 
              ", ".join("'{x}'".format(x=param) for param in params_unset) + ".")
        return NO
    else:
        return YES

# Confirm destination database parameters are set.
def confirm_dst_db_params_set(migration_info,dst_db_info):
    params_unset = {key for (key,val) in dst_db_info.items() if len(val.strip()) == 0}
    if params_unset and \
      (migration_info['create_tables_only'] == YES or \
       migration_info['create_tables_insert_data'] == YES):
        print("Setting 'create_tables_only' or 'create_tables_insert_data' parameters requires all destination database parameters to be set.")
        return NO
    params_set = {key for (key,val) in dst_db_info.items() if len(val.strip()) > 0}
    if params_set and \
       migration_info['extract_csv_dir'] and \
       migration_info['create_tables_only'] == NO and \
       migration_info['create_tables_insert_data'] == NO:
        print("Set all destination database parameters to '' if you only want to extract table data to CSV files.")
        return NO
    else:
        return YES

# Test source database parameters.
def confirm_src_db_params_valid(src_db_info):
    try:
        src_db_conn = psycopg2.connect(user=src_db_info['user'],
                                       password=src_db_info['pwd'],
                                       host=src_db_info['host'],
                                       port=src_db_info['port'],
                                       database=src_db_info['database'])
    except:
        print('Connection to source database failed. Check parameter settings or database status.')
        return NO        
    else:
        cursor = src_db_conn.cursor()
        sqlqry = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{src_db_info['schema']}'"
        cursor.execute(sqlqry)
        schemaname = cursor.fetchone()
        cursor.close()
        src_db_conn.close()
        if schemaname[0] == src_db_info['schema']:
            return YES
        else:
            print(f"Schema '{src_db_info['schema']}' doesn't exist in the source database.")
            return NO

# Test destination database parameters.
def confirm_dst_db_params_valid(dst_db_info):
    if len({key for (key,val) in dst_db_info.items() if len(val.strip()) > 0}) == len(dst_db_info):
        try:
            dst_db_conn = psycopg2.connect(user=dst_db_info['user'],
                                           password=dst_db_info['pwd'],
                                           host=dst_db_info['host'],
                                           port=dst_db_info['port'],
                                           database=dst_db_info['database'])
        except:
            print('Connection to destination database failed. Check parameter settings or database status.')
            return NO        
        else:
            cursor = dst_db_conn.cursor()
            sqlqry = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dst_db_info['schema']}'"
            cursor.execute(sqlqry)
            schemaname = cursor.fetchone()
            cursor.close()
            dst_db_conn.close()
            if schemaname[0] == dst_db_info['schema']:
                return YES
            else:
                print(f"Schema '{dst_db_info['schema']}' doesn't exist in the destination database.")
                return NO
    else:
        return YES

# Create CSV data extract files.
def create_csv_files(migration_info,src_db_info):
    src_db_conn = psycopg2.connect(user=src_db_info['user'],
                                   password=src_db_info['pwd'],
                                   host=src_db_info['host'],
                                   port=src_db_info['port'],
                                   database=src_db_info['database'])
    cursor = src_db_conn.cursor()
    sqlqry = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{src_db_info['schema']}'"
    cursor.execute(sqlqry)
    tablenames = cursor.fetchall()
    print("*** CSV Extract Processing ***")
    for table in tablenames:
        print(f"Extracting CSV data for '{table[0]}' data...")
        columnssqlqry = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table[0]}' ORDER BY ordinal_position"
        cursor.execute(columnssqlqry)
        columndata = cursor.fetchall()
        columnnames = list(column[0] for column in columndata)
        datasqlqry = f"SELECT * FROM {src_db_info['schema']}.{table[0]}"
        cursor.execute(datasqlqry)
        tabledata = cursor.fetchall()
        dataframe = pd.DataFrame(tabledata,columns=columnnames)
        datetimestamp = datetime.datetime.now().strftime("%m%d%Y_%H%M")
        filename = migration_info['extract_csv_dir']+os.path.sep+"pgres_extract_"+table[0]+f"_{datetimestamp}.csv"
        dataframe.to_csv(filename,encoding='utf-8',index=False)
    cursor.close()
    src_db_conn.close()

# Perform migration of tables, table data and table constraints.
def perform_migration(migration_info,src_db_info,dst_db_info):
    src_db_conn = psycopg2.connect(user=src_db_info['user'],
                                   password=src_db_info['pwd'],
                                   host=src_db_info['host'],
                                   port=src_db_info['port'],
                                   database=src_db_info['database'])
    dst_db_conn = psycopg2.connect(user=dst_db_info['user'],
                                   password=dst_db_info['pwd'],
                                   host=dst_db_info['host'],
                                   port=dst_db_info['port'],
                                   database=dst_db_info['database'])
    tables_in_src_db,tables_in_dst_db = get_list_of_tables_in_src_and_dst_db(src_db_info,src_db_conn,dst_db_info,dst_db_conn)
    table_list = []
    dst_db_cursor = dst_db_conn.cursor()
    print("*** Table Creation Processing Begin ***")
    for table in tables_in_src_db:
        if table in tables_in_dst_db:
            print(f"Table '{table}' already exists; table creation skipped.")
            continue
        ddlqry = generate_table_ddl(src_db_conn,src_db_info,dst_db_info,table)
        try:
            dst_db_cursor.execute(ddlqry)
            dst_db_conn.commit()
            table_list.append(table)
            print(f"\nTable '{table}' created.")
        except psycopg2.Error as errmsg:
            print(f"\nTable '{table}' failed to create. ERRMSG: {errmsg}".strip())
    print("\n*** Table Creation Processing End ***")
    if migration_info['create_tables_only'] == YES:
        create_table_constraints(src_db_info,src_db_conn,dst_db_info,dst_db_conn,table_list)
    if migration_info['create_tables_insert_data'] == YES and table_list:
        manage_load_tracker_table_in_dst_db(dst_db_info,dst_db_conn,CREATE_TABLE,table_list)
        migrate_table_data(src_db_info,dst_db_info)
        create_table_constraints(src_db_info,src_db_conn,dst_db_info,dst_db_conn,table_list)
        manage_load_tracker_table_in_dst_db(dst_db_info,dst_db_conn,DROP_TABLE,[])
    src_db_conn.close()        
    dst_db_cursor.close()
    dst_db_conn.close()

# Get list of tables in source and destination databases.
def get_list_of_tables_in_src_and_dst_db(src_db_info,src_db_conn,dst_db_info,dst_db_conn):
    src_db_cursor = src_db_conn.cursor()
    sqlqry = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{src_db_info['schema']}'"
    src_db_cursor.execute(sqlqry)
    tables_in_src_db = [table[0] for table in src_db_cursor.fetchall()]
    table_list = ", ".join("'{x}'".format(x=table) for table in tables_in_src_db)
    src_db_cursor.close()
    dst_db_cursor = dst_db_conn.cursor()
    sqlqry = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{dst_db_info['schema']}' AND tablename IN ({table_list})"
    dst_db_cursor.execute(sqlqry)
    tables_in_dst_db = [table[0] for table in dst_db_cursor.fetchall()]
    dst_db_cursor.close()
    return tables_in_src_db,tables_in_dst_db

# Generate table DDL for creating tables in destination database.
def generate_table_ddl(src_db_conn,src_db_info,dst_db_info,table):
    src_db_cursor = src_db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sqlqry = "SELECT column_name,ordinal_position,column_default,is_nullable,data_type,udt_name,typcategory,typlen,character_maximum_length,numeric_precision,numeric_scale,datetime_precision " \
             "FROM information_schema.columns LEFT JOIN pg_catalog.pg_type ON information_schema.columns.udt_name = pg_catalog.pg_type.typname " \
            f"WHERE table_schema = '{src_db_info['schema']}' AND table_name = '{table}' ORDER BY ordinal_position"
    src_db_cursor.execute(sqlqry)
    columns = src_db_cursor.fetchall()
    ddlqry = f"CREATE TABLE {dst_db_info['schema']}.{table} ("
    for column in columns:
        ddlqry += f"{column['column_name']} {column['data_type']}"
        if column['typcategory'] == "S": # Strings.
          if column['typlen'] == -1 and column['character_maximum_length']:
            ddlqry += f"({column['character_maximum_length']})" + "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")
          else:
            ddlqry += "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")
        elif column['typcategory'] == "N": # Numerics.
          if column['typlen'] == -1 and column['numeric_precision']:
            ddlqry += f"({column['numeric_precision']},{column['numeric_scale']})" + "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")
          else:
            ddlqry += "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")
        else: # All other types.
            if column['typlen'] == -1 and column['character_maximum_length']:
                ddlqry += f"({column['character_maximum_length']})" + "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")
            else:
                ddlqry += "{x}".format(x=" NOT NULL" if column['is_nullable'] == "NO" else "")           
        if column != columns[-1]:
            ddlqry += ", "
        else:
            ddlqry += ")"
    src_db_cursor.close()
    return ddlqry

# Create table constraints in destination database.
def create_table_constraints(src_db_info,src_db_conn,dst_db_info,dst_db_conn,table_list):
    table_list = ", ".join("'{x}'".format(x=table) for table in table_list)
    src_db_cursor = src_db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sqlqry = "SELECT con.conname,con.contype,rel.relname AS tablename,pg_get_constraintdef(con.oid) AS condef " \
             "FROM pg_catalog.pg_constraint con INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace " \
            f"WHERE nsp.nspname = '{src_db_info['schema']}' AND rel.relname IN ({table_list}) ORDER BY con.contype DESC"
    src_db_cursor.execute(sqlqry)
    constraints = src_db_cursor.fetchall()
    dst_db_cursor = dst_db_conn.cursor()
    print("\n*** Constraint Creation Processing Begin ***")
    for constraint in constraints:
        ddlqry = f"ALTER TABLE {dst_db_info['schema']}.{constraint['tablename']} ADD CONSTRAINT {constraint['conname']} {constraint['condef']}"
        if constraint['contype'] == "f":
            ddlqry = ddlqry.replace(f"{src_db_info['schema']}.",f"{dst_db_info['schema']}.")
        try:
            dst_db_cursor.execute(ddlqry)
            dst_db_conn.commit()
            print(f"\nTable {constraint['tablename']} constraint created: '{ddlqry}'")
        except psycopg2.Error as errmsg:
            print(f"\nTable {constraint['tablename']} constraint failed: '{ddlqry}' ERRMSG: {errmsg}".strip())
    print("\n*** Constraint Creation Processing End ***")
    src_db_cursor.close()
    dst_db_cursor.close()

# Manage the table in the destination database that's used by the load data process to load the tables being migrated. 
def manage_load_tracker_table_in_dst_db(dst_db_info,dst_db_conn,action,tables):
    dst_db_cursor = dst_db_conn.cursor()
    if action == CREATE_TABLE:
        sqlqry = f"SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = '{dst_db_info['schema']}' AND tablename = 'psgres_load_tables'"
        dst_db_cursor.execute(sqlqry)
        the_count = dst_db_cursor.fetchone()
        if the_count[0] == 0:
            sqlqry = f"CREATE TABLE {dst_db_info['schema']}.psgres_load_tables (tablename VARCHAR (50))"
            dst_db_cursor.execute(sqlqry)
            dst_db_conn.commit()
        else:
            sqlqry = f"DELETE FROM {dst_db_info['schema']}.psgres_load_tables"
            dst_db_cursor.execute(sqlqry)
            dst_db_conn.commit()
        sqlqry = f"INSERT INTO {dst_db_info['schema']}.psgres_load_tables (tablename) VALUES (%s)"
        tables = tuple([table] for table in tables)
        dst_db_cursor.executemany(sqlqry,tables)
        dst_db_conn.commit()
    if action == DROP_TABLE:
        sqlqry = f"DROP TABLE {dst_db_info['schema']}.psgres_load_tables"
        dst_db_cursor.execute(sqlqry)
        dst_db_conn.commit()
    dst_db_cursor.close()

# Migrate table data.
def migrate_table_data(src_db_info,dst_db_info):
    src_db_conn = psycopg2.connect(user=src_db_info['user'],
                                   password=src_db_info['pwd'],
                                   host=src_db_info['host'],
                                   port=src_db_info['port'],
                                   database=src_db_info['database'])
    src_db_cursor = src_db_conn.cursor()
    dst_db_url = f"postgresql://{dst_db_info['user']}:{dst_db_info['pwd']}@{dst_db_info['host']}:{dst_db_info['port']}/{dst_db_info['database']}"
    dst_db_engine = create_engine(dst_db_url)
    dst_db_conn = dst_db_engine.connect()
    sqlqry = f"SELECT tablename FROM {dst_db_info['schema']}.psgres_load_tables"
    tables_to_load = dst_db_conn.execute(sqlqry)
    print("\n*** Table Loading Processing Begin ***")
    for table in tables_to_load:
        print(f"\nLoading '{table[0]}' table...")
        start_time = timer()
        sqlqry = f"SELECT * FROM {src_db_info['schema']}.{table[0]}"
        src_db_cursor.execute(sqlqry)
        tabledata = src_db_cursor.fetchall()
        dmlqry = generate_table_dml(src_db_conn,src_db_info,dst_db_info,table[0])
        rowcnt = 0
        bulk_insert_list = []
        for row in tabledata:
            rowcnt = rowcnt + 1
            recordvalue = "(" + ",".join("'{x}'".format(x=str(column).replace("'","''")) for column in row) + ")"
            bulk_insert_list.append(recordvalue)
            if row == tabledata[-1]:
                dst_db_conn.execute(dmlqry.format(",".join(bulk_insert_list)).replace("'None'","Null"))
            elif rowcnt == BULK_INSERT_AMT:
                dst_db_conn.execute(dmlqry.format(",".join(bulk_insert_list)).replace("'None'","Null"))
                rowcnt = 0
                bulk_insert_list = []
        end_time = timer()
        elapsed_time = timedelta(seconds = end_time - start_time)
        print(f"Load statistics for '{table[0]}' table >>> Records Loaded: {len(tabledata)} | Load Time: {elapsed_time}")
    print("\n*** Table Loading Processing End ***")
    src_db_cursor.close()
    src_db_conn.close()
    dst_db_conn.close()

# Generate table DML for loading tables in destination database.
def generate_table_dml(src_db_conn,src_db_info,dst_db_info,table):
    src_db_cursor = src_db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sqlqry = f"SELECT column_name FROM information_schema.columns WHERE " \
             f"table_schema = '{src_db_info['schema']}' AND table_name = '{table}' ORDER BY ordinal_position"
    src_db_cursor.execute(sqlqry)
    columndata = src_db_cursor.fetchall()
    dmlqry = f"INSERT INTO {dst_db_info['schema']}.{table} (" + \
              ", ".join("{x}".format(x=column[0]) for column in columndata) + ") VALUES {}"
    return dmlqry

# Migration settings dictionary. 
# Note: Set 'extract_csv_dir' to '' if you don't want to extract table data to
# CSV files. Also, the create_tables_only' and 'create_tables_insert_data'
# parameters are mutually exclusive and one must be set to 'Y' and the
# other to 'N' or both must be set to 'N'.
migration_settings = {'extract_csv_dir':'',
                      'create_tables_only':'N',
                      'create_tables_insert_data':'Y'}

# Source database settings dictionary.
# Note: All key values must be set.
src_db_settings = {'user':'postgres',
                   'pwd':'********************',
                   'host':'localhost',
                   'port':'5432',
                   'database':'oracle',
                   'schema':'human_resources'}

# Destination database settings dictionary.
# Note: All key values must be set if the 'create_tables_only' or 
# 'create_tables_insert_data' migration parameters are set to 'Y'.
# Otherwise, set all key values to ''.
dst_db_settings = {'user':'postgres',
                   'pwd':'********************',
                   'host':'********************************.us-east-1.rds.amazonaws.com',
                   'port':'5432',
                   'database':'aws',
                   'schema':'human_resources'}

# Execute psgres_to_psgres function.
psgres_to_psgres(migration_settings,src_db_settings,dst_db_settings)

--- EXECUTION RESULTS ---

*** Table Creation Processing Begin ***

Table 'countries' created.

Table 'locations' created.

Table 'regions' created.

Table 'departments' created.

Table 'jobs' created.

Table 'job_history' created.

Table 'employees' created.

Table 'sales_data' created.

*** Table Creation Processing End ***

*** Table Loading Processing Begin ***

Loading 'countries' table...
Load statistics for 'countries' table >>> Records Loaded: 25 | Load Time: 0:00:00.133472

Loading 'locations' table...
Load statistics for 'locations' table >>> Records Loaded: 23 | Load Time: 0:00:00.152871

Loading 'regions' table...
Load statistics for 'regions' table >>> Records Loaded: 4 | Load Time: 0:00:00.146267

Loading 'departments' table...
Load statistics for 'departments' table >>> Records Loaded: 27 | Load Time: 0:00:00.148110

Loading 'jobs' table...
Load statistics for 'jobs' table >>> Records Loaded: 19 | Load Time: 0:00:00.149591

Loading 'job_history' table...
Load statistics for 'job_history' table >>> Records Loaded: 11 | Load Time: 0:00:00.145235

Loading 'employees' table...
Load statistics for 'employees' table >>> Records Loaded: 50 | Load Time: 0:00:00.151338

Loading 'sales_data' table...
Load statistics for 'sales_data' table >>> Records Loaded: 2000000 | Load Time: 0:10:19.261824

*** Table Loading Processing End ***

*** Constraint Creation Processing Begin ***

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_email_uk UNIQUE (email)'

Table regions constraint created: 'ALTER TABLE human_resources.regions ADD CONSTRAINT reg_id_pk PRIMARY KEY (region_id)'

Table locations constraint created: 'ALTER TABLE human_resources.locations ADD CONSTRAINT loc_id_pk PRIMARY KEY (location_id)'

Table departments constraint created: 'ALTER TABLE human_resources.departments ADD CONSTRAINT dept_id_pk PRIMARY KEY (department_id)'

Table countries constraint created: 'ALTER TABLE human_resources.countries ADD CONSTRAINT country_c_id_pk PRIMARY KEY (country_id)'

Table job_history constraint created: 'ALTER TABLE human_resources.job_history ADD CONSTRAINT jhist_emp_id_st_date_pk PRIMARY KEY (employee_id, start_date)'

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_id_pk PRIMARY KEY (employee_id)'

Table jobs constraint created: 'ALTER TABLE human_resources.jobs ADD CONSTRAINT job_id_pk PRIMARY KEY (job_id)'

Table locations constraint created: 'ALTER TABLE human_resources.locations ADD CONSTRAINT loc_c_id_fk FOREIGN KEY (country_id) REFERENCES human_resources.countries(country_id)'

Table job_history constraint created: 'ALTER TABLE human_resources.job_history ADD CONSTRAINT jhist_job_fk FOREIGN KEY (job_id) REFERENCES human_resources.jobs(job_id)'

Table sales_data constraint created: 'ALTER TABLE human_resources.sales_data ADD CONSTRAINT emp_id_emp_fk FOREIGN KEY (employee_id) REFERENCES human_resources.employees(employee_id) NOT VALID'

Table job_history constraint created: 'ALTER TABLE human_resources.job_history ADD CONSTRAINT jhist_emp_fk FOREIGN KEY (employee_id) REFERENCES human_resources.employees(employee_id) NOT VALID'

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_job_fk FOREIGN KEY (job_id) REFERENCES human_resources.jobs(job_id)'

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_dept_fk FOREIGN KEY (department_id) REFERENCES human_resources.departments(department_id)'

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_manager_fk FOREIGN KEY (manager_id) REFERENCES human_resources.employees(employee_id)'

Table countries constraint created: 'ALTER TABLE human_resources.countries ADD CONSTRAINT countr_reg_fk FOREIGN KEY (region_id) REFERENCES human_resources.regions(region_id)'

Table departments constraint created: 'ALTER TABLE human_resources.departments ADD CONSTRAINT dept_loc_fk FOREIGN KEY (location_id) REFERENCES human_resources.locations(location_id)'

Table job_history constraint created: 'ALTER TABLE human_resources.job_history ADD CONSTRAINT jhist_dept_fk FOREIGN KEY (department_id) REFERENCES human_resources.departments(department_id)'

Table departments constraint created: 'ALTER TABLE human_resources.departments ADD CONSTRAINT dept_mgr_fk FOREIGN KEY (manager_id) REFERENCES human_resources.employees(employee_id) NOT VALID'

Table job_history constraint created: 'ALTER TABLE human_resources.job_history ADD CONSTRAINT jhist_date_interval CHECK ((end_date > start_date))'

Table employees constraint created: 'ALTER TABLE human_resources.employees ADD CONSTRAINT emp_salary_min CHECK ((salary > (0)::numeric))'

*** Constraint Creation Processing End ***
[Finished in 629.8s]
