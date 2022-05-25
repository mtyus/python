""" This script connects to a PostgreSQL server and queries the data in the students table,
""" which is located in the colleges database's casewesternreserve schema. It then writes
""" the student data out to a file in JSON format.

import json
import psycopg2

try:
    connection = psycopg2.connect(user="postgres",
    	                          password="********************",
                                  host="localhost",
                                  port="5432",
                                  database="colleges")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from casewesternreserve.students"

    cursor.execute(postgreSQL_select_Query)
    print("Selecting rows from casewesternreserve student table using cursor.fetchall.")
    student_records = cursor.fetchall()

    print("Creating students.json file.")
    filename = 'students.json'
    with open(filename, 'w') as file_object:
        for idx, row in enumerate(student_records):
            # Title first and last names.
            data = {'student_id': f"{row[0]}", 'first_name': f"{row[1].title()}", 'last_name': f"{row[2].title()}", 'date_of_birth': f"{row[3]}", 'email': f"{row[4]}"}
            file_object.write(json.dumps(data))
            # Don't create newline after last record.
            if idx != len(student_records) - 1:
                file_object.write('\n')

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

-- EXECUTION RESULTS --

Selecting rows from casewesternreserve student table using cursor.fetchall.
Creating students.json file.
PostgreSQL connection is closed
[Finished in 558ms]

students.json contents
{"student_id": "4", "first_name": "Suzanne", "last_name": "Farmer", "date_of_birth": "2014-09-18", "email": "suzannefarmer@gmail.com"}
{"student_id": "5", "first_name": "Leonard", "last_name": "Grant", "date_of_birth": "2009-12-05", "email": "leonardgrant@gmail.com"}
{"student_id": "20", "first_name": "Elaine", "last_name": "Jefferson", "date_of_birth": "2020-03-02", "email": "elainejefferson@gmail.com"}
{"student_id": "27", "first_name": "Raquel", "last_name": "Booth", "date_of_birth": "2010-10-24", "email": "raquelbooth@gmail.com"}
{"student_id": "28", "first_name": "Eric", "last_name": "Jackson", "date_of_birth": "2020-06-30", "email": "ericjackson@gmail.com"}
{"student_id": "36", "first_name": "Chris", "last_name": "Preston", "date_of_birth": "2020-03-27", "email": "chrispreston@gmail.com"}
{"student_id": "44", "first_name": "Diane", "last_name": "Andrews", "date_of_birth": "2017-06-07", "email": "dianeandrews@gmail.com"}
{"student_id": "58", "first_name": "Jessica", "last_name": "Chapman", "date_of_birth": "2020-04-10", "email": "jessicachapman@gmail.com"}
{"student_id": "59", "first_name": "Michael", "last_name": "Bowman", "date_of_birth": "2018-09-03", "email": "michaelbowman@gmail.com"}
{"student_id": "61", "first_name": "Mark", "last_name": "Moses", "date_of_birth": "2017-10-13", "email": "markmoses@gmail.com"}
{"student_id": "67", "first_name": "Leslie", "last_name": "Doyle", "date_of_birth": "2014-12-24", "email": "lesliedoyle@gmail.com"}
{"student_id": "77", "first_name": "Allan", "last_name": "Carter", "date_of_birth": "2010-12-18", "email": "allancarter@gmail.com"}
{"student_id": "84", "first_name": "Michael", "last_name": "Kirby", "date_of_birth": "2018-11-19", "email": "michaelkirby@gmail.com"}
{"student_id": "96", "first_name": "Christopher", "last_name": "Soto", "date_of_birth": "2019-09-24", "email": "christophersoto@gmail.com"}
{"student_id": "103", "first_name": "Deborah", "last_name": "Lindsey", "date_of_birth": "2013-05-25", "email": "deborahlindsey@gmail.com"}
{"student_id": "111", "first_name": "Teason", "last_name": "Anderson", "date_of_birth": "2018-01-30", "email": "teasonanderson@gmail.com"}
{"student_id": "112", "first_name": "Douglas", "last_name": "Howell", "date_of_birth": "2009-08-06", "email": "douglashowell@gmail.com"}
{"student_id": "114", "first_name": "Bryant", "last_name": "Vargas", "date_of_birth": "2019-08-21", "email": "bryantvargas@gmail.com"}
{"student_id": "139", "first_name": "Edward", "last_name": "Hayes", "date_of_birth": "2020-03-11", "email": "edwardhayes@gmail.com"}
{"student_id": "157", "first_name": "Al", "last_name": "Serrano", "date_of_birth": "2019-11-01", "email": "alserrano@gmail.com"}
{"student_id": "168", "first_name": "John", "last_name": "Cameron", "date_of_birth": "2017-08-28", "email": "johncameron@gmail.com"}
{"student_id": "190", "first_name": "Jessica", "last_name": "Wilson", "date_of_birth": "2019-01-21", "email": "jessicawilson@gmail.com"}
{"student_id": "198", "first_name": "Hunyen", "last_name": "Curry", "date_of_birth": "2009-10-30", "email": "hunyencurry@gmail.com"}
{"student_id": "205", "first_name": "Michael", "last_name": "Vasquez", "date_of_birth": "2018-06-06", "email": "michaelvasquez@gmail.com"}
{"student_id": "213", "first_name": "Brian", "last_name": "Morton", "date_of_birth": "2019-06-18", "email": "brianmorton@gmail.com"}
{"student_id": "214", "first_name": "Gary", "last_name": "Jennings", "date_of_birth": "2020-04-07", "email": "garyjennings@gmail.com"}
{"student_id": "222", "first_name": "Danielle", "last_name": "Atkinson", "date_of_birth": "2018-07-27", "email": "danielleatkinson@gmail.com"}
{"student_id": "234", "first_name": "Gary", "last_name": "Long", "date_of_birth": "2019-01-12", "email": "garylong@gmail.com"}
{"student_id": "251", "first_name": "Michael", "last_name": "Schmidt", "date_of_birth": "2010-07-28", "email": "michaelschmidt@gmail.com"}
{"student_id": "259", "first_name": "George", "last_name": "Horn", "date_of_birth": "2020-02-06", "email": "georgehorn@gmail.com"}
{"student_id": "275", "first_name": "Shannon", "last_name": "Gilbert", "date_of_birth": "2011-07-03", "email": "shannongilbert@gmail.com"}
{"student_id": "287", "first_name": "Dennis", "last_name": "Freeman", "date_of_birth": "2019-08-20", "email": "dennisfreeman@gmail.com"}
{"student_id": "290", "first_name": "Robert", "last_name": "French", "date_of_birth": "2013-02-06", "email": "robertfrench@gmail.com"}
{"student_id": "303", "first_name": "Cynthia", "last_name": "Harper", "date_of_birth": "2019-09-20", "email": "cynthiaharper@gmail.com"}
{"student_id": "313", "first_name": "Ellen", "last_name": "Fox", "date_of_birth": "2010-01-20", "email": "ellenfox@gmail.com"}
