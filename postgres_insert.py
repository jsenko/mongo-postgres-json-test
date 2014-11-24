__author__ = 'Tomas Skopal'

import psycopg2

try:
    conn = psycopg2.connect("dbname='no_sql' user='postgres' host='localhost' password='postgres'")
    print "coccesfully connect"
except:
    print "I am unable to connect to the database"

cur = conn.cursor()

cur.execute('TRUNCATE json_data CASCADE')
query =  "INSERT INTO json_data (id, data) values (%s, %s);"
for i in range(100):
	data = (str(i), '{"name": "Apple Phone", "type": "phone"}')
	cur.execute(query, data)
conn.commit()

print "End of the script"


    







