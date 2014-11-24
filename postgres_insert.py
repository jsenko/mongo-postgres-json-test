__author__ = 'Tomas Skopal'

import psycopg2
import time
from pymongo import *

print

try:
    conn = psycopg2.connect("dbname='no_sql' user='postgres' host='localhost' password='postgres'")
except:
    print "I am unable to connect to the database"

#json
cur = conn.cursor()
cur.execute('TRUNCATE json_data CASCADE')
query =  "INSERT INTO json_data (data) values ({});"
n = 1000
for i in range(n):
	data = '\'{"name": "Apple Phone", "type": "phone"}\''
	cur.execute(query.format(data))

start = time.time()
conn.commit()
end = time.time()
print 'Commit %d inserts into postgres with json took %0.3f ms' % (n, (end-start)*1000.0)

#json_b
cur = conn.cursor()
cur.execute('TRUNCATE jsonb_data CASCADE')
query =  "INSERT INTO jsonb_data (data) values ({});"
n = 1000
for i in range(n):
	data = '\'{"name": "Apple Phone", "type": "phone"}\''
	cur.execute(query.format(data))

start = time.time()
conn.commit()
end = time.time()
print 'Commit %d inserts into postgres with jsonb took %0.3f ms' % (n, (end-start)*1000.0)

#mongo

client = MongoClient()
collection = client.test.test

collection.remove()

start = time.time()

data = ""
for i in range(n):
	collection.insert({"name": "Apple Phone", "type": "phone"})

end = time.time()
print '%d inserts in Mongo took %0.3f ms' % (n, (end-start)*1000.0)

print
print "End of the script"


    







