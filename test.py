__author__ = 'Jakub Senko'

from pymongo import *

client = MongoClient()

collection = client.test.test

id = collection.insert({"name": "James", "surname": "Bond"})

print id


import psycopg2

try:
    conn = psycopg2.connect("dbname='root' user='root' host='localhost' password='root'")

except:
    print "I am unable to connect to the database"






