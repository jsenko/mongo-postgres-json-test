__author__ = 'Jakub Senko'

from pymongo import *
import psycopg2



def get_mongo_client():
    client = MongoClient()
    return client



def get_postgres_connection():
    try:
        return psycopg2.connect("dbname='root' user='root' host='localhost' password='root'")
    except:
        print "Cannot connect to the PostgreSQL database."
