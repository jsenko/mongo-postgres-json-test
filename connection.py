__author__ = 'Jakub Senko'

from pymongo import MongoClient
import psycopg2


def get_mongo_client():
    client = MongoClient()
    return client


def get_postgres_connection():
    try:
        #return psycopg2.connect("dbname='no_sql' user='postgres' host='localhost' password='postgres'")
        return psycopg2.connect("dbname='root' user='root' host='localhost' password='root'")
    except:
        print "Cannot connect to the PostgreSQL database."
