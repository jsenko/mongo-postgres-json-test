__author__ = 'jsenko'

from connection import get_postgres_connection, get_mongo_client

setup_queries = [

    "CREATE EXTENSION IF NOT EXISTS hstore",  # PACKAGE postresql94-contrib REQUIRED!

    #"CREATE SEQUENCE json_table_id_seq START 1",

    #"CREATE SEQUENCE jsonb_table_id_seq START 1",

    #"CREATE SEQUENCE hstore_table_id_seq START 1",

    """ CREATE TABLE json_table (
            id integer PRIMARY KEY,
            data JSON
        )
    """, #DEFAULT nextval('json_table_id_seq')

    """ CREATE TABLE jsonb_table (
            id integer PRIMARY KEY,
            data JSONB
        )
    """,

    """ CREATE TABLE hstore_table (
            id integer PRIMARY KEY,
            data hstore
        )
    """
]

cleanup_queries = [
    "DROP TABLE IF EXISTS hstore_table",
    "DROP TABLE IF EXISTS jsonb_table",
    "DROP TABLE IF EXISTS json_table",
    #"DROP SEQUENCE IF EXISTS hstore_table_id_seq", MANUALLY
    #"DROP SEQUENCE IF EXISTS jsonb_table_id_seq",
    #"DROP SEQUENCE IF EXISTS json_table_id_seq"
]


def run_queries(con, queries, params=()):
    with con.cursor() as cur:
        for query in queries:
            #print query
            cur.execute(query, params)
    con.commit()


def setup(con):
    run_queries(con, setup_queries)


def cleanup(con):
    run_queries(con, cleanup_queries)
    get_mongo_client().test.test.remove()

#setup(get_postgres_connection())

#cleanup(get_postgres_connection())