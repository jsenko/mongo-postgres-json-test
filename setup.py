__author__ = 'Jakub Senko'

from connection import get_postgres_connection, get_mongo_client

setup_queries = [

    "CREATE EXTENSION IF NOT EXISTS hstore",  # PACKAGE postresql94-contrib REQUIRED!

    "CREATE EXTENSION IF NOT EXISTS btree_gin",

    "CREATE EXTENSION IF NOT EXISTS btree_gist",

    """ CREATE TABLE hstore_table (
            id integer PRIMARY KEY,
            data hstore
        )
    """,

    """ CREATE TABLE json_table (
            id integer PRIMARY KEY,
            data JSON
        )
    """,

    """ CREATE TABLE jsonb_table (
            id integer PRIMARY KEY,
            data JSONB
        )
    """
]



cleanup_queries = [

 #   "DROP TABLE IF EXISTS jsonb_table_gist",

#    "DROP TABLE IF EXISTS jsonb_table_gin",

    "DROP TABLE IF EXISTS jsonb_table",

  #  "DROP TABLE IF EXISTS json_table_gist",

   # "DROP TABLE IF EXISTS json_table_gin",

    "DROP TABLE IF EXISTS json_table",

    #"DROP TABLE IF EXISTS hstore_table_gist",

    #"DROP TABLE IF EXISTS hstore_table_gin",

    "DROP TABLE IF EXISTS hstore_table",
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