__author__ = 'jsenko'


from connection import get_postgres_connection


setup_queries = [
    "CREATE SEQUENCE json_table_id_seq START 1",

    "CREATE SEQUENCE jsonb_table_id_seq START 1",

    "CREATE SEQUENCE hstore_table_id_seq START 1",

    """CREATE TABLE json_table (
           id integer DEFAULT nextval('json_table_id_seq'),
           data JSON
       );
    """
]



cleanup_queries = [
    "DROP TABLE IF EXISTS json_table",
    "DROP SEQUENCE IF EXISTS hstore_table_id_seq",
    "DROP SEQUENCE IF EXISTS jsonb_table_id_seq",
    "DROP SEQUENCE IF EXISTS json_table_id_seq"
]



def run_queries(con, queries):
    with con.cursor() as cur:
        for query in queries:
            print query
            cur.execute(query)
    con.commit()



def setup(con):
    run_queries(con, setup_queries)

def cleanup(con):
    run_queries(con, cleanup_queries)

#setup(get_postgres_connection())

#cleanup(get_postgres_connection())