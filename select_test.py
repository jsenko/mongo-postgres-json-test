__author__ = 'Tomas Skopal, Jakub Senko'

import time

from connection import get_postgres_connection, get_mongo_client
from setup import cleanup, run_queries, setup_queries
from generate_json import generate_all

# DO NOT name this file "select.py" because it causes some stupid import issues with pymongo



"""
    1. select by ID, in postgres this is a separate column
    2. exact field not nested - name
    3. exact fields nested, with AND, manufacturer company and country
    4. count owners
    5. fulltext - all cars with owners in some country (address)

    WARNING this script clears the database tables
"""



# TODO make this nicer
setup_function = [
    """CREATE OR REPLACE FUNCTION json_val_arr(_j json, _key text) RETURNS text[] AS
       $$
       SELECT array_agg(elem->>_key) FROM json_array_elements(_j) AS x(elem)
       $$
       LANGUAGE sql IMMUTABLE;
    """]  # this seems quite inconvenient



queries_setup_gin = [
    # GIN

    "CREATE INDEX hstore_gin ON hstore_table USING gin(data)",

    "CREATE INDEX json_gin_company ON json_table USING gin((data#>>'{manufactured, company}'))",
    "CREATE INDEX jsonb_gin_company ON jsonb_table USING gin((data#>>'{manufactured, company}'))",

    "CREATE INDEX json_gin_country ON json_table USING gin((data#>>'{manufactured, country}'))",
    "CREATE INDEX jsonb_gin_country ON jsonb_table USING gin((data#>>'{manufactured, country}'))",

    "CREATE INDEX json_gin_address ON json_table USING gin(json_val_arr((data#>>'{owners}')::json, 'address'::text))",
    "CREATE INDEX jsonb_gin_address ON jsonb_table USING gin(json_val_arr((data#>>'{owners}')::json, 'address'::text))"
]



queries_setup_gist = [
    # GIST
    "CREATE INDEX hstore_gist ON hstore_table USING gist(data)",

    "CREATE INDEX json_gist_company ON json_table USING gist((data#>>'{manufactured, company}'))",
    "CREATE INDEX jsonb_gist_company ON jsonb_table USING gist((data#>>'{manufactured, company}'))",

    "CREATE INDEX json_gist_country ON json_table USING gist((data#>>'{manufactured, country}'))",
    "CREATE INDEX jsonb_gist_country ON jsonb_table USING gist((data#>>'{manufactured, country}'))"

    "CREATE INDEX json_gist_address ON json_table USING gist(json_val_arr(data#>>'{owners}', 'address'))",
    "CREATE INDEX jsonb_gist_address ON jsonb_table USING gist(json_val_arr(data#>>'{owners}', 'address'))"
]



def setup_gin(con):
    cleanup(get_postgres_connection())
    """
        setup indexes, then
        generate and insert 1000 documents
    """
    run_queries(get_postgres_connection(), setup_queries
                                           + setup_function
                                           + queries_setup_gin)
    client = get_mongo_client()
    for id in range(20): # just 20 for now
        json = generate_all()
        with con.cursor() as cur:
            # print json
            # The following is because hstore DOES NOT support
            # json syntax and arrays, so the data must be pruned and edited.
            # The result is a root key-value document WITHOUT nested elements.
            foo = json[1:json.find("\"manufactured\"")].replace("{", "").replace("}", "").replace(":", "=>") + "\"last\"=>\"\""
            # print foo
            cur.execute("INSERT INTO hstore_table (id, data) values (%s, '%s')"
                        % (id, foo)
            )
            cur.execute("INSERT INTO json_table (id, data) values (%s, '%s')" % (id, json))
            cur.execute("INSERT INTO jsonb_table (id, data) values (%s, '%s')" % (id, json))
    con.commit()
    json = json.replace("{", '{ "_id" : ' + str(id) + ",")
    json = eval(json)  # pymongo wants python dict
    client.test.test.insert(json)



class Result:
    data = None
    time = 0.0



def postgres_select(con):
    result = Result()
    start = time.time()
    with con.cursor() as cur:
        cur.execute("SELECT data FROM json_table LIMIT 1;")
        result.data = cur.fetchall()
    result.time = (time.time() - start) * 1000.0
    return result



def simple_select():
    result = postgres_select(get_postgres_connection())
    print result.data, result.time



setup_gin(get_postgres_connection())
simple_select()
