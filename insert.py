__author__ = 'Tomas Skopal, Jakub Senko'

import time

from generate_json import generate_manufactured
from connection import get_postgres_connection, get_mongo_client
from setup import setup, cleanup, run_queries



"""
    Tests:
        1. inserting map-only JSON document so we can also test hstore
        2. do this with indexes
        3. finally full JSON (no hstore) with indexes (then test reads)
"""



def run_hstore(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO hstore_table (id, data) values (%s, '%s')"
            % (id, json.replace("{", "").replace("}", "").replace(":", "=>"))
        )
    #con.commit()
    return (time.time() - start) * 1000.0



def run_json(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO json_table (id, data) values (%s, '%s')" % (id, json))
    #con.commit()
    return (time.time() - start) * 1000.0



def run_jsonb(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO jsonb_table (id, data) values (%s, '%s')" % (id, json))
    #con.commit()
    return (time.time() - start) * 1000.0



def run_mongo(client, id, json):
    json = json.replace("{", '{ "_id" : ' + str(id) + ",")
    json = eval(json)  # pymongo wants python dict
    start = time.time()
    client.test.test.insert(json)
    return (time.time() - start) * 1000.0



def test_base(n, times):
    for i in range(n):
        json = generate_manufactured()

        con = get_postgres_connection()
        times[0] += run_hstore(con, i, json)
        con.commit()

        con = get_postgres_connection()
        times[1] += run_json(con, i, json)
        con.commit()

        con = get_postgres_connection()
        times[2] += run_jsonb(con, i, json)
        con.commit()

        client = get_mongo_client()
        times[3] += run_mongo(client, i, json)



def test1():
    cleanup(get_postgres_connection())
    setup(get_postgres_connection())
    results = [0.0, 0.0, 0.0, 0.0]
    test_base(1000, results)
    print "test 1 - insert without index: ", results



index_queries_gin = [

    "CREATE INDEX hstore_gin ON hstore_table USING gin(data)",

    "CREATE INDEX json_gin ON json_table USING gin((data#>>'{date}'))",

    "CREATE INDEX jsonb_gin ON jsonb_table USING gin((data#>>'{date}'))"
]



def test2():
    "with index on 'country'"
    cleanup(get_postgres_connection())
    setup(get_postgres_connection())
    run_queries(get_postgres_connection(), index_queries_gin)
    get_mongo_client().test.test.ensure_index("country")
    results = [0.0, 0.0, 0.0, 0.0]
    test_base(1000, results)
    print "test 2 - insert with GIN (+mongo index): ", results



index_queries_gist = [

    "CREATE INDEX hstore_gist ON hstore_table USING gist(data)",

    "CREATE INDEX json_gist ON json_table USING gist((data#>>'{date}'))",

    "CREATE INDEX jsonb_gist ON jsonb_table USING gist((data#>>'{date}'))"
]



def test3():
    "with index on 'country'"
    cleanup(get_postgres_connection())
    setup(get_postgres_connection())
    run_queries(get_postgres_connection(), index_queries_gist)
    get_mongo_client().test.test.ensure_index("date")
    results = [0.0, 0.0, 0.0, 0.0]
    test_base(1000, results)
    print "test 3 - insert with GIST (+mongo index): ", results



print "running tests. this may take some time, keep calm!"
print "results are cumulative times, always in order [hstore, json, jsonb, mongo]"



test1()
test2()
test3()


print "End of the script"
