__author__ = 'Tomas Skopal'

import time

from generate_json import generate_manufactured
from connection import get_postgres_connection, get_mongo_client
from setup import setup, cleanup



"""
    Tests:
        1. inserting map-only JSON document so we can also test hstore
        2. do this with indexes
        3. finally full JSON (no hstore) with indexes (then test reads)
"""



def run_json(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO json_table (id, data) values (%s, '%s')" % (id, json))
    return (time.time() - start) * 1000.0



def run_jsonb(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO jsonb_table (id, data) values (%s, '%s')" % (id, json))
    return (time.time() - start) * 1000.0



def run_hstore(con, id, json):
    start = time.time()
    with con.cursor() as cur:
        cur.execute("INSERT INTO hstore_table (id, data) values (%s, '%s')" % (id,
                    json.replace("{", "")
                        .replace("}", "")
                        .replace(":", "=>")))
    return (time.time() - start) * 1000.0



def run_mongo(client, id, json):
    json = json.replace("{", '{ "_id" : ' + str(id) + ",")
    json = eval(json)
    start = time.time()
    client.test.test.insert(json)
    return (time.time() - start) * 1000.0



def test1(n, times):
    for i in range(n):
        json = generate_manufactured()

        con = get_postgres_connection()
        times[0] += run_json(con, i, json)
        con.commit() # not sure if this should be included in time, probably yes

        con = get_postgres_connection()
        times[1] += run_jsonb(con, i, json)
        con.commit()

        con = get_postgres_connection()
        times[2] += run_hstore(con, i, json)
        con.commit()

        client = get_mongo_client()
        times[3] += run_mongo(client, i, json)



cleanup(get_postgres_connection())
setup(get_postgres_connection())
results = [0.0, 0.0, 0.0, 0.0]
test1(1000, results)
print results



print "End of the script"
