__author__ = 'Tomas Skopal, Jakub Senko'

import time

from connection import get_postgres_connection

def postgres_select(con, result):
	start = time.time()
	with con.cursor() as cur:
		resutl = cur.execute("SELECT data->>'date' as date FROM jsonb_table") 
	return (time.time() - start) * 1000.0

def simple_select():
	text_result = ""
	time = postgres_select(get_postgres_connection(), text_result)
	print text_result
	print time

simple_select()
