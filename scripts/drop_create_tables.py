import psycopg2
import scripts.config as config

print ("Connecting via psycopg")

conn = psycopg2.connect(host=config.HOST, port=config.PORT, user=config.USER, password=config.PASSWORD, database=config.DBNAME)

inputdir = 'sql/create_drop_tables.sql'

cur = conn.cursor()
with open(inputdir, 'r') as sql_file:
    cur.execute(sql_file.read())
conn.commit()  # Remember to commit all your changes!
conn.close()