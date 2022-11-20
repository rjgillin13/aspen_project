import psycopg2
import config

print ("Connecting via psycopg")

conn = psycopg2.connect(host=config.HOST, port=config.PORT, user=config.USER, password=config.PASSWORD, database=config.DBNAME)

cur = conn.cursor()
f = open(r'../data/role_profile.csv', 'r')
cur.execute("""truncate table stg_role_profile;""")
cur.copy_from(f, 'stg_role_profile', sep=',')
conn.commit()
conn.close()