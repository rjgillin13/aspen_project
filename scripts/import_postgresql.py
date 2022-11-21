import scripts.tools as tools
import scripts.config as config

conn = tools.connect_postgresql(host=config.HOST, port=config.PORT, user=config.USER, password=config.PASSWORD, db=config.DBNAME)
cur = conn.cursor()

cur.execute("""truncate table stg_role_profile;""")
cur.execute("""truncate table stg_borrower;""")

f = open(r'data/borrower.csv', 'r')
cur.copy_from(f, 'stg_borrower', sep=',')
conn.commit()

f = open(r'data/role_profile.csv', 'r')
cur.copy_from(f, 'stg_role_profile', sep=',')
conn.commit()
conn.close()

