import runpy
from scripts import tools, config
def execute_pipeline():
    print('Starting Pipeline')
    print('Splitting .xlsx into .csv and load to S3')
    runpy.run_path(path_name='scripts/load_data_s3.py')
    print('import .csv to postgresql')
    runpy.run_path(path_name='scripts/import_postgresql.py')
    print('drop and create tables in postgresql')
    runpy.run_path(path_name='scripts/drop_create_tables.py')
    print('creating postgresql connection object')
    conn = tools.connect_postgresql(host=config.HOST, port=config.PORT, user=config.USER, password=config.PASSWORD, db=config.DBNAME)
    cur = conn.cursor()
    with open('sql/user_profile_procedure.sql', 'r') as f:
        print('create_user_profile_table')
        cur.execute(f.read())
        cur.execute('CALL sp_user_profile()')
        conn.commit()
    with open('sql/role_profile_type_procedure.sql', 'r') as f:
        print('create_role_profile_type_table')
        cur.execute(f.read())
        cur.execute('CALL sp_role_profile_type()')
        conn.commit()
    with open('sql/phone_number_type_procedure.sql', 'r') as f:
        print('create_phone_number_type_table')
        cur.execute(f.read())
        cur.execute('CALL sp_phone_number_type()')
        conn.commit()
    with open('sql/email_type_procedure.sql', 'r') as f:
        print('create_email_type_table')
        cur.execute(f.read())
        cur.execute('CALL sp_email_type()')
        conn.commit()
    with open('sql/role_profile_procedure.sql', 'r') as f:
        print('create_role_profile_table')
        cur.execute(f.read())
        cur.execute('CALL sp_role_profile()')
        conn.commit()
    with open('sql/email_procedure.sql', 'r') as f:
        print('create_email_table')
        cur.execute(f.read())
        cur.execute('CALL sp_email()')
        conn.commit()
    with open('sql/address_procedure.sql', 'r') as f:
        print('create_address_table')
        cur.execute(f.read())
        cur.execute('CALL sp_address()')
        conn.commit()
    with open('sql/phone_number_procedure.sql', 'r') as f:
        print('create_phone_Number_table')
        cur.execute('CALL sp_phone_number()')
        cur.execute(f.read())
        conn.commit()
    conn.close()
    print('pipeline finished')


if __name__ == "__main__":
    execute_pipeline()