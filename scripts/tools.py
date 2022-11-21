import logging
import boto3
from botocore.exceptions import ClientError
import os
import psycopg2
import sys
from sqlalchemy import create_engine

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def run_glue_job(job_name, arguments = {}):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
        return job_run_id
    except ClientError as e:
        raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in run_glue_job: " + e.__str__())


def connect_postgresql(host, port, user, db, password):
    '''Connect to RDS postgresql via psycopg2

    params:
        host (str): Url of client to RDS database
        port (int): Port to connect on
        user (string): Username for db
        db (string): Name of database
        password (string): User Password
    returns:
        conn: psycopg2 connection object
    '''
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=db)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def connect_s3(aws_access_key, aws_secret_key, region):
    '''Establishes connection to s3 bucket

        params:
          aws_access_key (str): AWS access key id
          aws_secret_key (str): AWS secret access key associated with account
          region (str): Default region name for resources
        returns:
          None
    '''

    session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)
    s3 = session.resource('s3')

    return s3


def aws_connect(aws_access_key, aws_secret_key, region, db_name, port, username):
    '''Sets up session with AWS account and returns session token

        params:
          aws_access_key (str): AWS access key id
          aws_secret_key (str): AWS secret key for account
          region (str): Default region for resources
          db_name (str): Database name for RDS
          port (int): Port to connect on
          username (str): User for accessing RDS db
        returns:
          token (str): Session token/password
    '''

    session = boto3.Session(aws_access_key_id=aws_access_key, aws_seret_access_key=aws_secret_key, region=region)
    client = session.client('rds')
    token = client.generate_db_auth_token(dbhostname=db_name, port=port, dbusername=username, region=region)

    return token

