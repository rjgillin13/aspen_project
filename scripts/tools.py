import logging
import boto3
from botocore.exceptions import ClientError
import os
import psycopg2
import sqlite3

# from google.cloud.bigquery.dbapi import connect
# from ploomber.clients import (DBAPIClient, GCloudStorageClient,
#                               SQLAlchemyClient)

# NOTE: you may use db or db_sqlalchemy. Both work the same


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


def connect_mssql(host, port, user, db, password):
    '''Connect to RDS msql via psycopg2

    params:
        host (str): Url of client to RDS database
        port (int): Port to connect on
        user (string): Username for db
        db (string): Name of database
        password (string): Result of AWS session token
    returns:
        conn: psycopg2 connection object
    '''

    conn = psycopg2.connect(host=host, port=port, user=user, database=db, password=password, sslmode='require', sslrootcert='SSLCERTIFICATE')

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


    # def db_sqlalchemy():
        #     """Client to send queries to BigQuery (uses SQLAlchemy as backend)
        #     """
        #     # you may pass bigquery://{project-name} to use a specific project,
        #     # otherwise thiw will use the default one
        #     return SQLAlchemyClient('bigquery://')
        #
        #
        # def storage():
        #     """Client to upload files to Google Cloud Storage
        #     """
        #     # ensure your bucket_name matches
        #     return GCloudStorageClient(bucket_name='ploomber-bucket',
        #                                parent='my-pipeline')
        #
        #
        # def metadata():
        #     """
        #     (Optional) client to store SQL tasks metadata to enable incremental builds
        #     """
        #     return DBAPIClient(sqlite3.connect, dict(database='products/metadata.db'))