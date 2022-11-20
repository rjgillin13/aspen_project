import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

credentials = glueContext.extract_jdbc_conf(
    connection_name = "rds_postgresql"
)

url = credentials['url'] + "/aspencapitaldb"
user = credentials['user']
password = credentials['password']
connection_type = 'JDBC'

# Script generated for node S3 bucket
S3bucket_borrower = glueContext.create_dynamic_frame.from_catalog(
    database="aspen_etl", table_name="borrower_csv", transformation_ctx="S3bucket_borrower"
)

# Script generated for node Amazon S3
S3Bucket_role_profile = glueContext.create_dynamic_frame.from_catalog(
    database="aspen_etl",
    table_name="role_profile_csv",
    transformation_ctx="S3Bucket_role_profile",
)

# Script generated for node ApplyMapping
ApplyMapping_borrower = ApplyMapping.apply(
    frame=S3bucket_borrower,
    mappings=[
        ("id", "string", "BORROWER_ID", "string"),
        ("full_name", "string", "FULL_NAME", "string"),
        ("street", "string", "STREET", "string"),
        ("city", "string", "CITY", "string"),
        ("state", "string", "STATE", "string"),
        ("zip_code", "long", "ZIP_CODE", "string"),
        ("phone_home", "string", "PHONE_HOME", "string"),
        ("phone_cell", "string", "PHONE_CELL", "string"),
        ("email", "string", "EMAIL", "string"),
    ],
    transformation_ctx="ApplyMapping_borrower",
)

# Script generated for node Change Schema (Apply Mapping)
ApplyMapping_role_profile = ApplyMapping.apply(
    frame=S3Bucket_role_profile,
    mappings=[
        ("col0", "string", "BORROWER_ID", "string"),
        ("col1", "string", "ROLE_PROFILE", "string"),
    ],
    transformation_ctx="ApplyMapping_role_profile",
)

# # Script generated for node PostgreSQL table
# PostgreSQLtable_node3 = glueContext.write_dynamic_frame.from_catalog(
#     frame=ApplyMapping_node2,
#     database="aspen_etl",
#     table_name="stg_borrower",
#     transformation_ctx="PostgreSQLtable_node3",
# )

glueContext.write_dynamic_frame_from_options(
    frame=ApplyMapping_borrower,
    connection_type=connection_type,
    connection_options= {
        "url":url,
        "user":user,
        "password":password,
        "dbtable":"stg_borrower"
    },
    transformation_ctx="load_borrower"
)

glueContext.write_dynamic_frame_from_options(
    frame=ApplyMapping_role_profile,
    connection_type=connection_type,
    connection_options={
        "url":url,
        "user":user,
        "password":password,
        "dbtable":"stg_role_profile"
    },
    transformation_ctx="load_borrower_profile"
)

job.commit()