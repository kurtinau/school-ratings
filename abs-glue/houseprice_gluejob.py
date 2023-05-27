from awsglue.utils import getResolvedOptions
from awsglue.transforms import Join
from pyspark.context import SparkContext
from pyspark.sql.functions import *
import sys
import boto3
from datetime import datetime
import pytz

# Define bucket name and file folder prefix
bucket_name = 'school-rating-raw-data'
file_folder_prefix = 'ABS/house-price/'

# Define the path for transformed data
transformed_data_path_dict = {
    "houseprice": "s3://school-rating-silver-apsoutheast2-dev/houseprice"
}

# Function to create a dynamic frame from the S3 bucket for a CSV file
def create_dyf(gluecontext, path):
    """
    Creates a dynamic frame from the S3 bucket for a CSV file.

    Args:
        gluecontext (GlueContext): Glue context object.
        path (str): Path of the CSV file.

    Returns:
        DynamicFrame: The dynamic frame created from the CSV file.
    """
    dyf = gluecontext.create_dynamic_frame_from_options(
        connection_type='s3',
        connection_options={
            'paths': [path],
            'recurse': True
        },
        format='csv',
        format_options={
            "withHeader": True
        }
    )
    return dyf


# Function to save a dynamic frame as a Parquet file in S3
def save_as_parquet(gluecontext, dyf, path):
    """
    Saves a dynamic frame as a Parquet file in S3.

    Args:
        gluecontext (GlueContext): Glue context object.
        dyf (DynamicFrame): Dynamic frame to be saved.
        path (str): Path in S3 to save the Parquet file.
    """
    gluecontext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3", connection_options={
        "path": path,
        "partitionKeys": ["year"]
    },
    format="parquet")


# Function to get the latest CSV file from an S3 bucket
def get_latest_csv_file(bucket_name, prefix):
    """
    Retrieves the latest CSV file from an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix of the file path.

    Returns:
        str: The path of the latest CSV file in the S3 bucket.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    latest_modified_time = datetime.min.replace(tzinfo=pytz.UTC)
    latest_file = ''

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page['Contents']:
            if obj['LastModified'] > latest_modified_time and obj['Key'].endswith('.csv'):
                latest_modified_time = obj['LastModified']
                latest_file = obj['Key']

    return latest_file


# Function to clean and transform the house price CSV file
def house_price(glue_context, dyf):
    """
    Cleans and transforms the house price CSV file.

    Args:
        glue_context (GlueContext): Glue context object.
        dyf (DynamicFrame): Dynamic frame representing the house price data.

    Returns:
        DynamicFrame: The transformed dynamic frame.
    """
    # Select 'LGA_2021', 'TIME_PERIOD', 'OBS_VALUE' columns only
    dyf = dyf.select_fields(['LGA_2021', 'TIME_PERIOD', 'OBS_VALUE'])
    # Convert dynamic frame to PySpark DataFrame
    dyf = dyf.toDF()
    # Rename the columns & change the value type to int
    dyf = dyf.withColumn('OBS_VALUE', col('OBS_VALUE').cast("integer")).withColumn('LGA_2021', col('LGA_2021').cast("integer"))\
        .withColumnRenamed('LGA_2021', 'local_government_code')\
        .withColumnRenamed('TIME_PERIOD', 'year')\
        .withColumnRenamed('OBS_VALUE', 'median_house_price')
    # Convert the resulting DataFrame back to a dynamic frame
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_house_price")
    return dyf


# Main program

# Get the command line arguments
params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)

# Create GlueContext and Job objects
gluecontext = GlueContext(SparkContext.getOrCreate())
job = Job(gluecontext)

# Set the job name
if 'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"
job.init(jobname, args)

# Get the latest house price CSV file path in S3
latest_csv_file = get_latest_csv_file(bucket_name, file_folder_prefix)
latest_csv_file_path = 's3://' + bucket_name + '/' + latest_csv_file

# Create dynamic frame for house price
dyf_house_price = create_dyf(gluecontext, latest_csv_file_path)
dyf_house_price = house_price(gluecontext, dyf_house_price)

# Save the transformed file as Parquet
save_as_parquet(gluecontext, dyf_house_price, transformed_data_path_dict['houseprice'])

# Commit the job
job.commit()
