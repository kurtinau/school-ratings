from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Join
from pyspark.context import SparkContext
from pyspark.sql.functions import *
import sys
import boto3
from datetime import datetime
import pytz

bucket_name = 'school-rating-raw-data'
file_folder_prefix = 'ABS/house-price/'

transformed_data_path_dict = {
    "houseprice": "s3://school-rating-silver-apsourheast2-dev/houseprice"

}

def create_dyf(gluecontext,path):
    # Create a dynamic frame from the S3 bucket for csv file
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

def save_as_parquet(gluecontext,dyf,path):
    #save transformed file in s3 as a parquet file
    gluecontext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3", connection_options={
      "path": path,
      "partitionKeys": ["year"]
      }, 
      format="parquet")


#create a fuction to loop through the bucket and find the lastest csv file
def get_latest_csv_file(bucket_name, prefix):
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

#clean & transform house price csv file
def house_price(glue_context, dyf):
    
    #select 'LGA_2021', 'TIME_PERIOD', 'OBS_VALUE' columns only
    dyf = dyf.select_fields(['LGA_2021', 'TIME_PERIOD', 'OBS_VALUE'])
    # Convert dynamic frame to PySpark DataFrame
    dyf = dyf.toDF()
    #rename the columns & change the value type to int
    dyf = dyf.withColumn('OBS_VALUE', col('OBS_VALUE').cast("integer")).withColumn('LGA_2021', col('LGA_2021').cast("integer"))\
        .withColumnRenamed('LGA_2021', 'local_government_code')\
        .withColumnRenamed('TIME_PERIOD', 'year')\
        .withColumnRenamed('OBS_VALUE', 'Median_price_of_established_residential_house_transfers_($)')
    # convert the resulting DataFrame back to a dynamicframe
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_house_price")
    return dyf


params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)
gluecontext = GlueContext(SparkContext.getOrCreate())
job = Job(gluecontext)
if 'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"
job.init(jobname, args)


#get the latest house price csv file path in s3
latest_csv_file = get_latest_csv_file(bucket_name, file_folder_prefix)
latest_csv_file_path = 's3://' + bucket_name + '/' + latest_csv_file


#create dyf for house price
dyf_house_price = create_dyf(gluecontext,latest_csv_file_path)
dyf_house_price = house_price(gluecontext, dyf_house_price)
#save file to parquet
save_as_parquet(gluecontext,dyf_house_price,transformed_data_path_dict['houseprice'])

job.commit()