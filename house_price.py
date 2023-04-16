from pyspark.sql.functions import *
from gluejob import GlueJob
from clean import *
import boto3
from datetime import datetime
import pytz

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

#get the latest house price csv file path in s3

bucket_name = 'school-rating-raw-data'
prefix = 'ABS/house-price/'
latest_csv_file = get_latest_csv_file(bucket_name, prefix)
latest_csv_file_path = 's3://' + bucket_name + '/' + latest_csv_file

#create a gluejob instance
job = GlueJob()

#create dyf for house price, run clean function
dyf_house_price = job.create_dyf(latest_csv_file_path)
dyf_house_price = house_price(job.context, dyf_house_price)
job.run(dyf_house_price,"House_Price_By_LGA_2016-2021")