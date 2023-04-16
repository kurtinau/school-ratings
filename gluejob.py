import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import datetime


class GlueJob:
    def __init__(self):
        params = []
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        args = getResolvedOptions(sys.argv, params)

        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)

        if 'JOB_NAME' in args:
            jobname = args['JOB_NAME']
        else:
            jobname = "test"
        self.job.init(jobname, args)

    def create_dyf(self,path):
        # Create a dynamic frame from the S3 bucket for csv file
        dyf = self.context.create_dynamic_frame_from_options(
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

    def run(self,dyf,name):
        #save transformed file in s3 as a parquet file
        self.context.write_dynamic_frame.from_options(frame=dyf, connection_type="s3", connection_options={
          "path": f"s3://school-rating-silver-apsourheast2-dev/{name}/",
          "partitionKeys": ["year"]
          }, 
          format="parquet")

        self.job.commit()


