from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Join
from pyspark.context import SparkContext
from pyspark.sql.functions import *
import sys
import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# Define the raw data file paths
raw_data_path_dict = {
    "income": "s3://school-rating-raw-data/ABS/ABS-Average_household_income.csv",
    "demographic": "s3://school-rating-raw-data/ABS/ABS-Demographic.csv",
    "rent": "s3://school-rating-raw-data/ABS/median_weekly_rent_POA_2021.csv"
}

# Define the transformed data path
transformed_data_path_dict = {
    "suburbinfo": "s3://school-rating-silver-apsoutheast2-dev/suburbinfo"
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
    dyf = dyf.coalesce(2)
    gluecontext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3", connection_options={
        "path": path,
        "partitionKeys": ["year"]
    }, format="parquet")

# Function to clean and transform the household income data
def household_income(glue_context, dyf):
    """
    Cleans and transforms the household income data.

    Args:
        glue_context (GlueContext): Glue context object.
        dyf (DynamicFrame): Dynamic frame representing the household income data.

    Returns:
        DynamicFrame: The transformed dynamic frame.
    """
    dyf = dyf.drop_fields(['DATAFLOW', 'MEDAVG: Median/Average', 'REGION_TYPE: Region Type'])
    dyf = dyf.toDF()
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    dyf = dyf.withColumn("OBS_VALUE", col("OBS_VALUE").cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_HI')\
        .withColumnRenamed('STATE: State', 'State')\
        .withColumnRenamed('TIME_PERIOD: Time Period', 'year')\
        .withColumnRenamed('OBS_VALUE', 'Median total household income ($/weekly)')
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_householdincome")
    return dyf

# Function to clean and transform the demographic data
def demographic_info(glue_context, dyf):
    """
    Cleans and transforms the demographic data.

    Args:
        glue_context (GlueContext): Glue context object.
        dyf (DynamicFrame): Dynamic frame representing the demographic data.

    Returns:
        DynamicFrame: The transformed dynamic frame.
    """
    dyf = dyf.drop_fields(['DATAFLOW', 'PCHAR: Selected person characteristic', 'REGION_TYPE: Region Type'])
    dyf = dyf.toDF()
    dyf = dyf.filter(col('SEXP: Sex') == '3: Persons')
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    dyf = dyf.withColumn("OBS_VALUE", col("OBS_VALUE").cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    dyf = dyf.drop('SEXP: Sex')
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_D')\
        .withColumnRenamed('STATE: State', 'State')\
        .withColumnRenamed('TIME_PERIOD: Time Period', 'year')\
        .withColumnRenamed('OBS_VALUE', 'Population')
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_demographic")
    return dyf

# Function to clean and transform the weekly rent data
def weekly_rent(glue_context, dyf):
    """
    Cleans and transforms the weekly rent data.

    Args:
        glue_context (GlueContext): Glue context object.
        dyf (DynamicFrame): Dynamic frame representing the weekly rent data.

    Returns:
        DynamicFrame: The transformed dynamic frame.
    """
    dyf = dyf.drop_fields(['DATAFLOW', 'MEDAVG: Median/Average', 'REGION_TYPE: Region Type'])
    dyf = dyf.toDF()
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    dyf = dyf.withColumn('OBS_VALUE', col('OBS_VALUE').cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_WR')\
        .withColumnRenamed('STATE: State', 'State')\
        .withColumnRenamed('TIME_PERIOD: Time Period', 'year')\
        .withColumnRenamed('OBS_VALUE', 'Median_weekly_rent')
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_weekly_rent")
    return dyf

## @params: [JOB_NAME]
params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)
gluecontext = GlueContext(SparkContext.getOrCreate())
job = Job(gluecontext)
if'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"
job.init(jobname, args)

# Create dynamic frames for the median income, demographic, and weekly rent files
dyf_median_income = create_dyf(gluecontext, raw_data_path_dict['income'])
dyf_median_income = household_income(gluecontext, dyf_median_income)

dyf_demographic = create_dyf(gluecontext, raw_data_path_dict['demographic'])
dyf_demographic = demographic_info(gluecontext, dyf_demographic)

dyf_weekly_rent = create_dyf(gluecontext, raw_data_path_dict['rent'])
dyf_weekly_rent = weekly_rent(gluecontext, dyf_weekly_rent)

# Join the three dynamic frames by post code
joined_dyf = Join.apply(dyf_weekly_rent.drop_fields(['State', 'year']),
                        Join.apply(dyf_demographic,
                                   dyf_median_income.drop_fields(['State', 'year']),
                                   'Post_code_D', 'Post_code_HI'),
                        'Post_code_WR', 'Post_code_HI').drop_fields(['Post_code_HI', 'Post_code_WR'])

# Rearrange the order of the columns
joined_dyf = joined_dyf.apply_mapping([('year', 'string', 'year', 'int'),
                                       ('Post_code_D', 'int', 'postcode', 'int'),
                                       ('Median total household income ($/weekly)', 'int', 'median_total_household_income_($/weekly)', 'int'),
                                       ('Median_weekly_rent', 'int', 'median_weekly_rent', 'int'),
                                       ('Population', 'int', 'population', 'int'),
                                       ('State', 'string', 'state', 'string')])

# Save the joined dynamic frame as Parquet
save_as_parquet(gluecontext, joined_dyf, transformed_data_path_dict['suburbinfo'])

job.commit()