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
raw_data_path_dict = {
    "income": "s3://school-rating-raw-data/ABS/ABS-Average_household_income.csv",
    "demographic": "s3://school-rating-raw-data/ABS/ABS-Demographic.csv",
    "rent": "s3://school-rating-raw-data/ABS/median_weekly_rent_POA_2021.csv"
}

transformed_data_path_dict = {
    "suburbinfo": "s3://school-rating-silver-apsourheast2-dev/suburbinfo"

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


def household_income(glue_context, dyf):
    #remove DATAFLOW, MEDAVG: Median/Average, REGION_TYPE: Region Type columns
    dyf = dyf.drop_fields(['DATAFLOW','MEDAVG: Median/Average', 'REGION_TYPE: Region Type'])
    # Convert dynamic frame to PySpark DataFrame
    dyf = dyf.toDF()
    #clean post code column and only choose the first four digits
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    #clean state column and remove '5:'
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    #cast the value column as integer data type
    dyf = dyf.withColumn("OBS_VALUE", col("OBS_VALUE").cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    #rename the columns
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_HI').withColumnRenamed('STATE: State', 'State').withColumnRenamed('TIME_PERIOD: Time Period', 'year').withColumnRenamed('OBS_VALUE', 'Median total household income ($/weekly)')
    # convert the resulting DataFrame back to a dynamicframe
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_householdincome")
    return dyf

def demographic_info(glue_context, dyf):
    
    #remove DATAFLOW, PCHAR: Selected person characteristic	,REGION_TYPE: Region Type columns
    dyf = dyf.drop_fields(['DATAFLOW','PCHAR: Selected person characteristic', 'REGION_TYPE: Region Type'])
    # Convert dynamic frame to PySpark DataFrame
    dyf = dyf.toDF()
    #filter only 'Sex' == 3: Persons
    dyf = dyf.filter(col('SEXP: Sex') == '3: Persons')
    #clean post code column and only choose the first four digits
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    #clean state column and remove '5:'
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    #cast the value column as integer data type
    dyf = dyf.withColumn("OBS_VALUE", col("OBS_VALUE").cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    #drop sex column
    dyf = dyf.drop('SEXP: Sex')
    #rename the columns
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_D').withColumnRenamed('STATE: State', 'State')\
        .withColumnRenamed('TIME_PERIOD: Time Period', 'year').withColumnRenamed('OBS_VALUE', 'Population')
    # convert the resulting DataFrame back to a dynamicframe
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_demographic")
    return dyf

def weekly_rent(glue_context, dyf):

    #remove DATAFLOW, MEDAVG: Median/Average, REGION_TYPE: Region Type columns
    dyf = dyf.drop_fields(['DATAFLOW','MEDAVG: Median/Average', 'REGION_TYPE: Region Type'])
    # Convert dynamic frame to PySpark DataFrame
    dyf = dyf.toDF()
    #clean post code column and only choose the first four digits
    dyf = dyf.withColumn('REGION: Region', substring('REGION: Region', 1, 4))
    #clean state column and remove '5:'
    dyf = dyf.withColumn('STATE: State', trim(split('STATE: State', ':')[1]))
    #cast the median household income column as integer data type
    dyf = dyf.withColumn('OBS_VALUE', col('OBS_VALUE').cast("integer"))
    dyf = dyf.withColumn('REGION: Region', col('REGION: Region').cast("integer"))
    #rename the columns
    dyf = dyf.withColumnRenamed('REGION: Region', 'Post_code_WR').withColumnRenamed('STATE: State', 'State').withColumnRenamed('TIME_PERIOD: Time Period', 'year').withColumnRenamed('OBS_VALUE', 'Median_weekly_rent')
    # convert the resulting DataFrame back to a dynamicframe
    dyf = DynamicFrame.fromDF(dyf, glue_context, "transformed_weekly_rent")
    return dyf

## @params: [JOB_NAME]
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

#create dyf for median income file, run clean function
dyf_median_income = create_dyf(gluecontext,raw_data_path_dict['income'])
dyf_median_income = household_income(gluecontext, dyf_median_income)


#create dyf for demographic, run clean function
dyf_demographic = create_dyf(gluecontext,raw_data_path_dict['demographic'])
dyf_demographic = demographic_info(gluecontext, dyf_demographic)

#create dyf for weekly_rent, run clean function
dyf_weekly_rent = create_dyf(gluecontext,raw_data_path_dict['rent'])
dyf_weekly_rent = weekly_rent(gluecontext, dyf_weekly_rent)

#join the three dyf by post code
joined_dyf = Join.apply(dyf_weekly_rent.drop_fields(['State','year']), 
                        Join.apply(dyf_demographic,
                                   dyf_median_income.drop_fields(['State','year']), 
                                   'Post_code_D', 'Post_code_HI'), 
                                   'Post_code_WR', 'Post_code_HI').drop_fields(['Post_code_HI', 'Post_code_WR'])
#rearrange order of the columns
joined_dyf = joined_dyf.apply_mapping([('year', 'string', 'year', 'int'), ('Post_code_D', 'int', 'postcode', 'int'), 
           ('Median total household income ($/weekly)', 'int', 'median_total_household_income_($/weekly)', 'int'),
           ('Median_weekly_rent', 'int', 'median_weekly_rent', 'int'),
           ('Population', 'int', 'population', 'int'), ('State', 'string', 'state', 'string')])

save_as_parquet(gluecontext,joined_dyf,transformed_data_path_dict['suburbinfo'])

job.commit()

