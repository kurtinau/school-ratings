from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame


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