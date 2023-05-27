from awsglue.dynamicframe import DynamicFrame
import pyspark.pandas as ps
from pyspark.sql.functions import col
from datetime import datetime


def read_xlsx(glue_context, path, header=0, sheet_num=1):
    # read excel xlsx file
    pandas_on_spark_df = ps.read_excel(path, sheet_name=sheet_num, header=header)
    spark_df = pandas_on_spark_df.to_spark()
    # create a PySpark DataFrame with the correct schema and column names
    spark_df = spark_df.select(
        [col(c).alias(c.lower().replace(" ", "_")) for c in spark_df.columns]
    )
    return spark_df


def read_csv(spark, path):
    spark_df = spark.read.option("header", True).csv(path)
    spark_df = spark_df.select(
        [col(c).alias(c.lower().replace(" ", "_")) for c in spark_df.columns]
    )
    return spark_df


def read_parquet(spark, path):
    spark_df = spark.read.parquet(path)
    return spark_df


def save_spark_df_to_s3_as_parquet(glue_context, spark_df, path):
    # # convert spark data frame to dynamic dataFrame
    dydf = DynamicFrame.fromDF(spark_df, glue_context, "dydf")
    dydf = dydf.coalesce(2)
    glue_context.write_dynamic_frame_from_options(
        frame=dydf,
        connection_type="s3",
        connection_options={"path": path},
        format="parquet",
    )


def get_todays_year():
    now = datetime.now()
    year_format = "%Y"
    return now.strftime(year_format)
