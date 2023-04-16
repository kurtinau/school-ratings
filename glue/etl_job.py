from pyspark.sql.functions import (
    col,
    regexp_extract,
    trim,
    row_number,
)
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import sys
import os

from utils import read_csv, read_xlsx, save_spark_df_to_s3_as_parquet, get_todays_year

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

def join_tables_with_campus_removed(left_table, right_table, dim_table_id_str):
    """Based on profile table, join other tables on "school_name", "postcode"
        1. inner join two tables
        2. use left anti and right anti join to find rows that's not match
        3. remove campus name that from right anti table
    Args:
        left_table (spark dataframe): left table that at least contains "school_id","school_name"
        right_table (spark dataframe): other features that should be joined in profile table
        dim_table_id_str (string): dim table primary key name
    Returns:
        Dataframe: a transformed data frame
    """
    inner_join_sdf = left_table.join(
        right_table, ["school_name", "postcode"]
    ).drop_duplicates(["school_name", "postcode"])
    left_anti_sdf = left_table.join(
        right_table, ["school_name", "postcode"], "leftanti"
    )
    right_anti_sdf = right_table.join(
        left_table, ["school_name", "postcode"], "leftanti"
    )

    right_anti_sdf_with_campus = right_anti_sdf.filter(col("school_name").like("% - %"))
    right_anti_sdf_without_campus = right_anti_sdf_with_campus.withColumn(
        "school_name", trim(regexp_extract("school_name", r'^"?(.+?)\s+-\s+', 1))
    )
    anti_combined = left_anti_sdf.join(
        right_anti_sdf_without_campus, ["school_name", "postcode"], "left"
    )

    union_sdf = inner_join_sdf.union(anti_combined).drop_duplicates(
        ["school_name", "postcode"]
    )
    # create ID for dim table
    union_sdf = union_sdf.withColumn(
        dim_table_id_str, row_number().over(Window.orderBy("school_id"))
    )
    # reorder dataframe columns to let ID be the first column
    union_sdf = union_sdf.select([union_sdf.columns[-1]]+union_sdf.columns[:-1])
    return union_sdf

def combine_profile_and_details_df():
    """combine profile and details dataframe into a profile dim table and a school fact table,
        store profile dim table into S3 bucket
    Returns:
        Dataframe: a school fact table
    """
    profile_sdf = read_xlsx(
        glueContext,
        raw_data_path_dict["profile"],
    )
    profile_sdf = (
        profile_sdf.withColumnRenamed("acara_sml_id", "school_id")
        .withColumnRenamed("indigenous_enrolments_(%)", "indigenous")
        .withColumnRenamed(
            "language_background_other_than_english_-_yes_(%)", "multi_lingual"
        )
        .select(
            "school_id",
            "school_name",
            "school_type",
            "campus_type",
            "school_sector",
            "school_url",
            "governing_body",
            "year_range",
            "teaching_staff",
            "total_enrolments",
            "girls_enrolments",
            "boys_enrolments",
            "indigenous",
            "multi_lingual",
            "suburb",
            "postcode",
            "state",
        )
    )

    # Read detail file from website crawling
    school_detail_sdf = read_csv(spark, raw_data_path_dict["detail"])
    school_detail_sdf = school_detail_sdf.select(
        "school_name", "postcode", "gender", "religion"
    )
    # school_detail_sdf.printSchema()
    school_detail_sdf = school_detail_sdf.drop_duplicates(["school_name", "postcode"])

    union_sdf = join_tables_with_campus_removed(
        profile_sdf, school_detail_sdf, "profile_id"
    )

    profile_dim_table = union_sdf.drop("school_id", "suburb", "postcode", "state")
    print("Profile Dim table count and schema: ", profile_dim_table.count())
    profile_dim_table.printSchema()
    # create tables that fit star schema
    school_fact_table = union_sdf.select(
        "school_id", "school_name", "profile_id", "suburb", "postcode", "state"
    )
    # store profileDim table to s3 bucket
    save_spark_df_to_s3_as_parquet(
        glueContext,
        profile_dim_table,
        transformed_data_path_dict["profile_dim_table"],
    )
    return school_fact_table

def create_naplan_dim_and_bridge_table(school_id_name_sdf):
    """Create a NAPLAN dim table and a school_naplan bridge table
    Args:
        school_id_name_sdf (Dataframe): a spark dataframe that contains ("school_id", "school_name", "suburb", "postcode")
    """
    naplan_sdf = (
        read_csv(spark, raw_data_path_dict["naplan"]).drop("suburb").drop_duplicates()
    )
    union_sdf = join_tables_with_campus_removed(
        school_id_name_sdf, naplan_sdf, "naplan_id"
    )
    # create a bridage table for schoolFact and naplanDim
    school_naplan_bridge_table = union_sdf.select(
        "school_id", "naplan_id", "grade"
    ).na.drop(subset=["grade"])
    print(
        "school naplan bridage table schema and count: ",
        school_naplan_bridge_table.count(),
    )
    school_naplan_bridge_table.printSchema()
    # create a naplan dim table
    naplan_dim_table = union_sdf.drop(
        "school_name", "postcode", "suburb", "school_id", "year", "grade"
    ).na.drop(subset=["reading", "writing", "spelling", "grammar", "numeracy"])
    print("naplan dim table schema and count: ", naplan_dim_table.count())
    naplan_dim_table.printSchema()
    # store school_naplan bridge table to s3
    save_spark_df_to_s3_as_parquet(
        glueContext,
        school_naplan_bridge_table,
        transformed_data_path_dict["school_naplan_bridge_table"],
    )
    # store naplan dim table to s3
    save_spark_df_to_s3_as_parquet(
        glueContext,
        naplan_dim_table,
        transformed_data_path_dict["napalan_dim_table"],
    )

def create_fees_dim_and_bridge_table(school_id_name_sdf):
    """Create a fees dim table and a school_fees bridge table

    Args:
        school_id_name_sdf (Dataframe): a spark dataframe that contains ("school_id", "school_name", "suburb", "postcode")
    """
    fees_sdf = (
        read_csv(spark, raw_data_path_dict["fees"]).drop("suburb").drop_duplicates()
    )
    union_sdf = join_tables_with_campus_removed(school_id_name_sdf, fees_sdf, "fees_id").na.drop("any")
    # create a bridage table for schoolFact and feesDim
    school_fees_bridge_table = union_sdf.select(
        "school_id", "fees_id", "boarding", "international"
    ).na.drop(subset=["boarding","international"])
    print(
        "school fees bridage table schema and count: ",
        school_fees_bridge_table.count(),
    )
    school_fees_bridge_table.printSchema()
    # create a fees dim table
    fees_dim_table = union_sdf.drop(
        "school_name",
        "postcode",
        "suburb",
        "school_id",
        "boarding",
        "international",
    )
    print("fees dim table schema and count: ", fees_dim_table.count())
    fees_dim_table.printSchema()
    # store school_fees bridge table to s3
    save_spark_df_to_s3_as_parquet(
        glueContext,
        school_fees_bridge_table,
        transformed_data_path_dict["school_fees_bridge_table"],
    )
    # store fess dim table to s3
    save_spark_df_to_s3_as_parquet(
        glueContext,
        fees_dim_table,
        transformed_data_path_dict["fees_dim_table"],
    )


## @params: [JOB_NAME]
params = []
if "--JOB_NAME" in sys.argv:
    params.append("JOB_NAME")
args = getResolvedOptions(sys.argv, params)
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
if "JOB_NAME" in args:
    jobname = args["JOB_NAME"]
else:
    jobname = "test"
job.init(jobname, args)

raw_data_path_dict = {
    "profile": "s3://school-rating-raw-data/school-profile/input/school-profile-202111812f404c94637ead88ff00003e0139.xlsx",
    "detail": "s3://school-rating-raw-data/web-crawler/detail/detail.csv",
    "naplan": "s3://school-rating-raw-data/web-crawler/naplan/naplan.csv",
    "fees": "s3://school-rating-raw-data/web-crawler/fees/fees.csv",
}
transformed_data_path_dict = {
    "school_middle_table": "s3://school-rating-raw-data/output/school_id_name",
    "profile_dim_table": "s3://school-rating-raw-data/stage-test/gov-website/au/profile/year="
    + get_todays_year(),
    "school_naplan_bridge_table": "s3://school-rating-raw-data/stage-test/scrapy/au/school_naplan/year="
    + get_todays_year(),
    "napalan_dim_table": "s3://school-rating-raw-data/stage-test/scrapy/au/naplan/year="
    + get_todays_year(),
    "school_fees_bridge_table": "s3://school-rating-raw-data/stage-test/scrapy/au/school_fees/year="
    + get_todays_year(),
    "fees_dim_table": "s3://school-rating-raw-data/stage-test/scrapy/au/fees/year="
    + get_todays_year(),
}

temp_sdf = combine_profile_and_details_df()
# create middle table for other glue jobs to use
school_id_name_sdf = temp_sdf.select("school_id", "school_name", "suburb", "postcode")
# keep school fact table to be used later
school_fact_table = temp_sdf.drop("school_name")

create_naplan_dim_and_bridge_table(school_id_name_sdf)
create_fees_dim_and_bridge_table(school_id_name_sdf)

job.commit()
