from pyspark.sql.functions import (
    col,
    regexp_extract,
    trim,
    row_number,
    udf,
    concat,
    lit,
    lower,
    trim,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import pandas as pd
import boto3
import sys
import os

from utils import (
    convert_geometry_to_wkt,
    extract_centre_code,
    read_csv,
    read_geo_json,
    read_kml_file,
    read_shape_file,
    read_xlsx,
    save_spark_df_to_s3_as_parquet,
    get_todays_year,
    unary_union_geometry,
)

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
    union_sdf = union_sdf.select([union_sdf.columns[-1]] + union_sdf.columns[:-1])
    return union_sdf


def combine_profile_and_details_df():
    """combine profile and details dataframe into a profile dim table and a school fact table,
        store profile dim table into S3 bucket
    Returns:
        DataFrame: a school fact table
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
        .withColumnRenamed("location_age_id", "location_id")
        .select(
            "school_id",
            "location_id",
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
    school_detail_sdf = school_detail_sdf.drop_duplicates(["school_name", "postcode"])

    union_sdf = join_tables_with_campus_removed(
        profile_sdf, school_detail_sdf, "profile_id"
    )

    profile_dim_table = union_sdf.drop("school_id", "suburb", "postcode", "state")
    print("Profile Dim table count and schema: ", profile_dim_table.count())
    profile_dim_table.printSchema()
    # create tables that fit star schema
    school_fact_table = union_sdf.select(
        "school_id",
        "school_name",
        "profile_id",
        "location_id",
        "suburb",
        "postcode",
        "state",
    )
    ## store profileDim table to s3 bucket
    save_spark_df_to_s3_as_parquet(
        glueContext,
        profile_dim_table,
        transformed_data_path_dict["profile_dim_table"],
    )
    return school_fact_table


def create_naplan_dim_and_bridge_table(school_id_name_sdf):
    """Create a NAPLAN dim table and a school_naplan bridge table
    Args:
        school_id_name_sdf (DataFrame): a spark dataframe that contains ("school_id", "school_name", "suburb", "postcode")
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
        school_id_name_sdf (DataFrame): a spark dataframe that contains ("school_id", "school_name", "suburb", "postcode")
    """
    fees_sdf = (
        read_csv(spark, raw_data_path_dict["fees"]).drop("suburb").drop_duplicates()
    )
    union_sdf = join_tables_with_campus_removed(
        school_id_name_sdf, fees_sdf, "fees_id"
    ).na.drop("any")
    # create a bridage table for schoolFact and feesDim
    school_fees_bridge_table = union_sdf.select(
        "school_id", "fees_id", "boarding", "international"
    ).na.drop(subset=["boarding", "international"])
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


def join_location_and_catchment_on_column(
    location_sdf, catchment_sdf, on_column="school_name"
):
    """Left join location and catchment data

    Args:
        location_sdf (DataFrame): location spark dataframe
        catchment_sdf (DataFrame): catchment spark dataframe
        on_column (str, optional): Join on which column. Defaults to "school_name".

    Returns:
        Dataframe: The result of left join.
    """
    inner_join_sdf = location_sdf.join(catchment_sdf, on_column)
    print("Inner join count: ", inner_join_sdf.count())
    left_join_sdf = location_sdf.join(catchment_sdf, on_column, "left")
    if on_column != "school_name":
        left_join_sdf = left_join_sdf.drop(on_column)
    print("Left join count: ", left_join_sdf.count())
    left_join_sdf = (
        left_join_sdf.drop_duplicates(["school_name", "postcode"])
        .drop("state")
        .drop("school_name")
        .drop("suburb")
        .drop("postcode")
        .drop("ageid")
        .drop("school_id")
    )
    print("Left join count after drop duplicates: ", left_join_sdf.count())
    left_join_sdf.printSchema()
    return left_join_sdf


def merge_duplicate_catchment_rows_based_on_unique_id(catchment_geo_df, unique_id):
    """Use unary_union to union catchment geometry when school has different campus
    Args:
        catchment_geo_df (GeoDataFrame): catchment GeoDataFrame including all campus filter by state.
        unique_id (str): the string of unique ID

    Returns:
        GeoDataFrame: the result of union catchment geometry data.
    """
    duplicated_rows_df = catchment_geo_df[
        catchment_geo_df.duplicated(unique_id, keep=False)
    ]
    if not duplicated_rows_df.empty:
        print("Duplicated row found in catchment data.")
        no_duplicated_rows_df = catchment_geo_df[
            ~catchment_geo_df.duplicated(unique_id, keep=False)
        ]

        grouped_df = duplicated_rows_df.groupby(unique_id).apply(
            lambda x: unary_union_geometry(x)
        )
        combined_geo_df = pd.concat([no_duplicated_rows_df, grouped_df])
        return combined_geo_df
    return catchment_geo_df


def handle_NSW_data():
    """Read NSW catchment data and master_dataset.
    The data flow shows below:
        1. catchment -> USE_ID, geometry
        2. master_dataset -> school_code, ageID
        3. inner join catment and master_dataset on USE_ID == school_code, so that get ageID and geometry.
        4. inner join location on ageID to get the final result
    Returns:
        DataFrame: NSW catchment data
    """
    master_dataset_sdf = read_csv(
        spark, raw_data_path_dict["catchment"]["NSW"]["master_dataset"]
    )
    geo_df_primary = read_shape_file(
        glueContext, raw_data_path_dict["catchment"]["NSW"]["PS_data"]
    )
    geo_df_secondary = read_shape_file(
        glueContext, raw_data_path_dict["catchment"]["NSW"]["HS_data"]
    )
    combined_geo_df = pd.concat(
        [
            geo_df_primary.loc[:, ["USE_ID", "geometry"]],
            geo_df_secondary.loc[:, ["USE_ID", "geometry"]],
        ]
    ).drop_duplicates()
    print("NSW catchment count before merge duplicate: ", combined_geo_df.count())
    combined_geo_df = merge_duplicate_catchment_rows_based_on_unique_id(
        combined_geo_df, "USE_ID"
    )
    geo_sdf = (
        spark.createDataFrame(convert_geometry_to_wkt(combined_geo_df))
        .withColumnRenamed("USE_ID", "school_code")
        .withColumnRenamed("wkt", "catchment")
    )
    print("After NSW catchment: ", geo_sdf.count())
    # print("NSW catchment count: ", spark_df.count())
    # spark_df.printSchema()
    # print("master dataset count: ", master_dataset_sdf.count())
    master_dataset_sdf = master_dataset_sdf.drop_duplicates(["school_code"]).select(
        "school_code", "ageid"
    )
    # master_dataset_sdf.printSchema()
    # print("master dataset count after drop: ", master_dataset_sdf.count())
    join_sdf = geo_sdf.join(master_dataset_sdf, "school_code").drop("school_code")
    print("NSW catchment inner join master_dataset count: ", join_sdf.count())
    # join_sdf.printSchema()
    return join_sdf


def handle_QLD_data():
    """The data flow shows below:
    1.centre_detail -> centre_code, centre_name
    2.catchment -> name, Description, geometry
        2.1 extract centre_code from Description (process html table)
    3.inner join catchment and centre_detail on centre_code ====> centre_name, geometry
    """
    centre_detail_sdf = read_csv(
        spark, raw_data_path_dict["catchment"]["QLD"]["centre_detail"]
    ).drop_duplicates(["centre_code"])
    primary_geo_df = read_kml_file(raw_data_path_dict["catchment"]["QLD"]["PS_data"])
    junior_secondary_geo_df = read_kml_file(
        raw_data_path_dict["catchment"]["QLD"]["JS_data"]
    )
    senior_secondary_geo_df = read_kml_file(
        raw_data_path_dict["catchment"]["QLD"]["SS_data"]
    )
    combined_geo_df = pd.concat(
        [primary_geo_df, junior_secondary_geo_df, senior_secondary_geo_df]
    ).drop_duplicates()
    print("QLD catchment count before merge duplicate: ", combined_geo_df.count())
    combined_geo_df = merge_duplicate_catchment_rows_based_on_unique_id(
        combined_geo_df, "Name"
    )
    print("QLD catchment count after merge duplicate: ", combined_geo_df.count())
    geo_sdf = (
        spark.createDataFrame(convert_geometry_to_wkt(combined_geo_df))
        .drop("Name")
        .withColumnRenamed("wkt", "catchment")
    )
    get_centre_code = udf(lambda z: extract_centre_code(z), StringType())
    geo_sdf = geo_sdf.withColumn(
        "centre_code", get_centre_code(col("Description"))
    ).drop("Description")
    geo_sdf.printSchema()
    join_df = (
        geo_sdf.join(centre_detail_sdf, "centre_code")
        .select("centre_name", "catchment")
        .withColumnRenamed("centre_name", "school_name")
    )
    print("QLD catchment inner join centre_detail count: ", join_df.count())
    join_df.printSchema()
    return join_df


def handle_SA_data():
    """The data flow shows below:
    1. decd_sites -> ORG_UNIT_NO, FULL_NAME
    2. catchment -> ORG_NUM, geometry
    3. inner join catchemnt and decd_sites to get ---> FULL_NAME, geometry
    """
    decd_sdf = read_xlsx(
        glueContext,
        raw_data_path_dict["catchment"]["SA"]["decd_sites"],
        2,
        0,
    )
    geo_df_primary = read_shape_file(
        glueContext,
        raw_data_path_dict["catchment"]["SA"]["PS_data"],
    )
    geo_df_secondary = read_shape_file(
        glueContext,
        raw_data_path_dict["catchment"]["SA"]["HS_data"],
    )

    combined_geo_df = pd.concat(
        [
            geo_df_primary.loc[:, ["ORG_NUM", "geometry"]],
            geo_df_secondary.loc[:, ["ORG_NUM", "geometry"]],
        ]
    ).drop_duplicates()
    print("SA catchment count before merge duplicate: ", combined_geo_df.count())
    combined_geo_df = merge_duplicate_catchment_rows_based_on_unique_id(
        combined_geo_df, "ORG_NUM"
    )
    geo_sdf = (
        spark.createDataFrame(convert_geometry_to_wkt(combined_geo_df))
        .withColumnRenamed("ORG_NUM", "org_id")
        .withColumnRenamed("wkt", "catchment")
    )
    print("After SA catchment: ", geo_sdf.count())
    decd_sdf = (
        decd_sdf.drop_duplicates(["org_unit_no"])
        .select("org_unit_no", "full_name")
        .withColumnRenamed("org_unit_no", "org_id")
        .withColumnRenamed("full_name", "school_name")
    )
    join_df = geo_sdf.join(decd_sdf, "org_id").drop("org_id")
    print("SA catchment inner join decd_sites count and schema: ", join_df.count())
    join_df.printSchema()
    return join_df


def handle_VIC_data():
    """The VIC dataset has full school name, no need further process.
    catchment -> School_Name, geometry
    Find duplicates row for all catchment data and unary_union geometry
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(raw_data_path_dict["catchment"]["VIC"]["bucket_name"])
    prefix_objs = bucket.objects.filter(
        Prefix=raw_data_path_dict["catchment"]["VIC"]["data_prefix"]
    )
    combined_geo_df = pd.DataFrame()
    for obj in prefix_objs:
        key = obj.key
        if ".geojson" in key:
            file_path = (
                "s3://"
                + raw_data_path_dict["catchment"]["VIC"]["bucket_name"]
                + "/"
                + key
            )
            geo_df = read_geo_json(file_path)
            combined_geo_df = pd.concat([combined_geo_df, geo_df])
    if not combined_geo_df.empty:
        # combined_geo_df = combined_geo_df.drop_duplicates().astype(
        #     {"Boundary_Year": "string"}
        # )
        combined_geo_df = combined_geo_df.drop_duplicates()
        combined_geo_df = combined_geo_df.loc[
            :, ["ENTITY_CODE", "School_Name", "Campus_Name", "geometry"]
        ]
        print("VIC catchment count before merge duplicate: ", combined_geo_df.count())
        combined_geo_df = merge_duplicate_catchment_rows_based_on_unique_id(
            combined_geo_df, "ENTITY_CODE"
        )
        geo_sdf = (
            spark.createDataFrame(convert_geometry_to_wkt(combined_geo_df))
            .withColumnRenamed("wkt", "catchment")
            .withColumnRenamed("School_Name", "school_name")
            .drop("ENTITY_CODE")
            .drop("Campus_Name")
        )
        print("After VIC catchment: ", geo_sdf.count())
        # geo_sdf = (
        #     geo_sdf.withColumn(
        #         "school_name",
        #         when(
        #             trim(lower(col("School_Name"))) != trim(lower(col("Campus_Name"))),
        #             concat(col("School_Name"), lit("-"), col("Campus_Name")),
        #         ).otherwise(col("School_Name")),
        #     )
        #     .withColumnRenamed("School_Name", "school_name")
        #     .drop("ENTITY_CODE")
        #     .drop("Campus_Name")
        # )
        geo_sdf.printSchema()

        return geo_sdf
    else:
        return combined_geo_df


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

raw_bucket_prefix = "s3://school-rating-raw-data"
silver_bucket_prefix = "s3://school-rating-raw-data/stage-test1"
raw_data_path_dict = {
    "profile": raw_bucket_prefix
    + "/school-profile/input/school-profile-202111812f404c94637ead88ff00003e0139.xlsx",
    "detail": raw_bucket_prefix + "/web-crawler/detail/detail.csv",
    "naplan": raw_bucket_prefix + "/web-crawler/naplan/naplan.csv",
    "fees": raw_bucket_prefix + "/web-crawler/fees/fees.csv",
    "location": raw_bucket_prefix
    + "/school-location/input/school-location-2021e23a2f404c94637ead88ff00003e0139.xlsx",
    "catchment": {
        "NSW": {
            "master_dataset": raw_bucket_prefix
            + "/catchment/input/nsw/master_dataset.csv",
            "PS_data": raw_bucket_prefix
            + "/catchment/input/nsw/primary/catchments_primary.shp",
            "HS_data": raw_bucket_prefix
            + "/catchment/input/nsw/secondary/catchments_secondary.shp",
        },
        "QLD": {
            "centre_detail": raw_bucket_prefix
            + "/catchment/input/qld/centredetails_may_2020.csv",
            "PS_data": raw_bucket_prefix
            + "/catchment/input/qld/primary_catchments_2022.kml",
            "JS_data": raw_bucket_prefix
            + "/catchment/input/qld/junior_secondary_catchments_2022.kml",
            "SS_data": raw_bucket_prefix
            + "/catchment/input/qld/senior_secondary_catchmets_2022.kml",
        },
        "SA": {
            "decd_sites": raw_bucket_prefix
            + "/catchment/input/sa/datasalistofsitesandservicesjune2017.xlsx",
            "PS_data": raw_bucket_prefix
            + "/catchment/input/sa/primary/PrimarySchoolZones2023EY.shp",
            "HS_data": raw_bucket_prefix
            + "/catchment/input/sa/secondary/HighSchoolZones2023EY.shp",
        },
        "VIC": {
            "bucket_name": "school-rating-raw-data",
            "data_prefix": "catchment/input/vic",
        },
    },
}
transformed_data_path_dict = {
    "school_middle_table": "s3://school-rating-raw-data/output/school_id_name",
    "school_fact_table": silver_bucket_prefix
    + "/gov-website/au/school/year="
    + get_todays_year(),
    "profile_dim_table": silver_bucket_prefix
    + "/gov-website/au/profile/year="
    + get_todays_year(),
    "location_dim_table": {
        "NSW": silver_bucket_prefix
        + "/gov-website/au/location/year="
        + get_todays_year()
        + "/state=NSW",
        "QLD": silver_bucket_prefix
        + "/gov-website/au/location/year="
        + get_todays_year()
        + "/state=QLD",
        "SA": silver_bucket_prefix
        + "/gov-website/au/location/year="
        + get_todays_year()
        + "/state=SA",
        "VIC": silver_bucket_prefix
        + "/gov-website/au/location/year="
        + get_todays_year()
        + "/state=VIC",
    },
    "school_naplan_bridge_table": silver_bucket_prefix
    + "/scrapy/au/school_naplan/year="
    + get_todays_year(),
    "napalan_dim_table": silver_bucket_prefix
    + "/scrapy/au/naplan/year="
    + get_todays_year(),
    "school_fees_bridge_table": silver_bucket_prefix
    + "/scrapy/au/school_fees/year="
    + get_todays_year(),
    "fees_dim_table": silver_bucket_prefix
    + "/scrapy/au/fees/year="
    + get_todays_year(),
}

# # keep school fact table to be used later
school_fact_table = combine_profile_and_details_df()
# # create middle table for other glue jobs to use
school_id_name_sdf = school_fact_table.select(
    "school_id", "school_name", "suburb", "postcode"
)

## save school fact table
save_spark_df_to_s3_as_parquet(
    glueContext,
    school_fact_table,
    transformed_data_path_dict["school_fact_table"],
)

create_naplan_dim_and_bridge_table(school_id_name_sdf)
create_fees_dim_and_bridge_table(school_id_name_sdf)

##### Deal with location and catchment data
location_sdf = read_xlsx(
    glueContext,
    raw_data_path_dict["location"],
)
location_sdf = (
    location_sdf.select(
        "acara_sml_id",
        "location_age_id",
        "school_name",
        "state",
        "suburb",
        "postcode",
        "latitude",
        "longitude",
        "local_government_area",
        "local_government_area_name",
        "abs_remoteness_area_name",
    )
    .withColumnRenamed("acara_sml_id", "school_id")
    .withColumnRenamed("local_government_area", "local_government_code")
    .withColumnRenamed("local_government_area_name", "local_government")
    .withColumnRenamed("abs_remoteness_area_name", "geolocation")
    .withColumnRenamed("location_age_id", "ageid")
    .drop_duplicates(["suburb", "school_name", "postcode"])
)

print("location count:: ", location_sdf.count())
location_sdf = location_sdf.join(school_id_name_sdf.select("school_id"), "school_id")
location_sdf = location_sdf.withColumn(
    "location_id", row_number().over(Window.orderBy("school_id"))
)
# reorder location dataframe to get location_id at the beginning
location_sdf = location_sdf.select(
    [location_sdf.columns[-1]] + location_sdf.columns[:-1]
)
# print("location count and schema after:: ", location_sdf.count())

###NSW catchment-location
print("-------------------------NSW----------------------------")
nsw_location_sdf = location_sdf.filter(location_sdf.state == "NSW")
print("NSW location count: ", nsw_location_sdf.count())
nsw_catchment_sdf = handle_NSW_data()
nsw_transformed_sdf = join_location_and_catchment_on_column(
    nsw_location_sdf, nsw_catchment_sdf, on_column="ageid"
)
print("NSW transformed count and schema: ", nsw_transformed_sdf.count())
nsw_transformed_sdf.printSchema()


###QLD catchment-location
print("-------------------------QLD----------------------------")
qld_location_sdf = location_sdf.filter(location_sdf.state == "QLD")
print("QLD location count: ", qld_location_sdf.count())
qld_catchment_sdf = handle_QLD_data()
qld_transformed_sdf = join_location_and_catchment_on_column(
    qld_location_sdf, qld_catchment_sdf
)
print("QLD transformed count and schema: ", qld_transformed_sdf.count())
qld_transformed_sdf.printSchema()


###SA catchment-location
print("-------------------------SA----------------------------")
sa_location_sdf = location_sdf.filter(location_sdf.state == "SA")
print("SA location count: ", sa_location_sdf.count())
sa_catchment_sdf = handle_SA_data()
sa_transformed_sdf = join_location_and_catchment_on_column(
    sa_location_sdf, sa_catchment_sdf
)
print("SA transformed count and schema: ", sa_transformed_sdf.count())
sa_transformed_sdf.printSchema()

###VIC catchment-location
print("-------------------------VIC----------------------------")
vic_location_sdf = location_sdf.filter(location_sdf.state == "VIC")
print("VIC location count: ", vic_location_sdf.count())
vic_catchment_sdf = handle_VIC_data()
vic_transformed_sdf = join_location_and_catchment_on_column(
    vic_location_sdf, vic_catchment_sdf
)
print("VIC transformed count and schema: ", vic_transformed_sdf.count())
vic_transformed_sdf.printSchema()

## Save catchment data to S3 bucket
save_spark_df_to_s3_as_parquet(
    glueContext,
    nsw_transformed_sdf,
    transformed_data_path_dict["location_dim_table"]["NSW"],
)
save_spark_df_to_s3_as_parquet(
    glueContext,
    qld_transformed_sdf,
    transformed_data_path_dict["location_dim_table"]["QLD"],
)
save_spark_df_to_s3_as_parquet(
    glueContext,
    sa_transformed_sdf,
    transformed_data_path_dict["location_dim_table"]["SA"],
)
save_spark_df_to_s3_as_parquet(
    glueContext,
    vic_transformed_sdf,
    transformed_data_path_dict["location_dim_table"]["VIC"],
)

job.commit()
