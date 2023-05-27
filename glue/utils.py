from awsglue.dynamicframe import DynamicFrame
import pyspark.pandas as ps
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.sql.functions import col
from datetime import datetime
import geopandas as gpd
import pandas as pd
import fiona


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


def read_shape_file(glue_context, path):
    # get geoDataFrame from shape file
    gdf = gpd.read_file(path)
    return gdf


def read_geo_json(path):
    gdf = gpd.read_file(path)
    return gdf


def read_kml_file(path):
    fiona.supported_drivers["KML"] = "rw"
    gdf = gpd.read_file(path, driver="KML")
    return gdf


def fuzzy_match_names(df_1, df_2):
    pipeline = Pipeline(
        stages=[
            RegexTokenizer(
                pattern="", inputCol="school_name", outputCol="tokens", minTokenLength=1
            ),
            NGram(n=3, inputCol="tokens", outputCol="ngrams"),
            HashingTF(inputCol="ngrams", outputCol="vectors"),
            MinHashLSH(inputCol="vectors", outputCol="lsh"),
        ]
    )

    model = pipeline.fit(df_1)

    stored_hashed = model.transform(df_1)
    landed_hashed = model.transform(df_2)

    matched_df = (
        model.stages[-1]
        .approxSimilarityJoin(stored_hashed, landed_hashed, 1.0, "confidence")
        .select(
            col("datasetA.school_name"), col("datasetB.school_name"), col("confidence")
        )
    )
    matched_df.sort(matched_df.confidence.asc()).show(500, False)
    # matched_df.show(100, False)


def extract_centre_code(str):
    # print(str)
    df_list = pd.read_html(str)
    df = df_list[0]
    df.head(1)
    if not df.empty:
        code = df[1][1]
        if code:
            return code.lstrip("0")
    return ""


def unary_union_geometry(df):
    # 1. keep other features except geometry
    # 2. create a geoseries with geometry
    # 3. get geoseries unary_union feature
    unary_union_geo_df = df.iloc[[0]].drop("geometry", axis=1)
    geos = gpd.GeoSeries(df["geometry"])
    unary_union_geo_df["geometry"] = geos.unary_union
    return unary_union_geo_df


def convert_geometry_to_wkt(geo_df):
    geo_df["wkt"] = pd.Series(
        map(lambda geom: str(geom.wkt), geo_df["geometry"]),
        index=geo_df.index,
        dtype="string",
    )
    result_df = geo_df.drop("geometry", axis=1)
    return result_df
