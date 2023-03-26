from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
import pyspark.pandas as ps
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import sys
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'


class ProfileTransform:
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

    def run(self):
        profile_dyf = read_xlsx(
            self.context, 's3://school-rating-raw-data/school-profile/input/school-profile-202111812f404c94637ead88ff00003e0139.xlsx')
        profile_dyf = profile_dyf.select_fields(['acara_sml_id', 'school_name', 'suburb', 'state', 'postcode', 'school_type', 'campus_type', 'school_url', 'governing_body', 'year_range', 'geolocation', 'teaching_staff', 'total_enrolments', 'girls_enrolments', 'boys_enrolments',
                                                'indigenous_enrolments_(%)', 'language_background_other_than_english_-_yes_(%)']).rename_field('acara_sml_id', 'school_id').rename_field('indigenous_enrolments_(%)', 'indigenous').rename_field('language_background_other_than_english_-_yes_(%)', 'multi_lingual')
        profile_dyf.printSchema()

        location_dyf = read_xlsx(
            self.context, 's3://school-rating-raw-data/school-location/input/school-location-2021e23a2f404c94637ead88ff00003e0139.xlsx')
        location_dyf = location_dyf.select_fields(['acara_sml_id', 'latitude', 'longitude', 'local_government_area_name']).rename_field(
            'acara_sml_id', 'location_school_id').rename_field('local_government_area_name', 'local_government')
        location_dyf.printSchema()

        # join profile and location together
        school_dyf = profile_dyf.join(paths1=["school_id"], paths2=[
                                      "location_school_id"], frame2=location_dyf).drop_fields(['location_school_id'])
        school_dyf.printSchema()
        self.context.write_dynamic_frame_from_options(
            frame=school_dyf, connection_type="s3", connection_options={"path": "s3://school-rating-raw-data/output/profile"}, format="parquet")

        self.job.commit()


def read_xlsx(glue_context, path):
    # read excel xlsx file
    pandas_on_spark_df = ps.read_excel(path, sheet_name=1)
    # convert the Pandas-on-Spark DataFrame to a PySpark DataFrame
    spark_df = pandas_on_spark_df.to_spark()
    # create a PySpark DataFrame with the correct schema and column names
    spark_df = spark_df.select([col(c).alias(c.lower().replace(' ', '_'))
                               for c in spark_df.columns])
    df = DynamicFrame.fromDF(spark_df, glue_context, 'df')
    return df


if __name__ == '__main__':
    ProfileTransform().run()
