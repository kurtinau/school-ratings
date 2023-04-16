from pyspark.sql.functions import *
from gluejob import GlueJob
from clean import *
from awsglue.transforms import Join

#create a gluejob instance
job = GlueJob()
#create dyf for median income file, run clean function
dyf_median_income = job.create_dyf("s3://school-rating-raw-data/ABS/ABS-Average_household_income.csv")
dyf_median_income = household_income(job.context, dyf_median_income)


#create dyf for demographic, run clean function
dyf_demographic = job.create_dyf("s3://school-rating-raw-data/ABS/ABS-Demographic.csv")
dyf_demographic = demographic_info(job.context, dyf_demographic)

#create dyf for weekly_rent, run clean function
dyf_weekly_rent = job.create_dyf("s3://school-rating-raw-data/ABS/median_weekly_rent_POA_2021.csv")
dyf_weekly_rent = weekly_rent(job.context, dyf_weekly_rent)

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

job.run(joined_dyf,"SuburbInfo_abs")



