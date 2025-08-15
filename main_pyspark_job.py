
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_DATA_SOURCE_PATH = 'gs://us-central1-composer-airflo-f3b665a4-bucket/data' # location of csv data file
S3_DATA_OUTPUT_PATH = 'gs://us-central1-composer-airflo-f3b665a4-bucket/output_files' # output files saving location

def func_run():
  spark = SparkSession.builder.appName('hmda_app').getOrCreate()
  all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
  selected_data = all_data.select('loan_purpose_name', 'county_name','county_code','applicant_ethnicity','applicant_sex_name','applicant_income_000s')
  selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
  print('Total number of records: %s' % all_data.count())

if __name__ == "__main__":
    func_run()
