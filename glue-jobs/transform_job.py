import logging
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from awsglue.dynamicframe import DynamicFrame # Add this line

# @params: [JOB_NAME, S3_INPUT_PATH, DATABASE_NAME, TARGET_TABLE_NAME, GLUE_CONNECTION_NAME]
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'S3_INPUT_PATH', 'DATABASE_NAME', 'TARGET_TABLE_NAME', 'GLUE_CONNECTION_NAME']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_INPUT_PATH = args['S3_INPUT_PATH']
DATABASE_NAME = args['DATABASE_NAME']
TARGET_TABLE_NAME = args['TARGET_TABLE_NAME']
GLUE_CONNECTION_NAME = args['GLUE_CONNECTION_NAME'] # Name of your Glue JDBC connection

# Define the schema explicitly for the incoming CSV from S3
# This is crucial for consistency and to avoid potential schema inference issues.
# Make sure the field names here match your CSV headers exactly (case-sensitive if applicable).
# The order should also match the order in your CSV.
csv_schema = StructType([
    StructField("anzsic06", StringType(), True),
    StructField("Area", StringType(), True),
    StructField("year", IntegerType(), True), # Original year is an integer
    StructField("geo_count", IntegerType(), True),
    StructField("ec_count", IntegerType(), True)
])

log = logging.getLogger(__name__)
# --- 1. Read Data directly from S3 CSV ---
# Using spark.read.csv for more direct control over schema and options
log.info(f"Reading data from S3 path: {S3_INPUT_PATH}")
s3_df = spark.read.csv(
    S3_INPUT_PATH,
    header=True,       # CSV has a header row
    schema=csv_schema  # Apply the defined schema
)

# Rename columns to match desired database column names (optional, but good practice)
# This mapping needs to be consistent with your STAGING_TABLE_SCHEMA in Airflow
renamed_df = s3_df.withColumnRenamed("anzsic06", "anzsic_code") \
                  .withColumnRenamed("Area", "area_code") \
                  .withColumnRenamed("year", "observation_year") \
                  .withColumnRenamed("geo_count", "geographic_count") \
                  .withColumnRenamed("ec_count", "economic_count")

# Add ingestion timestamp
from pyspark.sql.functions import current_timestamp
final_source_df = renamed_df.withColumn("ingestion_timestamp", current_timestamp())

# --- 2. Transform: Add New Column by Concatenating 'area_code' and 'observation_year' ---
# Ensure 'observation_year' is cast to string for concatenation
transformed_df = final_source_df.withColumn(
    "area_year_combined", concat(col("area_code"), lit("_"), col("observation_year").cast("string"))
)

# Select and reorder columns to match your target staging table exactly
# This is crucial for a smooth write to JDBC.
final_df_to_write = transformed_df.select(
    col("anzsic_code"),
    col("area_code"),
    col("observation_year"),
    col("geographic_count"),
    col("economic_count"),
    col("ingestion_timestamp"),
    col("area_year_combined")
)


# Print schema and show a few rows for debugging in Glue logs
log.info("Transformed DataFrame Schema:")
final_df_to_write.printSchema()
log.info("Transformed DataFrame Sample Rows:")
final_df_to_write.show(5, truncate=False)

# --- 3. Load: Store into Staging Table in Database ---
# Convert PySpark DataFrame back to DynamicFrame for Glue's sink
output_dynamic_frame = DynamicFrame.fromDF(final_df_to_write, glueContext, "output_dynamic_frame")

log.info(f"Writing data to database: {DATABASE_NAME}, table: {TARGET_TABLE_NAME} using connection: {GLUE_CONNECTION_NAME}")
glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="jdbc",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": TARGET_TABLE_NAME,
        "database": DATABASE_NAME,
        "connectionName": GLUE_CONNECTION_NAME
    },
    # connection_name=GLUE_CONNECTION_NAME, # Use the connection name directly
    # Using "overwrite" to ensure the staging table is refreshed with each run.
    # This will truncate and insert or drop/recreate the table depending on your DB and driver.
    # Be cautious with "overwrite" in production; consider "append" or upsert logic if needed.
    # catalog_connection_options={"rewriteTarget": True} # This option is for Glue Data Catalog table updates
)

job.commit()