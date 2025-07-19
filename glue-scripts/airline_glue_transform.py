#Sample AWS Glue ETL PySpark code to transform CSV and write Parquet output.
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())

# Load data from raw S3 location
airline_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://airline-datalake-ingest/raw/"]},
    format="csv",
    format_options={"withHeader": True}
)

# Filter out cancelled flights
filtered_data = Filter.apply(frame=airline_data, f=lambda x: x["status"] != "Canceled")

# Write data to curated S3 location as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=filtered_data,
    connection_type="s3",
    connection_options={"path": "s3://airline-datalake-ingest/curated/"},
    format="parquet"
)