#Use this to query the curated dataset in S3 via Athena.
-- Creating external table on top of Parquet data in S3

CREATE EXTERNAL TABLE IF NOT EXISTS airline_curated (
    flight_id INT,
    airline STRING,
    source STRING,
    destination STRING,
    date STRING,
    status STRING,
    price INT
)
STORED AS PARQUET
LOCATION 's3://airline-datalake-ingest/curated/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");