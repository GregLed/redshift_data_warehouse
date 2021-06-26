## Project summary
Building data warehouse on the AWS Redshift cluster. Raw data is available on public AWS S3 buckets:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

## Project steps
1. Create Redshift cluster programmatically and insert cluster and IAM role details into the dwh.cfg config file
2. Create staging tables for the raw data and analytical tables using the star schema - run create_tables.py file
3. Execute the ETL pipeline by loading the raw data into staging tables and then moving it into the star schema tables - run etl.py file (see details in ETL pipeline steps)

## ETL pipeline steps
1. Load song and log datasets from S3 buckets into staging tables
2. Transform the raw data into analytical tables optimized for queries (with distkeys and sortkeys)

## Database schema
| Table | Description |
| ---- | ---- |
| staging_events | stating table for log events dataset (raw format from S3 bucket) |
| staging_songs | staging table for songs dataset (raw format from S3 bucket) |
| songplays | fact table for played songs (played by who, when on which devise etc.) | 
| users | dimensional table for users (names, gender and level) | 
| songs | dimensional table for songs (artist, title, year and duration) | 
| artists | dimensional table for artists (name and location info) | 
| time | timestamps breakdown | 