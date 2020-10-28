# Project: Data Warehouse

## Description
The purpose of this project is to create an airflow dag and associated custom 
operators to creat a Redshift database of song and user 
activity for Sparkify's new music streaming app.  This will allow them to 
analyze their data which currently resides on S3 in a directory of JSON logs and 
JSON metadata.

JSON log and song data files are copied from S3 to staging tables in Redshift, then 
inserted into analytic tables to allow simple SQL queries for data analysis.

Project was tested utilizing a dc2.large Redshift cluster with 4 nodes. Staging tables took approximately 8 minutes to copy. Fact and dimension tables were created quickly afterword.

## Files

### create_tables.sql
This SQL must be run on the Redshift database once at initial setup. Creates `staging_events`, `staging_songs`, `songplay`, `artists`,`songs`, `time`, and `user` tables.

### ./dag/sparkify_etl.py
Airflow dag which properly orders and schedules tasks to stage data from S3 to Redshift, creates fact and dimension tables and then runs user defined data quality checks

### ./plugins/helpers/sql_queries.py
Contains SQL queries used by operators to insert data into 
redshift tables.

### ./plugins/operators/data_quality.py
Custom operator which runs user defined SQL checks against an expected result.

Arguments:

`redshift_conn_id` = Airflow Connection to Redshift  
`tests` = List of Dictionarys including an SQL test and expected result. Example format:  
` [ {'test': 'SELECT ... FROM', 'expected_result': '0'}, {'test': 'SELECT ... WHERE ... FROM', 'expected_result': '5'} ])`

### ./plugins/operators/load_dimension.py
Custom operator that inserts data from staging tables into dimension tables using provided SQL statements.

Arguments:  

`redshift_conn_id` = Airflow Connection to Redshift  
`table` = target dimension table  
`sql_statement` = SQL statement to perform INSERTS  
`append_mode` = (Boolean) If `True`, insert statement appends data to existing table data, otherwise table is wiped clean prior to performing inserts.

### ./plugins/operators/load_fact.py
Custom operator that inserts data from staging tables into fact tables using provided SQL statements.

Arguments:  

`redshift_conn_id` = Airflow Connection to Redshift  
`table` = target dimension table  
`sql_statement` = SQL statement to perform INSERTS

### ./plugins/operators/stage_redshift.py
Custom operator that copies data from JSON files on S3 to staging tables on redshift.

Arguments:  
`redshift_conn_id` = Airflow Connection to Redshift  
`table` = target staging table  
`sql_statement` = SQL statement to perform INSERTS  
`s3_bucket` = S3 bucket where files are stored  
`s3_key` = S3 key for directory or JSON file  
`json_opts` = Additional options to add to SQL COPY statement ie: `'Auto'` etc..  
