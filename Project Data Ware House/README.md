## Project: Data Warehouse

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Project Datasets


You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

### Song Dataset


The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.


### Schema for Song Play Analysis
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table

##### songplays
- records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


#### Dimension Tables
##### users 
- users in the app 
- user_id, first_name, last_name, gender, level
##### songs 
- songs in music database 
- song_id, title, artist_id, year, duration
##### artists
- artists in music database 
- artist_id, name, location, lattitude, longitude
##### time 
- timestamps of records in songplays broken down into specific units 
- start_time, hour, day, week, month, year, weekday
## Steps
1. Run cc.py to create a Redshift cluster.
2. Run create_tables.py to create tables.
3. Run etl.py to create pipelines.
4. After finishing run dlc.py to delete cluster.
## Create Table Schemas
* Design schemas for your fact and dimension tables
* Write a SQL CREATE statement for each of these tables in sql_queries.py 
* Complete the logic in create_tables.py to connect to the database and create these tables
* Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
* Launch a redshift cluster and create an IAM role that has read access to S3.
* Add redshift database and IAM role info to dwh.cfg.
* Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.
## Build ETL Pipeline
* Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
* Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
* Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
* Delete your redshift cluster when finished.
## SQL Queries

### Staging Events
| Column | Type|
|------------ | -------------|
|artist|varchar|
|auth|varchar|
|firstName|varchar|
|gender|varchar|
|itemInSessio|int|
|lastName|varchar|
|length|float|
|level|varchar|
|location|varchar|
|method|varchar|
|page|varchar|
|registration|varchar|
|sessionId|int|
|song|varchar|
|status|int|
|ts|bigint|
|userAgent|varchar|
|userId|int|

### staging_songs
| Column | Type|
|------------ | -------------|
|num_songs|int|
|artist_id|varchar|
|artist_latitude|float|
|artist_longitude|float|
|artist_location|varchar|
|artist_name|varchar|
|song_id|varchar|
|title|varchar|
|duration|float|
|year|int|

### songplays 

| Column | Type|
|------------ | -------------|
|songplay_id | int IDENTITY(0,1)|
|start_time  | timestamp |
|user_id| int|
|level | vachar|
|song_id|varchar|
|artist_id| varchar| 
|session_id|varchar|
|location |varchar| 
|user_agent|varchar|

### Users 
| Column | Type|
|------------ | -------------|
|user_id|int | 
|first_name| varchar|
|last_name| varchar|
|gender| varchar|
|level| varchar|

### Songs 
| Column | Type|
|------------ | -------------|
|song_id |varchar|
|title| varchar|
|artist_id |varchar | 
|year| int|
|duration| float|

### Artists
| Column | Type|
|------------ | -------------|
|artist_id |varchar |
|name |varchar| 
|location| varchar|
|latitude| float|
|longitude| float|

### Time
| Column | Type|
|------------ | -------------|
|start_time| timestamp|
|hour| int|
|day| int|
|week| int| 
|month |int|
|year| int|
|weekday| int|

