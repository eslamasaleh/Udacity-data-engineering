# Project: Data Modeling with Postgres
## Introduction
* A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

* They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
* In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

### Fact Table
* songplays 
- records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
#### Users 
- users in the app user_id, first_name, last_name, gender, level
#### Songs 
- songs in music database song_id, title, artist_id, year, duration
#### Artists 
- artists in music database artist_id, name, location, latitude, longitude
#### Time
- timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday.
## SQL Queries
### songplays 
| Column | Type|
|------------ | -------------|
|songplay_id | SERIAL|
|start_time  | bigint|
|user_id| int|
|level | vachar|
|song_id|varchar|
|artist_id| varchar| 
|session_id|int|
|location | text| 
|user_agent| text|

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
|start_time| bigint|
|hour| int|
|day| int|
|week| int| 
|month |int|
|year| int|
|weekday| varchar|

