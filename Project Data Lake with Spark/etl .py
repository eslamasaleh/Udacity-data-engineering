import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()

config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']

os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').distinct() 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs',mode='overwrite',partitionBy=['artist_id','year'])
    # extract columns to create artists table
    artists_table = df.select('artist_id','name','location','lattitude','longitude').distinct() 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df =df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(log_data+'users',mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(updateTime, TimestampType())
    df = df.withColumn('timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(date_convert, TimestampType())
    df = df.withColumn('start_time',get_datetime('timestamp'))
    
    # extract columns to create time table
    df=df.withColumn('hour',hour('start_time'))\
        .withColumn('day',dayofmonth('start_time'))\
        .withColumn('week',weekofyear('start_time'))\
        .withColumn('month',month('start_time'))\
        .withColumn('year',year('start_time'))\
        .withColumn('weekday',dayofweek('start_time'))
    time_table = df.select('start_time','hour','day','week','month','year','weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time-table',mode='overwrite',partitionBy=['year','month'])

    # read in song data to use for songplays table
    song_df = spark.read.parquet.option("basePath", os.path.join(output_data, "songs/")).load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    dfjoin = df.join(song_df,song_df.artist_name==df.artist,'inner').distinct()
    songplays_table=dfjoin.select('songplay_id','start_time','user_id','level','song_id','artist_id','session_id','location','user_agent')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays',mode='overwrite',partitionBy=['year','month'])


def main():
    spark = create_spark_session()
    input_songdata = "s3a://udacity-dend/song_data/*/*/*/*.json"
    input_logfdata='s3a://udacity-dend/log-data/*.json'

    output_data = "s3a://udacity-output/"
    
    process_song_data(spark, input_songdata, output_data)    
    process_log_data(spark, input_logdata, output_data)
    spark.stop()

if __name__ == "__main__":
    
    main()
