import configparser

import boto3
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
#ARN =config.get('IAM_ROLE','ARN')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

## Get iam role arn
iam = boto3.client('iam',aws_access_key_id=KEY,aws_secret_access_key=SECRET,region_name='us-west-2')

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS song;"
artist_table_drop = "DROP table IF EXISTS artist;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
                                (
                                artist           varchar,
                                auth             varchar,
                                firstName        varchar,
                                gender           varchar,
                                itemInSession    int,
                                lastName         varchar,
                                length           float,
                                level            varchar,
                                location         varchar,
                                method           varchar,
                                page             varchar,
                                registration     varchar,
                                sessionId        int,
                                song             varchar,
                                status           int,
                                ts               bigint,
                                userAgent        varchar,
                                userId           int
                                
                                );
                        
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs
                                (
                                num_songs        int, 
                                artist_id        varchar,
                                artist_latitude  float,
                                artist_longitude float,
                                artist_location  varchar,
                                artist_name      varchar,
                                song_id          varchar,
                                title            varchar,
                                duration         float,
                                year             int
                                );
""")

songplay_table_create = ("""CREATE TABLE songplay
                        (
                        songplay_id int IDENTITY(0,1) NOT NULL sortkey, 
                        start_time timestamp          NOT NULL , 
                        user_id    int                NOT NULL, 
                        level      varchar            NOT NULL, 
                        song_id    varchar            NOT NULL, 
                        artist_id  varchar            NOT NULL, 
                        session_id varchar            NOT NULL, 
                        location   varchar, 
                        user_agent varchar            NOT NULL
                        );
""")

user_table_create = ("""CREATE TABLE users
                        ( 
                        user_id    int  NOT NULL sortkey,
                        first_name varchar  NOT NULL, 
                        last_name  varchar  NOT NULL, 
                        gender     varchar  NOT NULL,
                        level      varchar  NOT NULL 
                        
                        );
""")

song_table_create = ("""CREATE TABLE song 
                        (
                        song_id varchar            NOT NULL sortkey,
                        title varchar          NOT NULL, 
                        artist_id varchar      NOT NULL, 
                        year int               NOT NULL, 
                        duration float           NOT NULL   
                        );
""")

artist_table_create = ("""CREATE TABLE artist
                        ( 
                        artist_id varchar NOT NULL sortkey, 
                        name varchar NOT NULL, 
                        location varchar NULL, 
                        latitude float NULL, 
                        longitude float NULL
                        
                        );
                
""")

time_table_create = (""" CREATE TABLE time 
                        (
                        start_time timestamp NOT NULL , 
                        hour       int       NOT NULL, 
                        day        int       NOT NULL, 
                        week       int       NOT NULL, 
                        month      int       NOT NULL, 
                        year       int       NOT NULL, 
                        weekday    int       NOT NULL
                        );
""")

# STAGING TABLES

staging_events_copy = (""" 
                        COPY staging_events
                        FROM {}
                        iam_role '{}'
                        FORMAT AS json {}
                        
""").format(LOG_DATA,roleArn,LOG_JSONPATH)

staging_songs_copy = ("""
                        COPY staging_songs
                        FROM {}
                        iam_role '{}'
                        FORMAT AS json 'auto'
                        
""").format(SONG_DATA,roleArn)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                                                start_time, 
                                                user_id, 
                                                level, 
                                                song_id, 
                                                artist_id, 
                                                session_id, 
                                                location, 
                                                user_agent
                                                 )
                            SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second',
                                            se.userId,
                                            se.level,
                                            se.song,
                                            se.artist,
                                            se.sessionId,
                                            se.location,
                                            se.userAgent
                            FROM staging_events se 
                            INNER JOIN staging_songs ss
                                ON se.song=ss.title AND se.artist=ss.artist_name
                            WHERE se.page='NextSong';
                            
""")

user_table_insert = ("""INSERT INTO users(user_id, 
                                         first_name, 
                                         last_name, 
                                         gender, 
                                         level
                                        )
                        SELECT DISTINCT(se.userId),
                                        se.firstName,
                                        se.lastName,
                                        se.gender,
                                        se.level
                        FROM staging_events se
                        WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO song(
                                        song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration
                                        )
                        SELECT DISTINCT(ss.song_id),
                                        ss.title,
                                        ss.artist_id,
                                        ss.year,
                                        ss.duration 
                        FROM staging_songs ss
                        WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO artist (
                                               artist_id, 
                                               name, 
                                               location, 
                                               latitude, 
                                               longitude
                                               )
                           SELECT DISTINCT(ss.artist_id),
                                           ss.artist_name,
                                           ss.artist_location,
                                           ss.artist_latitude,
                                           ss.artist_longitude
                           FROM staging_songs ss
                           WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time 
                                (start_time, 
                                hour, 
                                day, 
                                week, 
                                month, 
                                year, 
                                weekday)
                        SELECT  DISTINCT(start_time)                AS start_time,
                                EXTRACT(hour FROM start_time)       AS hour,
                                EXTRACT(day FROM start_time)        AS day,
                                EXTRACT(week FROM start_time)       AS week,
                                EXTRACT(month FROM start_time)      AS month,
                                EXTRACT(year FROM start_time)       AS year,
                                EXTRACT(dayofweek FROM start_time)  as weekday
                        FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
