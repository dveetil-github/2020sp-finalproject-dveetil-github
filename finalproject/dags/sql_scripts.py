import configparser

# CONFIG
# config = configparser.ConfigParser()
# config.read('connection.cfg')
#
# DWH_IAM_ROLE_NAME = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
# DB_USER = config.get("CLUSTER", "DB_USER")
# DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
# HOST = config.get("CLUSTER", "HOST")
# DB_PORT = config.get("CLUSTER", "DB_PORT")
# DB_NAME = config.get("CLUSTER", "DB_NAME")
#
# LOG_DATA = config.get("S3", "LOG_DATA")
# SONG_DATA = config.get("S3", "SONG_DATA")
import os

DWH_IAM_ROLE_NAME = os.environ["DWH_IAM_ROLE_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
HOST = os.environ["HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
#
LOG_DATA = "s3://mydvbucket/data_final_project/log_data"
SONG_DATA = "s3://mydvbucket/final_song_subset"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS;"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_EVENTS (
artist varchar, 
auth varchar,
firstName varchar,
gender varchar,
itemInSession integer,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
session_id integer,
song varchar,
status integer,
ts numeric,
userAgent varchar,
userId varchar
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_SONGS (
song_id varchar, 
title varchar,
year integer, 
duration NUMERIC,
artist_id varchar, 
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
num_songs integer
);
""")

songplay_table_create = (""" create table if not exists songplays(
songplay_id bigint IDENTITY(0,1) , 
start_time varchar, 
user_id varchar, 
level varchar,
song_id varchar, 
artist_id varchar, 
session_id varchar, 
location varchar, 
user_agent varchar
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS SONGS (
song_id varchar primary key, 
title varchar not null,
artist_id varchar not null, 
year int, 
duration int
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar primary key,
name varchar,
location varchar,
latitude varchar, 
longitude varchar
);
""")

time_table_create = ("""create table if not exists time (
start_time varchar,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-east-2';
""").format(LOG_DATA, DWH_IAM_ROLE_NAME)

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-east-2';
""").format(SONG_DATA, DWH_IAM_ROLE_NAME)

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) (select ts,userid, level,song_id,artist_id,session_id,location,useragent 
from staging_songs sts , STAGING_EVENTS ste where sts.title=ste.song and sts.artist_name=ste.artist and sts.duration=ste.length and ste.page='NextSong');
""")

user_table_insert = ("""insert into users (
select cast(userId as int), firstName, lastName, gender, level
from staging_events where page='NextSong') ;
""")

song_table_insert = ("""insert into songs (select song_id, title, artist_id, year, duration from staging_songs)
""")

artist_table_insert = ("""insert into artists (select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs)
""")

# time_table_insert = ("""insert into time (
# start_time,hour,day,week,month,year,weekday)
# values(%s,%s,%s,%s,%s,%s,%s);
# """)

time_table_insert = ("""insert into time (
select ts_time, extract(hr from ts_time) as hour, extract(d from ts_time) as day,extract(week from ts_time) as week,extract(mon from ts_time) as month, 
extract(yr from ts_time) as year,extract(dayofweek from ts_time) as weekday 
from (select timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' AS ts_time
from staging_events where page='NextSong' ) a);
""")

# FIND SONGS
select_staging_events = ("""
select * from staging_events limit 10;
""")

song_select = (
    """select song_id, artist_id from (select song_id,title,year,duration,name,a.artist_id,location from songs s, artists a where s.artist_id = a.artist_id ) sa where sa.title=%s and sa.name=%s and sa.duration=%s""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
data_stagg_qrys = [staging_events_copy, staging_songs_copy]
data_transform_qrys = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
