import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (artist text
,auth text
,firstName text
,gender VARCHAR(1)
,itemInSession int
,lastName text
,length float
,level text
,location text
,method text
,page text
,registration text
,sessionId int
,song text
,status int
,ts bigint
,userAgent text
,userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (num_songs int
                        , artist_id text
                        , artist_latitude float
                        , artist_longitude float
                        , artist_location text
                        , artist_name text
                        , song_id text
                        , title text
                        , duration float
                        , year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY
                                    ,start_time timestamp NOT NULL
                                    ,user_id int NOT NULL
                                    ,level text
                                    ,song_id text
                                    ,artist_id text
                                    ,session_id int
                                    ,location text
                                    ,user_agent text);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY
                                ,first_name text
                                ,last_name text
                                ,gender VARCHAR(1)
                                ,level text);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id text PRIMARY KEY
                                ,title text
                                ,artist_id text
                                ,year int
                                ,duration float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id text PRIMARY KEY
                                    ,name text
                                    ,location text
                                    ,lattitude float
                                    ,longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY
                                ,hour int
                                ,day int
                                ,week int
                                ,month int
                                ,year int
                                ,weekday int);
""")

# LOAD DATA INTO STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM {LOG_DATA}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS JSON {LOG_JSONPATH}
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM {SONG_DATA}
credentials 'aws_iam_role={DWH_ROLE_ARN}'
region 'us-west-2'
FORMAT AS JSON 'auto'
""")

# INSERT DATA INTO FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + a.ts * interval '0.001 second' AS ts, a.userid, a.level, b.song_id, b.artist_id, a.sessionid, a.location, a.useragent
FROM staging_events a
INNER JOIN staging_songs b
    ON a.artist = b.artist_name AND
    a.song = b.title
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstname, lastname, gender, level
FROM staging_events
WHERE userid IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
ORDER BY artist_id
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)

SELECT ts AS start_time
,extract(hour from ts) AS hour
,extract(day from ts) AS day
,extract(week from ts) AS week
,extract(month from ts) AS month
,extract(year from ts) AS year
,extract(weekday from ts) AS weekday

FROM (

select timestamp 'epoch' + ts * interval '0.001 second' AS ts
FROM staging_events )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
