import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

iam_role = config['IAM_ROLE']['ARN']
json = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(1000),
    auth VARCHAR(100),
    firstName VARCHAR(100),
    gender VARCHAR(100),
    itemInSession VARCHAR(100),
    lastName VARCHAR(100),
    length VARCHAR(100),
    level VARCHAR(100),
    location VARCHAR(100),
    method VARCHAR(100),
    page VARCHAR(100),
    registration VARCHAR(100),
    sessionId INTEGER,
    song VARCHAR(1000),
    status VARCHAR(100),
    ts VARCHAR(100),
    userAgent VARCHAR(250),
    userId INTEGER,
    PRIMARY KEY(sessionId, ts))
    DISTSTYLE ALL;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(100),
    num_songs VARCHAR(100),
    title VARCHAR(1000),
    artist_name VARCHAR(100),
    artist_latitude VARCHAR(100),
    year VARCHAR(100),
    duration VARCHAR(100),
    artist_id VARCHAR(100),
    artist_longitude VARCHAR(100),
    artist_location VARCHAR(100),
    PRIMARY KEY(song_id, artist_id))
    DISTSTYLE ALL;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER,
    start_time DATE,
    user_id INTEGER,
    level VARCHAR(100),
    song_id INTEGER,
    artist_id INTEGER,
    session_id INTEGER,
    location VARCHAR(100),
    user_agent VARCHAR(100),
    PRIMARY KEY(songplay_id))
    DISTSTYLE ALL;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(100),
    level VARCHAR(100),
    PRIMARY KEY(user_id))
    DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id INTEGER,
    title VARCHAR(1000),
    artist_id INTEGER,
    year VARCHAR(100),
    duration VARCHAR(100),
    PRIMARY KEY(song_id))
    DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id INTEGER,
    name VARCHAR(100),
    location INTEGER,
    latitude VARCHAR(100),
    longitude VARCHAR(100),
    PRIMARY KEY(artist_id))
    DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time DATE,
    hour VARCHAR(100),
    day INTEGER,
    week VARCHAR(100),
    month VARCHAR(100),
    year VARCHAR(100),
    weekday VARCHAR(100),
    PRIMARY KEY(start_time))
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    IAM_ROLE {}
    format as json {}
    region 'us-west-2';
""").format(iam_role, json)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    IAM_ROLE {}
    json 'auto'
    region 'us-west-2';
""").format(iam_role)

# FINAL TABLES

songplay_table_insert = ("""
   SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
""")

user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
""")

song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
""")

artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
""")

time_table_insert = ("""
        SELECT  start_time
        ,       extract(hour from start_time) as hour
        ,       extract(day from start_time) as day
        ,       extract(week from start_time) as week
        ,       extract(month from start_time) as month
        ,       extract(year from start_time) as year
        ,       extract(dayofweek from start_time) as dayofweek
        FROM    songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
