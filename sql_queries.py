import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
  event_id             BIGINT IDENTITY(0,1),
  artist               TEXT,
  auth                 TEXT,
  first_name           TEXT,
  gender               CHAR(1),
  item_id_session      INTEGER,
  last_name            TEXT,
  length               NUMERIC,
  level                TEXT,
  location             VARCHAR(500),
  method               TEXT,
  page                 TEXT,
  registration         NUMERIC,
  session_id           INTEGER,
  song                 VARCHAR(500),
  status               INTEGER,
  ts                   NUMERIC,
  user_agent           TEXT,
  user_id              INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
  num_songs         INTEGER,
  artist_id         TEXT,
  artist_latitude   NUMERIC,
  artist_longitude  NUMERIC,
  artist_location   VARCHAR(500),
  artist_name       VARCHAR(500),
  song_id           TEXT,
  title             VARCHAR(500),
  duration          NUMERIC,
  year              INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
start_time  TIMESTAMP NOT NULL SORTKEY, 
user_id     TEXT NOT NULL DISTKEY, 
level       TEXT, 
song_id     TEXT, 
artist_id   TEXT, 
session_id  INTEGER, 
location    VARCHAR(500), 
user_agent  TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id     INTEGER PRIMARY KEY, 
first_name  TEXT, 
last_name   TEXT, 
gender      CHAR(1), 
level       TEXT
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id     TEXT PRIMARY KEY, 
title       VARCHAR(500), 
artist_id   TEXT, 
year        INTEGER, 
duration    NUMERIC
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
artist_id   TEXT PRIMARY KEY, 
name        VARCHAR(500), 
location    VARCHAR(500), 
lattitude   NUMERIC, 
longitude   NUMERIC
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
start_time  TIMESTAMP PRIMARY KEY SORTKEY, 
hour        INT NOT NULL, 
day         INT NOT NULL, 
week        INT NOT NULL, 
month       INT NOT NULL, 
year        INT NOT NULL, 
weekday     INT NOT NULL
);
""")

# STAGING TABLES
staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}'
format as json 'auto'
ACCEPTINVCHARS AS '^'
region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  as start_time, 
       se.user_id, 
       se.level, 
       ss.song_id,
       ss.artist_id,
       se.session_id,
       se.location,
       se.user_agent
from staging_events se
join staging_songs ss on se.song = ss.title and se.artist = ss.artist_name
where page = 'NextSong';
""")

user_table_insert = ("""
insert into users
(user_id, first_name, last_name, gender, level)
select distinct se.user_id, 
       se.first_name, 
       se.last_name,
       se.gender,
       se.level
from staging_events se
where page = 'NextSong';
""")

song_table_insert = ("""
insert into songs
(song_id, title, artist_id, year, duration)
select distinct ss.song_id, 
       ss.title, 
       ss.artist_id,
       ss.year,
       ss.duration
from staging_songs ss;
""")

artist_table_insert = ("""
insert into artists
(artist_id, name, location, lattitude, longitude)
select distinct ss.artist_id, 
       ss.artist_name, 
       ss.artist_location,
       ss.artist_latitude,
       ss.artist_longitude
from staging_songs ss;
""")

time_table_insert = ("""
insert into time
(start_time, hour, day, week, month, year, weekday)
select sp.start_time, 
       EXTRACT(HOUR FROM sp.start_time), 
       EXTRACT(DAY  FROM sp.start_time), 
       EXTRACT(WEEK FROM sp.start_time), 
       EXTRACT(MONTH FROM sp.start_time), 
       EXTRACT(YEAR FROM sp.start_time),
       EXTRACT(DOW FROM sp.start_time)
from (select distinct start_time from songplays) sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy] 
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
