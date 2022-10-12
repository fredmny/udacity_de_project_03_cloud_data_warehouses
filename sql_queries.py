import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events
  (
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR(24) NOT NULL,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL,
    song_id VARCHAR(24) NOT NULL,
    title VARCHAR NOT NULL,
    duration NUMERIC NOT NULL,
    year INTEGER NOT NULL
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs
  (
    artist VARCHAR NOT NULL,
    auth VARCHAR NOT NULL,
    firstName VARCHAR NOT NULL,
    gender VARCHAR(1) NOT NULL,
    itemInSession INTEGER NOT NULL,
    lastName VARCHAR NOT NULL,
    length NUMERIC,
    level VARCHAR(4) NOT NULL,
    location VARCHAR NOT NULL,
    method VARCHAR(4) NOT NULL,
    page VARCHAR(15) NOT NULL,
    registration BIGINT NOT NULL,
    sesionId INTEGER NOT NULL,
    song VARCHAR,
    status INTEGER NOT NULL,
    ts TIMESTAMP,
    userAgent VARCHAR NOT NULL,
    userId INTEGER NOT NULL,
  );
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays
  (
    songplay_id IDENTITY (1,1) INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(4) NOT NULL,
    song_id VARCHAR(24) NOT NULL,
    artist_id VARCHA(24) NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
  );
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users
  (
    user_id INTEGER NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR(1) NOT NULL,
    level VARCHAR(4) NOT NULL
  );
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs
  (
    song_id VARCHAR(24) NOT NULL,
    title VARCHAR NOT NULL,
    artist_id VARCHAR(24) NOT NULL,
    year INTEGER NOT NULL,
    duration NUMERIC NOT NULL
  );
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists
  (
    artist_id VARCHAR(24) NOT NULL,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
  );
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time
  (
    start_time TIMESTAMP NOT NULL,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
  );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
