import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
dwh_role_arn = config.get('IAM_ROLE', 'ARN')

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
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender VARCHAR(1),
    itemInSession INTEGER NOT NULL,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR(4) NOT NULL,
    location VARCHAR,
    method VARCHAR(4),
    page VARCHAR NOT NULL,
    registration BIGINT,
    sessionId INTEGER NOT NULL,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs
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

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays
  (
    songplay_id INTEGER IDENTITY (1,1) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level VARCHAR(4),
    song_id VARCHAR(24),
    artist_id VARCHAR(24),
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
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
  COPY staging_events FROM 's3://udacity-dend/log_data'
  CREDENTIALS 'aws_iam_role={}'
  JSON 's3://udacity-dend/log_json_path.json'
  TIMEFORMAT 'epochmillisecs'
  COMPUPDATE ON REGION 'us-west-2'
""").format(dwh_role_arn)

staging_songs_copy = ("""
  COPY staging_songs FROM 's3://udacity-dend/song_data'
  CREDENTIALS 'aws_iam_role={}'
  JSON 'auto'
  COMPUPDATE ON REGION 'us-west-2'
""").format(dwh_role_arn)

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
  )
  SELECT
    TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second',
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
  FROM staging_events AS e
  LEFT JOIN staging_songs AS s ON s.title = e.song
  WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
  INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
  )
  SELECT
    DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
  FROM staging_events
  WHERE userId IS NOT NULL
""")

song_table_insert = ("""
  INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
  )
  SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
  FROM staging_songs
  WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
  INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
  )
  SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
  FROM staging_songs
  WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
  INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
  )
  SELECT
    TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(dow FROM start_time)
  FROM staging_events
  WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
