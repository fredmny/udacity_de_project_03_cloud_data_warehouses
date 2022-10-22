# Project: Data Warehouse
Third project of the Data Engineering Nanodegree from Udacity. 

Like in the other project, this also considers an imaginary startup called Sparkify which wants to analyze the data of its music streaming app to understand what songs their users are listening to. Unfortunately the startup has this data only within `json` files and wants a better way to query it. 

To do so, in this project, a Postgres database is created. This database is called `sparkifydb` and consists of following tables: 
- Staging tables:
  - `staging_events` - Events (songplay logs)
  - `staging_songs` - Song data
- Final tables:
  - `songplays` (fact table) - log of song plays
  - `users` (dimension table) - users in the app
  - `songs` (dimension table) - songs the app as in its database
  - `artists` (dimension table) - data about the song artists in the database
  - `time` (dimension table) - timestamps of the start time for every record in the songplays table broken down in specific units

The final tables are in the star schema, making it easy to aggregate data on the songplays fact table and at the same time easy to join with dimension tables for filtering and specify aggregation parameters.

The data is originated from data within two s3 buckets:
- **song play logs** - `s3://udacity-dend/log_data` with the JSON format specified within `s3://udacity-dend/log_json_path.json`
- **song data** - `s3://udacity-dend/song_data`

## How to run the code
1. Run `python crate_cluster.py` to create the necessary AWS resources, including the cluster
2. Run `python create_tables.py` to drop the tables, in case they exist and create them
3. Run `python etl.py` to load the data into the tables
4. To delete the cluster (including all the tables), run `python delete_cluster.py`
## Files
The project consists of following files:
- `sql_queries.py` - Contains all the SQL queries necessary for the project. This file is not to be run by itself, but is imported in other files.
- `create_tables.py` - Contains the python code to drop + create the tables. This file should be run before running the `etl.py` file.
- `etl.py` - Inserts the data contained in the `json` files into the staging tables using the `COPY` statement and into the final tables in the star schema through a `SELECT` statement.
- `create_cluster.py` - Create a Redshift cluster and other AWS resources necessaries for that (IaC).
- `delete_cluster.py` - Delete Redshift cluster and other AWS resources (IaC).
- `example_dwh.cfg` - Must be saved as `dwh.cfg` and then be filled with credentials and options to run the `create_cluster.py` and `delete_cluster.py` scripts.

## Dependencies
All required dependencies are saved in the `pyproject.toml` / `poetry.lock` files. To install them create a virtual environment with Poetry ([link](https://python-poetry.org/))