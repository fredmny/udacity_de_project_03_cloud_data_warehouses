import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads the data from JSON files saved in S3 bucket into staging tables staging_events and staging_songs

    Args:
        cur: psycopg2 cursor
        conn: Connection parameters
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables into final tables in star schema

    Args:
        cur: psycopg2 cursor
        conn: Connection parameters
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Runs ETL pipeline, executing following tasks
    - Loads config data from config file saved under the name dwh.cfg
    - Creates psycopg connection and cursor
    - Loads data into staging tables from JSON files saved in S3 buckets
    - Inserts data from staging tables into final tables 
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()