import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops tables from databes, if they exist

    Args:
        cur: psycopg2 cursor
        conn: Connection parameters
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates staging and final (star schema) tables

    Args:
        cur: psycopg2 cursor
        conn: Connection parameters
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Manages tables through followin steps
    - Loads config data from config file saved under the name dwh.cfg
    - Creates psycopg connection and cursor
    - Drops staging and final tables
    - Creates stagin and finale tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
