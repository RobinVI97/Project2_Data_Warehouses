import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""Extract staging tables from S3 to Redshift Landing."""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""Populate tables with table for final tables for analytical purposes."""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""Connect with Redshift Data Warehouse and call the functions."""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
