import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Extract staging tables from S3 to Redshift Landing."""
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Populate tables with table for final tables for analytical purposes."""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    """Connect with Redshift Data Warehouse and call the functions."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
