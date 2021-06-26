import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load raw data into staging tables. Uses copy statement and
    IAM role with AmazonS3ReadOnlyAccess rights. 

    Parameters
    ----------
    cur : psycopg2 cursor
        cursor to execute sql statements
    conn : psycopg2 connection
        connection to AWS Redshift to commit transtactions
    """

    print('start inserting into staging tables')

    for query in copy_table_queries:
        print('-' * 20)
        print(f'executing query for table: {query.split(" ")[1]}')
        print('-' * 20)
        cur.execute(query)
        conn.commit()

    print('done inserting into staging tables')

def insert_tables(cur, conn):
    """Execute queries to insert raw data from the staging tables
    into organized DB schema.

    Parameters
    ----------
    cur : psycopg2 cursor
        cursor to execute sql statements
    conn : psycopg2 connection
        connection to AWS Redshift to commit transtactions
    """

    print('start inserting into DB tables')

    for query in insert_table_queries:
        print('-' * 20)
        print(f'executing query for table: {query.split(" ")[2]}')
        print('-' * 20)
        cur.execute(query)
        conn.commit()

    print('done inserting into DB tables')

def main():
    """Read config settings and connect to aws redshift cluster.
    Subsequently execute functions to load raw data and later insert data
    into DB schema.
    """
    print('loading config file')
    print('-' * 20)
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('connect to AWS Redshift')
    print('-' * 20)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()