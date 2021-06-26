import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Remove staging and analytics table from Redshift cluster

    Parameters
    ----------
    cur : psycopg2 cursor
        cursor to execute sql statements
    conn : psycopg2 connection
        connection to AWS Redshift to commit transtactions
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging and analytics table on Redshift cluster

    Parameters
    ----------
    cur : psycopg2 cursor
        cursor to execute sql statements
    conn : psycopg2 connection
        connection to AWS Redshift to commit transtactions
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Load config file and connect to AWS Redshift.
    Subsequently, drop existing tables and create new ones. 
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print('Successfully created tables in Redshift')

if __name__ == "__main__":
    main()