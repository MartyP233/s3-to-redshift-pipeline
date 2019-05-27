import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 JSON files into redshift staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transforms data from staging to star schema tables in Redshift.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config['DWH']['DWH_ENDPOINT']} dbname={config['DWH']['DWH_DB']} user={config['DWH']['DWH_DB_USER']} password={config['DWH']['DWH_DB_PASSWORD']} port={config['DWH']['DWH_PORT']}")
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()