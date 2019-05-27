import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Executes sql for dropping staging and star schema tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Executes sql for creating staging and star schema tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load configuration info
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to redshift cluster
    conn = psycopg2.connect(f"host={config['DWH']['DWH_ENDPOINT']} dbname={config['DWH']['DWH_DB']} user={config['DWH']['DWH_DB_USER']} password={config['DWH']['DWH_DB_PASSWORD']} port={config['DWH']['DWH_PORT']}")
    cur = conn.cursor()

    # drop and create tables in redshift
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()