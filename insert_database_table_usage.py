import argparse
import psycopg2
from get_database_table_usage import get_database_table_usage 

def insert_database_table_usage(target_server, target_username, target_password, dba_username, dba_password):
    # Get tables and their usage from the target server
    table_usage = get_database_table_usage(target_server, target_username, target_password)

    # Connect to the DBA001 server
    conn_dba = psycopg2.connect(
        host='DBA001',
        user=dba_username,
        password=dba_password,
        dbname='dbaadmin'
    )
    cursor_dba = conn_dba.cursor()

    # Insert into dba.table_usage table
    for database_name, schema_name, table_name, sequential_scans, sequential_tuples_read, index_scans, index_tuples_fetched in table_usage:
        cursor_dba.execute(
            "INSERT INTO dba.table_usage (server_name, database_name, schema_name, table_name, sequential_scans, sequential_tuple_scans, index_scans, index_tuple_fetches, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
            (target_server, database_name, schema_name, table_name, sequential_scans, sequential_tuples_read, index_scans, index_tuples_fetched)
        )

    # Commit changes and close connection
    conn_dba.commit()
    cursor_dba.close()
    conn_dba.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert database sizes information into the DBAAdmin database.")
    parser.add_argument("target_server", help="Name of the target PostgreSQL server")
    parser.add_argument("target_username", help="Username for the target PostgreSQL server")
    parser.add_argument("target_password", help="Password for the target PostgreSQL server")
    parser.add_argument("dba_username", help="Username for the DBA PostgreSQL server")
    parser.add_argument("dba_password", help="Password for the DBA PostgreSQL server")

    args = parser.parse_args()

    insert_database_table_usage(args.target_server, args.target_username, args.target_password, args.dba_username, args.dba_password)
