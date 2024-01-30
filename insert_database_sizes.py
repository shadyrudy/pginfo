import argparse
import psycopg2
from get_database_sizes import get_database_sizes


def insert_database_sizes(
    target_server, target_username, target_password, dba_username, dba_password
):
    """
    Inserts the sizes of databases from the target PostgreSQL server into the DBAAdmin database.

    Args:
        target_server (str): Name of the target PostgreSQL server.
        target_username (str): Username for the target PostgreSQL server.
        target_password (str): Password for the target PostgreSQL server.
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.
    """
    # Get databases and their sizes from the target server
    databases = get_database_sizes(target_server, target_username, target_password)

    # Connect to the DBA001 server
    conn_dba = psycopg2.connect(
        host="DBA001", user=dba_username, password=dba_password, dbname="dbaadmin"
    )
    cursor_dba = conn_dba.cursor()

    # Insert into dba.database_sizes table
    for db_name, size_mb, size_gb in databases:
        cursor_dba.execute(
            "INSERT INTO dba.databases (server_name, database_name, database_size_bytes, database_size, last_updated) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
            (target_server, db_name, size_mb, size_gb),
        )

    # Commit changes and close connection
    conn_dba.commit()
    cursor_dba.close()
    conn_dba.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert database sizes information into the DBAAdmin database."
    )
    parser.add_argument("target_server", help="Name of the target PostgreSQL server")
    parser.add_argument(
        "target_username", help="Username for the target PostgreSQL server"
    )
    parser.add_argument(
        "target_password", help="Password for the target PostgreSQL server"
    )
    parser.add_argument("dba_username", help="Username for the DBA PostgreSQL server")
    parser.add_argument("dba_password", help="Password for the DBA PostgreSQL server")

    args = parser.parse_args()

    insert_database_sizes(
        args.target_server,
        args.target_username,
        args.target_password,
        args.dba_username,
        args.dba_password,
    )
