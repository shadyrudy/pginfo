import argparse
import psycopg2
from get_database_grants import get_database_grants
from get_databases import get_databases


def insert_database_grants(
    target_server, target_username, target_password, dba_username, dba_password
):
    """
    Inserts database grant information into the DBAAdmin database.

    Args:
        target_server (str): Name of the target PostgreSQL server.
        target_username (str): Username for the target PostgreSQL server.
        target_password (str): Password for the target PostgreSQL server.
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.

    Raises:
        Exception: An error occurred while connecting to the DBA database.
    """

    # Get databases from the target server
    databases = get_databases(target_server, target_username, target_password)

    # Foreach database, get tables and their grant information
    for current_database in databases:
        # Get database grants for the current database
        database_grants = get_database_grants(
            target_server, target_username, target_password, current_database
        )

        # Connect to the DBA001 server
        conn_dba = psycopg2.connect(
            host="DBA001", user=dba_username, password=dba_password, dbname="dbaadmin"
        )
        cursor_dba = conn_dba.cursor()

        # Insert into dba.grants table
        for (
            database_name,
            schema_name,
            object_name,
            object_type,
            grantor,
            grantee,
            privilege_type,
            is_grantable,
            with_hierarchy,
        ) in database_grants:
            cursor_dba.execute(
                "INSERT INTO dba.grants (server_name, database_name, schema_name, object_name, object_type, grantor, grantee, privilege_type, is_grantable, with_hierarchy, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                (
                    target_server,
                    database_name,
                    schema_name,
                    object_name,
                    object_type,
                    grantor,
                    grantee,
                    privilege_type,
                    is_grantable,
                    with_hierarchy,
                ),
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

    insert_database_grants(
        args.target_server,
        args.target_username,
        args.target_password,
        args.dba_username,
        args.dba_password,
    )
