import argparse
import psycopg2
from get_database_index_usage import get_database_index_usage
from get_databases import get_databases
from send_mail import send_mail


def insert_database_index_usage(
    target_server, target_username, target_password, dba_username, dba_password
):
    """
    Inserts database table usage information into the DBAAdmin database.

    Args:
        target_server (str): Name of the target PostgreSQL server.
        target_username (str): Username for the target PostgreSQL server.
        target_password (str): Password for the target PostgreSQL server.
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.

    Raises:
        Exception: An error occurred while connecting to the DBA database.
    """

    # Initialize connection and cursor
    conn_dba = None
    cursor_dba = None

    try:
        # Connect to the DBA001 server
        conn_dba = psycopg2.connect(
            host="DBA001",
            user=dba_username,
            password=dba_password,
            dbname="dbaadmin",
        )
        cursor_dba = conn_dba.cursor()

        # Get databases from the target server
        databases = get_databases(target_server, target_username, target_password)

        # Foreach database, get tables and their usage
        for current_database in databases:
            # Get tables and their usage from the target server for the current database
            index_usage = get_database_index_usage(
                target_server, target_username, target_password, current_database
            )

            # Insert into dba.index_usage table
            for (
                database_name,
                schema_name,
                table_name,
                index_name,
                index_scans,
                index_tuples_read,
                index_tuples_fetched
            ) in index_usage:
                cursor_dba.execute(
                    "INSERT INTO dba.index_usage (server_name, database_name, schema_name, table_name, index_name, index_scans, index_tuples_read, index_tuples_fetched, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (
                        target_server,
                        database_name,
                        schema_name,
                        table_name,
                        index_name,
                        index_scans,
                        index_tuples_read,
                        index_tuples_fetched
                    ),
                )

        # Commit after processing all databases for this server
        conn_dba.commit()

    except Exception as e:
        function_name = insert_database_index_usage.__name__
        error_message = f"An error occurred in {function_name}. The error is  {e}"
        error_subject = f"Failure: {function_name}"
        error_recipients = "name@example.com"
        print(error_message)
        try:
            send_mail(error_subject, error_message, error_recipients)
        except Exception as e:
            print(f"Failed to send email notification: {e}")

        # Rollback the transaction if there was an error
        if conn_dba is not None:
            conn_dba.rollback()

    finally:
        # Commit changes and close connection
        if cursor_dba is not None:
            cursor_dba.close()
        if conn_dba is not None:
            conn_dba.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert database index usage information into the DBAAdmin database."
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

    insert_database_index_usage(
        args.target_server,
        args.target_username,
        args.target_password,
        args.dba_username,
        args.dba_password,
    )
