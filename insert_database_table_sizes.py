import argparse
import psycopg2
from get_database_table_sizes import get_database_table_sizes
from get_databases import get_databases
from send_mail import send_mail


def insert_database_table_sizes(
    target_server, target_username, target_password, dba_username, dba_password
):
    """
    Inserts the sizes of tables in the target PostgreSQL server into the DBAAdmin database.

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
            table_sizes = get_database_table_sizes(
                target_server, target_username, target_password, current_database
            )

            # Insert into dba.tables table
            for (
                database_name,
                schema_name,
                table_name,
                table_size,
                index_size,
                total_size,
                row_estimate,
            ) in table_sizes:
                cursor_dba.execute(
                    "INSERT INTO dba.tables (server_name, database_name, schema_name, table_name, table_size_bytes, index_size_bytes, total_size_bytes, row_count, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (
                        target_server,
                        database_name,
                        schema_name,
                        table_name,
                        table_size,
                        index_size,
                        total_size,
                        row_estimate,
                    ),
                )

        # Commit after processing all databases for this server
        conn_dba.commit()
        print(f"Successfully inserted data for all databases on server {target_server}")

    except Exception as e:
        function_name = insert_database_table_sizes.__name__
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
        # Close connection
        if cursor_dba is not None:
            cursor_dba.close()
        if conn_dba is not None:
            conn_dba.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert database table sizes information into the DBAAdmin database."
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

    insert_database_table_sizes(
        args.target_server,
        args.target_username,
        args.target_password,
        args.dba_username,
        args.dba_password,
    )
