import argparse
import psycopg2
from send_mail import send_mail


def get_database_index_usage(server_name, user, password, db_name="postgres"):
    """
    Retrieves the usage statistics for indexes in a PostgreSQL database.

    Args:
        server_name (str): The name or IP address of the PostgreSQL server.
        user (str): The username for connecting to the PostgreSQL server.
        password (str): The password for connecting to the PostgreSQL server.
        db_name (str, optional): The name of the database. Defaults to 'postgres'.

    Returns:
        list: A list of tuples containing the usage statistics for each index.
              Each tuple contains the following information:
              - database_name: The name of the database.
              - schema_name: The name of the schema.
              - table_name: The name of the table.
              - index_name: The name of the index.
              - index_scans: The number of index scans
              - index_tuples_read: The number of tuples read from the index
              - index_tuples_fetched: The number of tuples fetched during index scans.

    Raises:
        Exception: If an error occurs while connecting to the PostgreSQL server or executing the query.
    """

    # Initialize connection and cursor
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(
            host=server_name, user=user, password=password, dbname=db_name
        )
        cursor = conn.cursor()

        query = """
        SELECT current_database() as database_name,
               i.schemaname AS schema_name,
               i.relname AS table_name,
               i.indexrelname AS index_name,    
               i.idx_scan AS index_scans,
               i.idx_tup_read as index_tuples_read,
               i.idx_tup_fetch as index_tuples_fetched
        FROM   pg_stat_user_indexes as i
        ORDER BY i.schemaname, i.relname, i.indexrelname;
        """

        cursor.execute(query)
        table_usage = cursor.fetchall()

        return table_usage

    except Exception as e:
        function_name = get_database_index_usage.__name__
        error_message = f"An error occurred in {function_name}. The error is  {e}"
        error_subject = f"Failure: {function_name}"
        error_recipients = "name@example.com"
        print(error_message)
        try:
            send_mail(error_subject, error_message, error_recipients)
        except Exception as e:
            print(f"Failed to send email notification: {e}")

        return []

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get a list of databases from a PostgreSQL server."
    )
    parser.add_argument("server_name", help="Name of the PostgreSQL server")
    parser.add_argument(
        "database_name", help="Target database in the PostgreSQL server"
    )
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    indexes = get_database_index_usage(
        args.server_name, args.username, args.password, args.database_name
    )
    for index in indexes:
        print(index)
