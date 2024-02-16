import argparse
import psycopg2
from send_mail import send_mail


def get_database_sizes(server_name, user, password, db_name="postgres"):
    """
    Retrieves the sizes of all non-template databases on a PostgreSQL server.

    Args:
        server_name (str): The name or IP address of the PostgreSQL server.
        user (str): The username to connect to the server.
        password (str): The password to authenticate with the server.
        db_name (str, optional): The name of the database to connect to. Defaults to 'postgres'.

    Returns:
        list: A list of tuples containing the database name, size in MB, and size in GB.

    Raises:
        Exception: If an error occurs while connecting to the server or executing the query.
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
        SELECT datname,
               pg_database_size(datname)/1024/1024 AS size_mb,
               pg_database_size(datname)/1024/1024/1024 AS size_gb
        FROM pg_database
        WHERE datistemplate = false;
        """

        cursor.execute(query)

        databases = cursor.fetchall()

        return databases

    except Exception as e:
        function_name = get_database_sizes.__name__
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
    parser.add_argument("server_name", help="Name of the PostgreSQL server.")
    parser.add_argument("username", help="Username for the PostgreSQL server.")
    parser.add_argument("password", help="Password for the PostgreSQL server.")

    args = parser.parse_args()

    dbs = get_database_sizes(args.server_name, args.username, args.password)
    for db in dbs:
        print(db)
