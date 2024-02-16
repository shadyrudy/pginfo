import argparse
import psycopg2
from send_mail import send_mail


def get_databases(server_name, user, password, db_name="postgres"):
    """
    Get a list of databases from a PostgreSQL server.

    Args:
        server_name (str): Name of the PostgreSQL server.
        user (str): Username for the PostgreSQL server.
        password (str): Password for the PostgreSQL server.
        db_name (str, optional): Name of the database to connect to. Defaults to 'postgres'.

    Returns:
        list: A list of database names.

    Raises:
        Exception: If an error occurs while connecting to the database.

    """

    # Initialize connection and cursor
    conn = None  # Initialize connection outside try block
    cursor = None  # Initialize cursor outside try block

    try:
        conn = psycopg2.connect(
            host=server_name, user=user, password=password, dbname=db_name
        )
        cursor = conn.cursor()

        # Get a list of non template databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [db[0] for db in cursor.fetchall()]

        return databases

    except Exception as e:
        function_name = get_databases.__name__
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

    dbs = get_databases(args.server_name, args.username, args.password)
    for db in dbs:
        print(db)
