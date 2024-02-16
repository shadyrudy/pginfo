import argparse
import psycopg2
from send_mail import send_mail


def get_database_indexes(server_name, user, password, db_name="postgres"):
    """
    Retrieves a list of indexes from a PostgreSQL database.

    Args:
        server_name (str): Name of the PostgreSQL server.
        user (str): Username for the PostgreSQL server.
        password (str): Password for the PostgreSQL server.
        db_name (str, optional): Target database in the PostgreSQL server. Defaults to 'postgres'.

    Returns:
        list: A list of tuples representing the indexes in the database, including the table name and the schema name.

    Raises:
        Exception: If an error occurs while connecting to the database.

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
            n.nspname AS schema_name,
            t.relname AS table_name,
            i.relname AS index_name,
            pg_relation_size(i.oid) AS index_size_bytes,
            LEFT(pg_get_indexdef(i.oid), 255) AS index_definition
        FROM pg_class t
        JOIN pg_index x ON t.oid = x.indrelid
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE t.relkind = 'r' AND i.relkind = 'i'
        and  n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, t.relname, i.relname;
        """

        cursor.execute(query)
        index_list = cursor.fetchall()

        return index_list

    except Exception as e:
        function_name = get_database_indexes.__name__
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
        description="Get a list of indexes in a databases from a PostgreSQL server."
    )
    parser.add_argument("server_name", help="Name of the PostgreSQL server")
    parser.add_argument(
        "database_name", help="Target database in the PostgreSQL server"
    )
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    indexes = get_database_indexes(
        args.server_name, args.username, args.password, args.database_name
    )
    for idx in indexes:
        print(idx)
