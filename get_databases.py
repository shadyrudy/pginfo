import argparse
import psycopg2


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
    conn = None  # Initialize connection outside try block
    cursor = None  # Initialize cursor outside try block

    try:
        conn = psycopg2.connect(
            host=server_name, user=user, password=password, dbname=db_name
        )
        cursor = conn.cursor()

        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [db[0] for db in cursor.fetchall()]

        return databases

    except Exception as e:
        print(f"An error occurred: {e}")
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
