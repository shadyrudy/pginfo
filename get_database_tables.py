import argparse
import psycopg2


def get_database_tables(server_name, user, password, db_name="postgres"):
    """
    Retrieves a list of tables from a PostgreSQL database.

    Args:
        server_name (str): Name of the PostgreSQL server.
        user (str): Username for the PostgreSQL server.
        password (str): Password for the PostgreSQL server.
        db_name (str, optional): Target database in the PostgreSQL server. Defaults to 'postgres'.

    Returns:
        list: A list of tuples representing the table schema and table name.

    Raises:
        Exception: If an error occurs while connecting to the database.

    """
    try:
        conn = psycopg2.connect(
            host=server_name, user=user, password=password, dbname=db_name
        )
        cursor = conn.cursor()

        query = """
        SELECT t.table_schema, 
                t.table_name
        FROM information_schema.tables as t
        WHERE t.table_type = 'BASE TABLE'
        AND t.table_schema not in ('pg_catalog', 'information_schema')
        ORDER BY t.table_schema, t.table_name;
        """

        cursor.execute(query)
        table_list = cursor.fetchall()

        return table_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        cursor.close()
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

    tables = get_database_tables(
        args.server_name, args.username, args.password, args.database_name
    )
    for table in tables:
        print(table)
