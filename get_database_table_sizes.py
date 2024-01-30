import argparse
import psycopg2


def get_database_table_sizes(server_name, user, password, db_name='postgres'):
    """
    Retrieves the sizes of tables in a PostgreSQL database.

    Args:
        server_name (str): The name or IP address of the PostgreSQL server.
        user (str): The username to connect to the server.
        password (str): The password to authenticate the user.
        db_name (str, optional): The name of the database. Defaults to 'postgres'.

    Returns:
        list: A list of tuples containing the following information for each table:
            - database_name: The name of the database.
            - schema_name: The name of the schema.
            - table_name: The name of the table.
            - table_size: The size of the table in bytes.
            - index_size: The size of the table's indexes in bytes.
            - total_size: The total size of the table including indexes in bytes.
            - row_estimate: The estimated number of rows in the table.

    Raises:
        Exception: If an error occurs while connecting to the database or executing the query.

    """
    try:
        conn = psycopg2.connect(
            host=server_name,
            user=user,
            password=password,
            dbname=db_name
        )
        cursor = conn.cursor()

        query = """
        SELECT  current_database() as database_name,
                nspname AS schema_name,
                relname AS table_name,
                pg_table_size(C.oid) AS table_size,
                pg_indexes_size(C.oid) AS index_size,
                pg_total_relation_size(C.oid) AS total_size,
                C.reltuples AS row_estimate
        FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
        WHERE nspname NOT IN ('pg_catalog', 'information_schema')
        AND   relkind = 'r'
        ORDER BY pg_total_relation_size(C.oid) DESC;
        """

        cursor.execute(query)
        table_sizes = cursor.fetchall()

        return table_sizes

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get a list of databases from a PostgreSQL server.")
    parser.add_argument("server_name", help="Name of the PostgreSQL server")
    parser.add_argument("database_name", help="Target database in the PostgreSQL server")
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    tables = get_database_table_sizes(args.server_name, args.username, args.password, args.database_name)
    for table in tables:
        print(table)
