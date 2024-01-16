import argparse
import psycopg2


def get_databases(server_name, user, password, db_name='postgres'):
    try:
        conn = psycopg2.connect(
            host=server_name,
            user=user,
            password=password,
            dbname=db_name
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
        print(f"An error occurred: {e}")
        return []

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get a list of databases from a PostgreSQL server.")
    parser.add_argument("server_name", help="Name of the PostgreSQL server.")
    parser.add_argument("username", help="Username for the PostgreSQL server.")
    parser.add_argument("password", help="Password for the PostgreSQL server.")

    args = parser.parse_args()

    dbs = get_databases(args.server_name, args.username, args.password)
    for db in dbs:
        print(db)