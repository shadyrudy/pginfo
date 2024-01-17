import argparse
import psycopg2


def get_database_table_usage(server_name, user, password, db_name='postgres'):
    try:
        conn = psycopg2.connect(
            host=server_name,
            user=user,
            password=password,
            dbname=db_name
        )
        cursor = conn.cursor()

        query = """
        select current_database() as database_name,       
                schemaname as schema_name,
                relname as table_name,
                seq_scan as sequential_scans,
                seq_tup_read as sequential_tuples_read,
                idx_scan as index_scans,
                idx_tup_fetch as index_tuples_fetched    
        from   pg_stat_user_tables
        order by schemaname, relname;
        """

        cursor.execute(query)
        table_usage = cursor.fetchall()

        return table_usage

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

    tables = get_database_table_usage(args.server_name, args.username, args.password, args.database_name)
    for table in tables:
        print(table)
