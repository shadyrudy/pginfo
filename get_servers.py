import argparse
import psycopg2


def get_servers(dba_username, dba_password):
    """
    Get a list of active servers from the DBA server.

    Args:
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.

    Returns:
        list: A list of active server names from the DBA database. These servers
        will be used for metrics collection
    """
    conn_dba = None  # Initialize connection outside try block
    cursor = None  # Initialize cursor outside try block

    try:
        # Connect to the DBA001 server
        conn_dba = psycopg2.connect(
            host="DBA001", user=dba_username, password=dba_password, dbname="dbaadmin"
        )
        cursor = conn_dba.cursor()

        cursor.execute("SELECT server_name FROM dba.servers WHERE server_status = 1")
        server_list = [db[0] for db in cursor.fetchall()]

        return server_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        cursor.close()
        conn_dba.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get a list of databases from a PostgreSQL server."
    )
    parser.add_argument("dba_username", help="Username for the DBA PostgreSQL server")
    parser.add_argument("dba_password", help="Password for the DBA PostgreSQL server")

    args = parser.parse_args()

    active_servers = get_servers(args.dba_username, args.dba_password)
    for active_server in active_servers:
        print(active_server)
