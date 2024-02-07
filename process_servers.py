from dotenv import dotenv_values
import argparse
from get_servers import get_servers
from insert_database_sizes import insert_database_sizes
from insert_database_table_sizes import insert_database_table_sizes
from insert_database_table_usage import insert_database_table_usage
from insert_database_grants import insert_database_grants
from insert_database_users import insert_database_users


def process_servers(dba_username, dba_password):
    """
    Import key information from the active servers in the DBA database.
    First, get database sizes for all databases on the server.
    Next, get table sizes for all databases on the server.
    Next, get table usage for all databases on the server.
    Next, get all users on the server.
    Finally, get grants for all databases on the server.

    Args:
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.

    Raises:
        Exception: An error occurred while connecting to the DBA database.
    """
    # Get servers from the DBA database
    servers = get_servers(dba_username, dba_password)

    # Load the .env file
    env_values = dotenv_values(".env")

    # Access the username and password variables
    current_username = env_values["DB_USERNAME"]
    current_password = env_values["DB_PASSWORD"]

    # Foreach server, process it
    for server in servers:
        print(f"Processing server: {server}")

        # Get database sizes for all databases on the server
        insert_database_sizes(
            server, current_username, current_password, dba_username, dba_password
        )

        # Next, get table sizes for all databases on the server
        insert_database_table_sizes(
            server, current_username, current_password, dba_username, dba_password
        )

        # next, get table usage for all databases on the server
        insert_database_table_usage(
            server, current_username, current_password, dba_username, dba_password
        )

        # Next, get all users on the server
        insert_database_users(
            server, current_username, current_password, dba_username, dba_password
        )
        print(f"    Finished with server: {server}")

        # Next, get grants for all databases on the server
        insert_database_grants(
            server, current_username, current_password, dba_username, dba_password
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process servers from the DBA database."
    )
    parser.add_argument("dba_username", help="Username for the DBA PostgreSQL server")
    parser.add_argument("dba_password", help="Password for the DBA PostgreSQL server")

    args = parser.parse_args()

    # Process servers from the DBA database
    process_servers(args.dba_username, args.dba_password)
