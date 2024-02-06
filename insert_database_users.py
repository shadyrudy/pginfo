import argparse
import psycopg2
from get_database_users import get_database_users


def insert_database_users(
    target_server, target_username, target_password, dba_username, dba_password
):
    """
    Inserts database users from the target PostgreSQL server into the DBAAdmin database.

    Args:
        target_server (str): Name of the target PostgreSQL server.
        target_username (str): Username for the target PostgreSQL server.
        target_password (str): Password for the target PostgreSQL server.
        dba_username (str): Username for the DBA PostgreSQL server.
        dba_password (str): Password for the DBA PostgreSQL server.
    """
    # Get database users from the target server
    database_users = get_database_users(target_server, target_username, target_password)

    # Connect to the DBA001 server
    conn_dba = psycopg2.connect(
        host="ec2-54-188-102-105.us-west-2.compute.amazonaws.com",
        user=dba_username,
        password=dba_password,
        dbname="dbaadmin",
    )
    cursor_dba = conn_dba.cursor()

    # Insert into dba.database_sizes table
    for (
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcanlogin,
        rolreplication,
        rolconnlimit,
        rolvaliduntil,
        memberof,
        rolconfig,
    ) in database_users:
        cursor_dba.execute(
            "INSERT INTO dba.users(server_name, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolconnlimit, rolvaliduntil, memberof, rolconfig, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ARRAY%s, ARRAY%s, CURRENT_TIMESTAMP)",
            (
                target_server,
                rolname,
                rolsuper,
                rolinherit,
                rolcreaterole,
                rolcreatedb,
                rolcanlogin,
                rolreplication,
                rolconnlimit,
                rolvaliduntil,
                (memberof,),
                (rolconfig,),
            ),
        )

    # Commit changes and close connection
    conn_dba.commit()
    cursor_dba.close()
    conn_dba.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert database users into the DBAAdmin database."
    )
    parser.add_argument("target_server", help="Name of the target PostgreSQL server")
    parser.add_argument(
        "target_username", help="Username for the target PostgreSQL server"
    )
    parser.add_argument(
        "target_password", help="Password for the target PostgreSQL server"
    )
    parser.add_argument("dba_username", help="Username for the DBA PostgreSQL server")
    parser.add_argument("dba_password", help="Password for the DBA PostgreSQL server")

    args = parser.parse_args()

    insert_database_users(
        args.target_server,
        args.target_username,
        args.target_password,
        args.dba_username,
        args.dba_password,
    )
