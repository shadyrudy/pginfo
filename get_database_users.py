import argparse
import psycopg2


def get_database_users(server_name, user, password, db_name="postgres"):
    """
    Retrieve the users in a PostgreSQL server.

    Args:
        server_name (str): Name or IP address of the PostgreSQL server.
        user (str): Username for the PostgreSQL server.
        password (str): Password for the PostgreSQL server.
        db_name (str, optional): Name of the database to connect to. Defaults to 'postgres'.

    Returns:
        list: A list of users in the PostgreSQL server.
              Each tuple contains the following information:
                - Username: The name of the user.
                - Superuser: Whether the user is a superuser.
                - Inherit: Whether the user can inherit privileges.
                - Create Role: Whether the user can create roles.
                - Create DB: Whether the user can create databases.
                - Can Login: Whether the user can log in.
                - Replication: Whether the user can perform replication.
                - Conn Limit: The connection limit for the user.
                - Valid Until: The date and time the user's password is valid until.
                - Member Of: The roles the user is a member of.
                - Configuration: The user's configuration settings.

    Raises:
        Exception: If an error occurs while connecting to the database or executing the query.
    """

    conn = None  # Initialize connection outside try block
    cursor = None  # Initialize cursor outside try block

    try:
        conn = psycopg2.connect(
            host=server_name, user=user, password=password, dbname=db_name
        )
        cursor = conn.cursor()

        query = """
        SELECT r.rolname
            ,r.rolsuper
            ,r.rolinherit
            ,r.rolcreaterole
            ,r.rolcreatedb
            ,r.rolcanlogin
            ,r.rolreplication
            ,r.rolconnlimit
            ,r.rolvaliduntil
            , ARRAY(SELECT b.rolname 
            FROM pg_catalog.pg_auth_members m 
            JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) AS memberof
            ,r.rolconfig
        FROM pg_catalog.pg_roles r
        ORDER BY 1;
        """

        cursor.execute(query)
        database_users = cursor.fetchall()

        return database_users

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
        description="Get a list of users from a PostgreSQL server."
    )
    parser.add_argument("server_name", help="Name of the PostgreSQL server")
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    db_users = get_database_users(args.server_name, args.username, args.password)
    for db_user in db_users:
        print(db_user)
