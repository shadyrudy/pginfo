import argparse
import psycopg2


def get_database_users(server_name, user, password, db_name="postgres"):
    """
    Retrieve the users in a PostgreSQL server.

    Args:
        server_name (str): Name or IP address of the PostgreSQL server.
        user (str): Username for the PostgreSQL server.
        password (str): Password for the PostgreSQL server.
        db_name (str, optional): Name of the target database. Defaults to 'postgres'.

    Returns:
        list: A list of tuples representing the grants for objects in the database.
              Each tuple contains the following information:
              - Grantor: The role that granted the privilege.
              - Grantee: The role that received the privilege.
              - Object Catalog: The catalog (database) of the object.
              - Object Schema: The schema of the object.
              - Object Name: The name of the object.
              - Object Type: The type of the object (e.g., table, function, etc.).
              - Privilege Type: The type of privilege granted.
              - Is Grantable: Whether the privilege is grantable.
              - With Hierarchy: Additional information for certain object types.

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
    parser.add_argument(
        "database_name", help="Target database in the PostgreSQL server"
    )
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    db_users = get_database_users(
        args.server_name, args.username, args.password, args.database_name
    )
    for db_user in db_users:
        print(db_user)
