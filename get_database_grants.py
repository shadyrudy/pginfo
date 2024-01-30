import argparse
import psycopg2


def get_database_grants(server_name, user, password, db_name="postgres"):
    """
    Retrieve the grants for objects in a PostgreSQL database.

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
        with results as (
        SELECT rug.grantor, rug.grantee, rug.object_catalog, rug.object_schema, rug.object_name, rug.object_type, rug.privilege_type, rug.is_grantable, null::text AS with_hierarchy
        FROM information_schema.role_usage_grants rug
        WHERE rug.object_schema NOT IN ('pg_catalog', 'information_schema')
        AND grantor <> grantee
        UNION
        SELECT rtg.grantor, rtg.grantee, rtg.table_catalog, rtg.table_schema, rtg.table_name, tab.table_type, rtg.privilege_type, rtg.is_grantable, rtg.with_hierarchy
        FROM information_schema.role_table_grants rtg
        LEFT JOIN information_schema.tables tab
        ON (tab.table_catalog = rtg.table_catalog AND tab.table_schema = rtg.table_schema AND tab.table_name = rtg.table_name)
        WHERE rtg.table_schema NOT IN ('pg_catalog', 'information_schema')
        AND grantor <> grantee
        UNION
        SELECT rrg.grantor, rrg.grantee, rrg.routine_catalog, rrg.routine_schema, rrg.routine_name, fcn.routine_type, rrg.privilege_type, rrg.is_grantable, null::text AS with_hierarchy
        FROM information_schema.role_routine_grants rrg
        LEFT JOIN information_schema.routines fcn
        ON (fcn.routine_catalog = rrg.routine_catalog AND fcn.routine_schema = rrg.routine_schema AND fcn.routine_name = rrg.routine_name)
        WHERE rrg.specific_schema NOT IN ('pg_catalog', 'information_schema')
        AND grantor <> grantee
        UNION
        SELECT rug.grantor, rug.grantee, rug.udt_catalog, rug.udt_schema, rug.udt_name, ''::text AS udt_type, rug.privilege_type, rug.is_grantable, null::text AS with_hierarchy
        FROM information_schema.role_udt_grants rug
        WHERE rug.udt_schema NOT IN ('pg_catalog', 'information_schema')
        AND substr (rug.udt_schema, 1, 3) <> 'pg_'
        AND grantor <> grantee
        )
        SELECT results.object_catalog as database_name
        , results.object_schema as schema_name
        , results.object_name
        , results.object_type
        , results.grantor
        , results.grantee
        , results.privilege_type
        , results.is_grantable
        , results.with_hierarchy
        from results
        order by object_catalog, object_schema, object_name, object_type;
        """

        cursor.execute(query)
        database_grants = cursor.fetchall()

        return database_grants

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
    parser.add_argument("server_name", help="Name of the PostgreSQL server")
    parser.add_argument(
        "database_name", help="Target database in the PostgreSQL server"
    )
    parser.add_argument("username", help="Username for the PostgreSQL server")
    parser.add_argument("password", help="Password for the PostgreSQL server")

    args = parser.parse_args()

    db_grants = get_database_grants(
        args.server_name, args.username, args.password, args.database_name
    )
    for db_grant in db_grants:
        print(db_grant)
