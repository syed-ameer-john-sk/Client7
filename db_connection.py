import pymysql
import os

def get_db_connection():
    """
    Establishes a PyMySQL connection using environment variables for configuration.
    These environment variables are expected to be set by the test environment (e.g., conftest.py).
    Falls back to original hardcoded values if environment variables are not set (for non-test execution).
    """
    host = "localhost"
    user = "root"
    password = "DBpass0rd"
    database = "dbtest22"
    port = 3306 # PyMySQL expects port to be an int

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=10 # Add a connection timeout
        )
        # print(f"Attempting to connect to DB: host={host}, port={port}, user={user}, db={database}")
        # Test the connection before returning
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        # print("Database connection successful.")
        return connection
    except pymysql.Error as e:
        print(f"Database connection not successful to {host}:{port}/{database} as user {user}: {e}")
        # Depending on how critical this is outside tests, you might raise the error
        # or return None / handle it. For tests, conftest.py handles failures.
        raise # Re-raise for tests to catch, or for calling code to handle

# The try-except block for printing connection status at import time can be misleading,
# especially if the DB isn't available during general import but is specifically for functions.
# It's better to confirm connection when get_db_connection() is actually called.
# Removing the global print statement.
# try:
#     # This would attempt a connection using defaults if env vars not set,
#     # which might not be desired at import time.
#     # conn = get_db_connection()
#     # conn.close()
#     # print("Able to establish initial DB check (may use defaults or env vars).")
# except Exception:
# print("Initial DB check failed (may use defaults or env vars). Connection will be attempted when get_db_connection() is called.")
    pass