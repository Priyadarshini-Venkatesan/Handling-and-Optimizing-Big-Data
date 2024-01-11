import psycopg2
import random
from datetime import date, timedelta
from faker import Faker
import psycopg2.extras
import json
import csv
from io import StringIO

DATABASE_NAME = 'tr30'
PASSWORD = 'pwd'
# establishing connection to postgres
def connect_postgres(dbname):
    try:
        # Connection parameters
        user = 'priya'
        host = 'localhost'
        password = PASSWORD
        
        # Create a connection to PostgreSQL
        conn = psycopg2.connect(
            user=user,
            dbname=dbname,
            host=host,
            password=password
        )
        
        return conn
    except Exception as e:
        print(f"There was an error while attempting to connect to PostgreSQL: {e}")
        return None

def read_data_from_table(conn, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            
            return data

    except Exception as e:
        print(f"There was an error while reading data from {table_name}: {e}")
        return None

def horizontal_fragmentation_by_state(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT state_name FROM state")
            states = cursor.fetchall()

            for state in states:
                state_name = state[0]
                cursor.execute(f"SELECT * FROM users WHERE state_id IN (SELECT state_id FROM state WHERE state_name = '{state_name}')")
                data = cursor.fetchall()

                print(f"\nData from users table for state {state_name}:")
                for row in data:
                    print(row)

    except Exception as e:
        print(f"There was an error during horizontal fragmentation: {e}")

def vertical_fragmentation_by_columns(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_id, full_name, email, mobile_number, state_id FROM users")
            data = cursor.fetchall()

            print("\nData from users table (vertical fragmentation - user_id, user_name):")
            for row in data:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS users_table1 AS
                SELECT
                    user_id,
                    full_name,
                    email
                FROM
                    users
            """)
                
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS users_table2 AS
                SELECT
                    user_id,
                    mobile_number
                FROM
                    users
            """)
                
            cursor.execute("SELECT * FROM users_table1")
            data_subset1 = cursor.fetchall()
            
            print("\nData from users_subsettable1:")
            for row in data_subset1:
                print(row)

            # Print data from the second subset
            cursor.execute("SELECT * FROM users_table2")
            data_subset2 = cursor.fetchall()
            
            print("\nData from users_subsettable2:")
            for row in data_subset2:
                print(row)

    except Exception as e:
        print(f"There was an error during vertical fragmentation: {e}")

if __name__ == '__main__':
    with connect_postgres(dbname=DATABASE_NAME) as conn:
        if conn:
            # List of tables to read from
            tables = ['state', 'transaction_status', 'account_type', 'users', 'user_preferences', 'accounts', 'transactions']

            for table_name in tables:
                data = read_data_from_table(conn, table_name)

                if data:
                    print(f"\nData from {table_name} table:")
                    for row in data:
                        print(row)

            # Horizontal Fragmentation Example: Users table fragmented by state
            horizontal_fragmentation_by_state(conn)

            # Vertical Fragmentation Example: Users table fragmented by columns
            vertical_fragmentation_by_columns(conn)
