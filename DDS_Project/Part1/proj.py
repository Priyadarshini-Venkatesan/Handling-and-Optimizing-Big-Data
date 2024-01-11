import psycopg2
import random
from datetime import date, timedelta
from faker import Faker
import psycopg2.extras
import json
import csv
from io import StringIO

DATABASE_NAME = 'tr1'
PASSWORD = '8531'
# establishing connection to postgres
def connect_postgres(dbname):
    try:
        # Connection parameters
        user = 'postgres'
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

# Creating a database
def getOpenConnection(user='postgres', password=PASSWORD, dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_database(dbname):
    # Connect to the default database
    con = getOpenConnection()
    print("establisted connection to Postgres!!")
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  
        print("Database has been created successfully")
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

def create_tables(conn):
    try:
        with conn.cursor() as cursor:
            # Table: state
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    state_id SERIAL PRIMARY KEY,
                    state_name VARCHAR
                )
            """)

            # Table: transaction_status
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_status (
                    status_id SERIAL PRIMARY KEY,
                    status_name VARCHAR
                )
            """)

            # Table: account_type
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS account_type (
                    type_id SERIAL PRIMARY KEY,
                    type_name VARCHAR
                )
            """)

            # Table: users
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    full_name VARCHAR,
                    email VARCHAR UNIQUE,
                    mobile_number INT,
                    state_id INT REFERENCES state(state_id)
                )
            """)

            # Table: user_preferences
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id INT PRIMARY KEY REFERENCES users(user_id),
                    notification_preference VARCHAR
                )
            """)

            # Table: accounts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL REFERENCES users(user_id),
                    balance DECIMAL DEFAULT 0.00,
                    type_id INT REFERENCES account_type(type_id)
                )
            """)

            # Table: transactions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    sender_account_id INT NOT NULL REFERENCES accounts(account_id),
                    receiver_account_id INT NOT NULL REFERENCES accounts(account_id),
                    amount DECIMAL,
                    status_id INT REFERENCES transaction_status(status_id),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT transaction_accounts_idx 
                        UNIQUE (sender_account_id, receiver_account_id)
                )
            """)

            print("Tables created successfully!")

    except Exception as e:
        print(f"There was an error while creating tables: {e}")

def insert_sample_data(conn):
    fake = Faker()
    
    with conn.cursor() as cursor:
        # Insert all States in the USA into the 'state' table
        states = [
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
            'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
            'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri',
            'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
            'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
            'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
            'West Virginia', 'Wisconsin', 'Wyoming'
        ]
        for state_name in states:
            cursor.execute("INSERT INTO state (state_name) VALUES (%s)", (state_name,))

        # Insert sample data into the 'transaction_status' table
        cursor.execute("INSERT INTO transaction_status (status_name) VALUES ('Success'), ('Failure'), ('Pending')")

        # Insert sample data into the 'account_type' table
        cursor.execute("INSERT INTO account_type (type_name) VALUES ('Savings'), ('Checking')")

        # Insert 100 users into the 'users' table
        for user_id in range(1, 101):
            # Ensure that the state_id exists in the 'state' table
            state_id = random.randint(1, len(states))
            cursor.execute("INSERT INTO users (full_name, email, mobile_number, state_id) VALUES (%s, %s, %s, %s)",
                           (fake.name(), fake.email(), fake.random_int(min=100000000, max=999999999), state_id))

        # Insert sample data into the 'user_preferences' table
        for user_id in range(1, 101):
            cursor.execute(
                "INSERT INTO user_preferences (user_id, notification_preference) VALUES (%s, %s)",
                (user_id, random.choice(['Email', 'SMS', 'Mail']))
            )

        # Insert account details for existing users into the 'accounts' table
        for user_id in range(1, 101):
            cursor.execute(
                "INSERT INTO accounts (user_id, balance, type_id) VALUES (%s, %s, %s)",
                (user_id, random.randint(100, 5000), random.randint(1, 2))
            )

        # Insert 50 transactions between different user accounts into the 'transactions' table
        for _ in range(50):
            sender_account_id = random.randint(1, 100)
            receiver_account_id = sender_account_id
            while receiver_account_id == sender_account_id:
                receiver_account_id = random.randint(1, 100)

            cursor.execute(
                "INSERT INTO transactions (sender_account_id, receiver_account_id, amount, status_id) VALUES (%s, %s, %s, %s)",
                (sender_account_id, receiver_account_id, random.randint(50, 500), random.randint(1, 3))
            )

    conn.commit()
    print("Sample data inserted successfully!")

def export_schema(conn, file_name='schema.json'):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("SELECT table_name, column_name, data_type FROM information_schema.columns")

            # Fetch schema information as a dictionary
            schema_data = {}
            for row in cursor.fetchall():
                table_name = row['table_name']
                column_name = row['column_name']
                data_type = row['data_type']

                if table_name not in schema_data:
                    schema_data[table_name] = {}

                schema_data[table_name][column_name] = data_type

            # Dump the schema to a JSON file
            with open(file_name, 'w') as schema_file:
                json.dump(schema_data, schema_file, indent=2)

            print(f"Schema exported to {file_name}")

    except Exception as e:
        print(f"There was an error while exporting schema: {e}")

def export_tables_to_csv(conn):
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            tables = ['state', 'transaction_status', 'account_type', 'users', 'user_preferences', 'accounts', 'transactions']

            for table in tables:
                cursor.execute(f"SELECT * FROM {table}")

                # Fetch data from the table
                data = cursor.fetchall()

                # Export data to CSV file
                csv_file_name = f"{table}.csv"
                with open(csv_file_name, 'w', newline='') as csv_file:
                    csv_writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
                    csv_writer.writeheader()
                    csv_writer.writerows(data)

                print(f"{table} exported to {csv_file_name}")

    except Exception as e:
        print(f"There was an error while exporting tables to CSV: {e}")

def print_tables(conn):
    # Display data from the tables
    with conn.cursor() as cursor:
        # Display data from the 'state' table
        cursor.execute("SELECT * FROM state")
        print("State Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'transaction_status' table
        cursor.execute("SELECT * FROM transaction_status")
        print("\nTransaction Status Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'account_type' table
        cursor.execute("SELECT * FROM account_type")
        print("\nAccount Type Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'users' table
        cursor.execute("SELECT * FROM users")
        print("\nUsers Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'user_preferences' table
        cursor.execute("SELECT * FROM user_preferences")
        print("\nUser Preferences Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'accounts' table
        cursor.execute("SELECT * FROM accounts")
        print("\nAccounts Table:")
        for row in cursor.fetchall():
            print(row)

        # Display data from the 'transactions' table
        cursor.execute("SELECT * FROM transactions")
        print("\nTransactions Table:")
        for row in cursor.fetchall():
            print(row)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    create_database(DATABASE_NAME)

    with connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        #create_tables(conn)
        #insert_sample_data(conn)
        export_tables_to_csv(conn)
        #print_tables(conn)

        