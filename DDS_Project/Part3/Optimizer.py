import psycopg2
import time

DATABASE_NAME = 'tr1'
PASSWORD = '8531'

def connect_postgres(dbname):
    try:
        user = 'postgres'
        host = 'localhost'
        password = PASSWORD

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

def create_indexes(conn):
    try:
        with conn.cursor() as cursor:
            # Indexes for the WHERE clause conditions
            cursor.execute('CREATE INDEX idx_sender_account_id ON transactions(sender_account_id)')
            
            # Indexes for join conditions
            cursor.execute('CREATE INDEX idx_transactions_sender ON transactions(sender_account_id)')
            cursor.execute('CREATE INDEX idx_accounts_sender ON accounts(account_id)')
            cursor.execute('CREATE INDEX idx_users_sender ON users(user_id)')
            cursor.execute('CREATE INDEX idx_state_sender ON state(state_id)')
            cursor.execute('CREATE INDEX idx_accounts_receiver ON accounts(account_id)')
            cursor.execute('CREATE INDEX idx_users_receiver ON users(user_id)')
            cursor.execute('CREATE INDEX idx_user_preferences_receiver ON user_preferences(user_id)')
            cursor.execute('CREATE INDEX idx_status ON transaction_status(status_id)')

            conn.commit()

    except Exception as e:
        print(f"There was an error while creating indexes: {e}")

def drop_index(conn, index_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP INDEX IF EXISTS {index_name}")

        print(f"Index '{index_name}' dropped successfully.")

    except Exception as e:
        print(f"There was an error while dropping the index: {e}")

def query_optimizer(original_query):
    # Extract SELECT, FROM, WHERE parts from the original query
    select_template = original_query.split("FROM")[0].strip()
    from_template = original_query.split("FROM")[1].split("WHERE")[0].strip()
    where_condition = original_query.split("WHERE")[1].strip()

    # Build a template for the JOIN part
    join_template = ""
    tables = from_template.split("INNER JOIN")[1:]
    for table in tables:
        join_template += f" "
        join_template += f"INNER JOIN {table.strip()}"

    # Build the optimized query dynamically
    optimized_query = f"""
        {select_template}
        FROM 
            (SELECT * FROM transactions WHERE {where_condition}) AS transactions
        {join_template}
    """

    return optimized_query
    
def measure_execution_time(conn, query):
    try:
        start_time = time.time()
        with conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
        end_time = time.time()

        execution_time = end_time - start_time
        return execution_time,data

    except Exception as e:
        print(f"There was an error while executing the query: {e}")
        return None

if __name__ == '__main__':
    with connect_postgres(dbname=DATABASE_NAME) as conn:
        if conn:

            # Specify the index names you want to drop
            index_names_to_drop = ['idx_sender_account_id', 'idx_transactions_sender', 'idx_accounts_sender', 'idx_users_sender','idx_state_sender','idx_accounts_receiver','idx_users_receiver','idx_user_preferences_receiver','idx_status']

            for index_name in index_names_to_drop:
                drop_index(conn, index_name)

            # Original Query
            original_query = """
                SELECT 
                    transactions.transaction_id, 
                    sender_user.full_name AS sender_name, 
                    sender_state.state_name AS sender_state,
                    receiver_user.full_name AS receiver_name,
                    receiver_preferences.notification_preference AS receiver_notification_preference,
                    status.status_name,
                    transactions.amount
                FROM transactions
                INNER JOIN 
                    accounts AS sender ON transactions.sender_account_id = sender.account_id
                INNER JOIN 
                    users AS sender_user ON sender.user_id = sender_user.user_id
                INNER JOIN 
                    state AS sender_state ON sender_user.state_id = sender_state.state_id
                INNER JOIN 
                    accounts AS receiver ON transactions.receiver_account_id = receiver.account_id
                INNER JOIN 
                    users AS receiver_user ON receiver.user_id = receiver_user.user_id
                INNER JOIN 
                    user_preferences AS receiver_preferences ON receiver_user.user_id = receiver_preferences.user_id
                INNER JOIN 
                    transaction_status AS status ON transactions.status_id = status.status_id
                WHERE 
                    transactions.sender_account_id < 5
            """
            explain_query = f"EXPLAIN ANALYZE {original_query}"
            print("Execution Plan for Original query :")
            cursor = conn.cursor()
            cursor.execute(explain_query)
            # Fetch and print the result
            result = cursor.fetchall()
            for row in result:
                print(row)
            
            original_execution_time, data = measure_execution_time(conn, original_query)
            
            # Create indexes
            create_indexes(conn)

            # Optimized Query
            # This approach aims to minimize the unnecessary joining of tables for data that is not intended, 
            # focusing on selecting relevant rows first and then performing joins.
            optimized_query = query_optimizer(original_query)

            explain_query_op = f"EXPLAIN ANALYZE {optimized_query}"
            print("Execution Plan for Optimized query :")
            cursor = conn.cursor()
            cursor.execute(explain_query_op)
            # Fetch and print the result
            result = cursor.fetchall()
            for row in result:
                print(row)


            optimized_execution_time, op_data = measure_execution_time(conn, optimized_query)

            # Calculate savings
            savings_percentage = ((original_execution_time - optimized_execution_time) / original_execution_time) * 100

            #print("\nOriginal Query:")
            #print(original_query)
            print(f"\nOriginal query returned results: {data}")
            print(f"\nOriginal Execution Time: {original_execution_time} seconds")

            #print("\nOptimized Query:")
            #print(optimized_query)
            print(f"\nOptimized query returned results: {op_data}")
            print(f"\nOptimized Execution Time: {optimized_execution_time} seconds")

            print(f"\nSavings: {savings_percentage:.2f}%")
