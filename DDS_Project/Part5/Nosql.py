from pymongo import MongoClient
import random
from datetime import datetime


def create_collections():

    db.states.create_index("state_name", unique=True)# Collection: state
    db.transaction_status.create_index("status_name", unique=True)# Collection: transaction_status
    db.account_type.create_index("type_name", unique=True) # Collection: account_type
    db.users.create_index("email", unique=True)# Collection: users
    db.users.create_index("state_id")
    db.user_preferences.create_index("user_id", unique=True)# Collection: user_preferences
    db.accounts.create_index("user_id", unique=True)# Collection: accounts
    db.accounts.create_index("type_id")
    db.transactions.create_index([("sender_account_id", 1), ("receiver_account_id", 1)], unique=True)# Collection: transactions
    db.transactions.create_index("status_id")
    
    print("Collections created successfully!")

def insert_data():
    # Insert all States in the USA into the 'state' collection
    states = [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
        'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
        'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri',
        'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
        'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming'
    ]
    db.states.insert_many([{"state_name": state_name} for state_name in states])

    # Insert sample data into the 'transaction_status' collection
    db.transaction_status.insert_many([
        {"status_name": "Success"},
        {"status_name": "Failure"},
        {"status_name": "Pending"}
    ])

    # Insert sample data into the 'account_type' collection
    db.account_type.insert_many([
        {"type_name": "Savings"},
        {"type_name": "Checking"}
    ])

    # Insert 100 users into the 'users' collection
    for user_id in range(1, 101):
        state_id = random.choice(db.states.distinct("_id"))
        db.users.insert_one({
            "user_id": user_id,
            "full_name": f"User {user_id}",
            "email": f"user{user_id}@example.com",
            "mobile_number": random.randint(100000000, 999999999),
            "state_id": state_id
        })

    # Insert sample data into the 'user_preferences' collection
    db.user_preferences.insert_many([
        {"user_id": user_id, "notification_preference": random.choice(['Email', 'SMS', 'Mail'])}
        for user_id in range(1, 101)
    ])

    # Insert account details for existing users into the 'accounts' collection
    for user_id in range(1, 101):
        type_id = random.choice(db.account_type.distinct("_id"))
        db.accounts.insert_one({
            "user_id": user_id,
            "balance": random.randint(100, 5000),
            "type_id": type_id
        })

    # Insert 50 transactions between different user accounts into the 'transactions' collection
    for _ in range(50):
        sender_account_id = random.choice(db.accounts.distinct("user_id"))
        receiver_account_id = sender_account_id
        while receiver_account_id == sender_account_id:
            receiver_account_id = random.choice(db.accounts.distinct("user_id"))

        db.transactions.insert_one({
            "sender_account_id": sender_account_id,
            "receiver_account_id": receiver_account_id,
            "amount": random.randint(50, 500),
            "status_id": random.choice(db.transaction_status.distinct("_id")),
            "timestamp": datetime.utcnow()
        })

    print("Sample data inserted successfully!")

def update_user(user_id):
    new_pref = "Push"
    db.user_preferences.update_one(
        {"user_id": user_id},
        {"$set": {"notification_preference": new_pref}}
    )
    print(f"\nUser {user_id}'s notification preference updated to {new_pref}.")

def delete_user(user_id):
    db.users.delete_one({"user_id": user_id})
    db.user_preferences.delete_one({"user_id": user_id})
    db.accounts.delete_one({"user_id": user_id})
    db.transactions.delete_many({"$or": [{"sender_account_id": user_id}, {"receiver_account_id": user_id}]})
    print(f"\nUser {user_id} and associated data deleted successfully.")

def print_data(): 

    datas = db["users"].find()
    print("\nUsers: ")
    for data in datas:
        print(data) 

    datas = db["user_preferences"].find()
    print("\nUser Preferences:")
    for data in datas:
        print(data) 

    datas = db["accounts"].find()
    print("\nAccounts:")
    for data in datas:
        print(data) 

    datas = db["transactions"].find()
    print("\nTransactions:")
    for data in datas:
        print(data) 

if __name__ == '__main__':

    client = MongoClient("mongodb://localhost:27017/")
    db = client["Transactions"]

    create_collections() #create
    insert_data()
    print_data() #read
    update_user(2) #update
    delete_user(1) #delete
    print_data()

    client.close()
