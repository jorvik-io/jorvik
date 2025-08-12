# This Python script generates 2 sample datasets: customers and transactions to demonstrate the examples.
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
from pathlib import Path

# Modify these constants to control data volume and location
N_CUSTOMERS = 100
N_TRANSACTIONS = 100_000
CUSTOMERS_PATH = '/tmp/sources/customers.csv'
TRANSACTIONS_PATH = '/tmp/sources/transactions_db.sqlite'

def random_dates(start: datetime, end: datetime, n: int) -> list[datetime]:
    """Generates a list of random dates between start and end"""
    start_u = start.timestamp()
    end_u = end.timestamp()
    return [datetime.fromtimestamp(np.random.uniform(start_u, end_u)) for _ in range(n)]

def generate_customers(num_rows: int) -> pd.DataFrame:
    """Generates a DataFrame of dummy customer data"""
    first_names = [
        "John", "Jane", "Michael", "Emily", "David", "Sarah", "Chris", "Jessica",
        "Daniel", "Laura", "James", "Olivia", "Matthew", "Emma", "Joshua", "Sophia"
    ]
    last_names = [
        "Smith", "Johnson", "Brown", "Williams", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson"
    ]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
              "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]

    start_date = datetime(2022, 1, 1, 0, 0, 0)
    end_date = datetime(2022, 6, 1, 0, 0, 0)

    data = []
    for customer_id in range(1, num_rows + 1):
        first = np.random.choice(first_names)
        last = np.random.choice(last_names)
        email = f"{first.lower()}.{last.lower()}@mail.com"
        age = np.random.randint(15, 80)
        city = np.random.choice(cities)
        reg_date = random_dates(start_date, end_date, 1)[0].date()
        data.append((customer_id, f"{first} {last}", email, age, city, reg_date))

    return pd.DataFrame(data, columns=[
        "customer_id", "name", "email", "age", "city", "registration_date"
    ])

def generate_transactions(num_rows: int, n_customers: int = N_CUSTOMERS) -> pd.DataFrame:
    """Generates a DataFrame of dummy transaction data"""
    start_date = datetime(2022, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 6, 1, 0, 0, 0)

    data = {
        "transaction_id": np.arange(1, num_rows + 1),
        "customer_id": np.random.randint(1, n_customers + 1, size=num_rows),
        "product_id": np.random.randint(1, 21, size=num_rows),
        "quantity": np.random.randint(1, 101, size=num_rows),
        "price": np.round(np.random.uniform(10.0, 100.0, size=num_rows), 2),
        "timestamp": random_dates(start_date, end_date, num_rows)
    }

    return pd.DataFrame(data)

if __name__ == "__main__":
    customers = generate_customers(N_CUSTOMERS)
    transactions = generate_transactions(N_TRANSACTIONS, N_CUSTOMERS)

    # Create path
    path = Path('/tmp/sources')
    path.mkdir(parents=True, exist_ok=True)

    # Save customers to CSV
    customers.to_csv(CUSTOMERS_PATH, index=False)

    # Save transactions to SQLite database
    with sqlite3.connect(TRANSACTIONS_PATH) as conn:
        transactions.to_sql('transactions', conn, if_exists='replace', index=False)
