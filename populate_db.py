import psycopg2
import pandas as pd
import io
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
PASSWORD = os.environ["PASSWORD"]
DBNAME = "neobanking"
USER = "postgres"

def create_transactions_table():
    """Create the transactions table explicitly"""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS transactions;")
        
        # Create table with explicit column definitions
        create_table_query = """
        CREATE TABLE transactions (
            id SERIAL PRIMARY KEY,
            time_stamp FLOAT,
            v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT,
            v6 FLOAT, v7 FLOAT, v8 FLOAT, v9 FLOAT, v10 FLOAT,
            v11 FLOAT, v12 FLOAT, v13 FLOAT, v14 FLOAT, v15 FLOAT,
            v16 FLOAT, v17 FLOAT, v18 FLOAT, v19 FLOAT, v20 FLOAT,
            v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT, v25 FLOAT,
            v26 FLOAT, v27 FLOAT, v28 FLOAT,
            amount FLOAT,
            class INTEGER
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully!")
        
    except Exception as e:
        print(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def populate_transactions_table(csv_file="creditcard.csv", num_transactions=1000):
    """
    Populates the transactions table with a specified number of rows from CSV file.

    Parameters:
    -----------
    csv_file : str
        Path to the CSV file containing transaction data
    num_transactions : int
        Number of transactions to load from the CSV file
    """
    conn = None
    cursor = None
    
    try:
        # Read only the specified number of rows from CSV file
        df = pd.read_csv(csv_file, nrows=num_transactions)
        print(f"Read {len(df)} rows from CSV file")
        
        # Create database connection
        conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
        cursor = conn.cursor()
        
        # Convert DataFrame to CSV string
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        # Copy data from buffer to database
        cursor.copy_from(
            buffer,
            'transactions',
            sep=',',
            columns=[
                'time_stamp', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 
                'v9', 'v10', 'v11', 'v12', 'v13', 'v14', 'v15', 'v16', 'v17', 
                'v18', 'v19', 'v20', 'v21', 'v22', 'v23', 'v24', 'v25', 'v26', 
                'v27', 'v28', 'amount', 'class'
            ]
        )
        
        conn.commit()
        print(f"Successfully inserted {len(df)} rows into the transactions table!")

    except Exception as e:
        print(f"Error during data insertion: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if buffer:
            buffer.close()
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    # First create the table
    create_transactions_table()
    
    # Then populate it with a specified number of transactions
    populate_transactions_table(
        csv_file="creditcard.csv",
        num_transactions=1000  # Change this number to load more or fewer transactions
    )