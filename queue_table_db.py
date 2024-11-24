import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
PASSWORD = os.environ["PASSWORD"]
DBNAME = "neobanking"
USER = "postgres"

def setup_api_queue():
    """
    Sets up the API queue table, trigger, and function for transaction notifications.
    The queue table will have the exact same structure as the transactions table.
    """
    conn = None
    cursor = None
    
    try:
        # Establish connection
        conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
        cursor = conn.cursor()
        
        # Create queue table with same structure as transactions
        create_queue_table = """
        CREATE TABLE IF NOT EXISTS api_queue (LIKE transactions INCLUDING ALL);
        """
        
        # Create trigger function
        create_trigger_function = """
        CREATE OR REPLACE FUNCTION update_api_queue()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO api_queue SELECT NEW.*;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        # Create trigger
        create_trigger = """
        DROP TRIGGER IF EXISTS insert_api_queue ON transactions;
        CREATE TRIGGER insert_api_queue
        AFTER INSERT ON transactions
        FOR EACH ROW
        EXECUTE FUNCTION update_api_queue();
        """
        
        # Execute all commands
        print("Creating API queue table...")
        cursor.execute(create_queue_table)
        
        print("Creating trigger function...")
        cursor.execute(create_trigger_function)
        
        print("Setting up trigger...")
        cursor.execute(create_trigger)
        
        # Commit all changes
        conn.commit()
        print("API queue system successfully set up!")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    setup_api_queue()