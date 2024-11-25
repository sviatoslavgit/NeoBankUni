import psycopg2
import requests
import csv
import io
import os
import time
from dotenv import load_dotenv
from datetime import datetime
import json
# Load environment variables
load_dotenv()

# Database connection parameters
PASSWORD = os.environ["PASSWORD"]
DBNAME = "neobanking"
USER = "postgres"

def send_to_api(data):
    """
    Send transaction data to API endpoint
    """
    url = "http://127.0.0.1:5000/cross_validate"
    headers = {'Content-Type': 'application/json'}  # Correct header for JSON content
    
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")
    
    # Debugging step: Print headers to ensure they're being set correctly
    print(f"Headers: {headers}")
    
    try:
        # Send data as a JSON payload
        response = requests.post(url, headers=headers, data=json.dumps(data))  # Convert data to JSON string
        print(f"Response: {response.status_code} - {response.text}")
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return False

def process_queue():
    """
    Process transactions from the queue and send them to the API
    """
    try:
        conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
        cursor = conn.cursor()
        
        # Select transactions from the queue (no 'processed' check)
        cursor.execute("""
            SELECT id, time_stamp, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, 
                   V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, 
                   V26, V27, V28, Amount, Class
            LIMIT 100  -- Limit to 100 records to avoid overloading the system
        """)
        
        records = cursor.fetchall()
        
        if not records:
            print("Queue is empty.")
            cursor.close()
            conn.close()
            return

        all_successful = True

        for record in records:
            id, time_stamp, *features, amount, transaction_class = record
            
            # Convert timestamp to datetime
            transaction_date = datetime.fromtimestamp(time_stamp)
            
            # Prepare the data dictionary, including all the features
            data = {
                "Time": transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
                "V1": str(features[0]), "V2": str(features[1]), "V3": str(features[2]), "V4": str(features[3]),
                "V5": str(features[4]), "V6": str(features[5]), "V7": str(features[6]), "V8": str(features[7]),
                "V9": str(features[8]), "V10": str(features[9]), "V11": str(features[10]), "V12": str(features[11]),
                "V13": str(features[12]), "V14": str(features[13]), "V15": str(features[14]), "V16": str(features[15]),
                "V17": str(features[16]), "V18": str(features[17]), "V19": str(features[18]), "V20": str(features[19]),
                "V21": str(features[20]), "V22": str(features[21]), "V23": str(features[22]), "V24": str(features[23]),
                "V25": str(features[24]), "V26": str(features[25]), "V27": str(features[26]), "V28": str(features[27]),
                "Amount": str(amount),
                "Class": str(transaction_class)
            }
            
            # Print the parsed transaction data before sending it to the API
            print(f"Preparing to send transaction data: {data}")
            
            if not send_to_api(data):
                all_successful = False
                print(f"Error processing record ID: {id}")
                break
            
            # Delete transaction from the database after successful API request
            cursor.execute("""
                DELETE FROM transactions 
                WHERE id = %s
            """, (id,))
        
        if all_successful:
            conn.commit()
            print("Successfully processed all records in batch.")
        else:
            conn.rollback()
            print("Processing batch failed. Rolling back changes.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if 'conn' in locals() and conn is not None:
            conn.rollback()
            conn.close()

if __name__ == '__main__':
    # Main processing loop
    while True:
        try:
            process_queue()
        except Exception as e:
            print(f"Critical error occurred: {e}")
        time.sleep(5)  # 5 second delay between processing attempts
