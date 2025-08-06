#!/usr/bin/env python3

import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
ACCOUNT = os.getenv("ACCOUNT")
HOST = os.getenv("HOST") 
USER = os.getenv("DEMO_USER")
DATABASE = os.getenv("DEMO_DATABASE")
SCHEMA = os.getenv("DEMO_SCHEMA")
ROLE = os.getenv("DEMO_USER_ROLE")
WAREHOUSE = os.getenv("WAREHOUSE")
RSA_PRIVATE_KEY_PATH = os.getenv("RSA_PRIVATE_KEY_PATH")

print("Testing Snowflake connection with JWT authentication...")
print(f"Account: {ACCOUNT}")
print(f"Host: {HOST}")
print(f"User: {USER}")
print(f"Database: {DATABASE}")
print(f"Schema: {SCHEMA}")
print(f"Role: {ROLE}")
print(f"Warehouse: {WAREHOUSE}")
print(f"RSA Key Path: {RSA_PRIVATE_KEY_PATH}")
print()

try:
    print("Attempting to connect to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER,
        authenticator="SNOWFLAKE_JWT",
        private_key_file=RSA_PRIVATE_KEY_PATH,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        role=ROLE,
        host=HOST
    )
    
    print("✅ Successfully connected to Snowflake!")
    
    # Test a simple query
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    result = cursor.fetchone()
    print(f"Snowflake Version: {result[0]}")
    print(f"Current User: {result[1]}")
    print(f"Current Role: {result[2]}")
    print(f"Current Warehouse: {result[3]}")
    
    cursor.close()
    conn.close()
    print("Connection test completed successfully!")
    
except Exception as e:
    print(f"❌ Failed to connect to Snowflake: {e}")
    print("\nPossible issues:")
    print("1. RSA public key not registered with user in Snowflake")
    print("2. User doesn't exist or doesn't have proper permissions")
    print("3. Account identifier is incorrect")
    print("4. Warehouse/Role permissions issue")