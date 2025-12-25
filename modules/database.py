import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from the root .env file
load_dotenv()

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using the DATABASE_URL env var.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not found in environment variables.")
    return psycopg2.connect(url)

def execute_upsert(query, values):
    """
    Helper function to execute batch upserts.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        execute_values(cur, query, values)
        conn.commit()
        print("Database operation successful.")
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def fetch_all_symbols():
    """
    Retrieves all symbols currently stored in the database.
    Returns a list of symbol strings.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT "symbol" FROM "symbol"')
        rows = cur.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []
    finally:
        if conn:
            conn.close()
