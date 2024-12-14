import psycopg2
import pandas as pd
from typing import Optional, Tuple
from psycopg2 import sql
from psycopg2.extras import execute_batch
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os
from airflow.hooks.base import BaseHook

load_dotenv()

def start_connection():

    try:
        connection = BaseHook.get_connection("aws-rds-aeon")  # Replace with your connection ID
        conn = psycopg2.connect(
            host=connection.host,
            database=connection.schema,
            user=connection.login,
            password=connection.password
        )

        # conn.set_session(autocommit=False)
        
        cursor = conn.cursor()
        print("Successfully connected to the aeon database.")
        return conn, cursor
    
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None


def close_connection(conn, cursor):

    try:
        if cursor and not cursor.closed:
            cursor.close()
            print("Cursor closed successfully.")
    except Exception as e:
        print(f"Error closing cursor: {e}")
    
    try:
        if conn and not conn.closed:
            conn.close()
            print("Database connection closed successfully.")
    except Exception as e:
        print(f"Error closing database connection: {e}")


def create_table(cursor):

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                tags TEXT[],
                date TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Articles table verified/created successfully.")
        return True
    
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        return False

def is_database_empty():

    conn, cursor = start_connection()
    if not create_table(cursor):
        return

    try:
        cursor.execute("SELECT COUNT(*) FROM articles")
        row_count = cursor.fetchone()[0]
        
        is_empty = row_count == 0
        
        print(f"Database articles table {'is' if is_empty else 'is not'} empty. Total rows: {row_count}")
        
        close_connection(conn, cursor)
        print("Database connection closed.")
        return is_empty, row_count
    
    except psycopg2.Error as e:
        print(f"Error checking database emptiness: {e}")
        return True, 0

def prepare_data(df):

    if df['tags'].dtype == object:
        df['tags'] = df['tags'].apply(lambda x: x.split(',') if isinstance(x, str) else x)
    
    filtered_df = df[
        (df['content'].str.lower() != 'content not found')
    ]
    
    removed_rows = len(df) - len(filtered_df)
    if removed_rows > 0:
        print(f"Removed {removed_rows} rows with 'content not found'")
    
    return [
        (row['url'], row['title'], row['content'], row['tags'], row['date']) 
        for _, row in filtered_df.iterrows()
    ]

def insert(df, cursor, conn, batch_size: int = 250):

    prepared_data = prepare_data(df)
    total_rows = len(prepared_data)
    successful_insertions = 0

    start_time = time.time()

    try:
        with tqdm(total=total_rows, desc="Inserting Articles", unit="row", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            
            for i in range(0, total_rows, batch_size):
                batch = prepared_data[i:i+batch_size]
                
                try:
                    execute_batch(
                        cursor,
                        sql.SQL("""
                            INSERT INTO articles (url, title, content, tags, date)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                        """),
                        batch,
                        page_size=batch_size
                    )
                    
                    batch_successful = len([row for row in batch if row])
                    successful_insertions += batch_successful
                    pbar.update(batch_successful)
                    
                    conn.commit()
                
                except psycopg2.Error as batch_error:
                    conn.rollback()
                    pbar.set_description(f"Error in batch {i//batch_size + 1}")
                    print(f"Batch insertion failed at index {i}: {batch_error}")

        end_time = time.time()
        total_time = end_time - start_time
        insertion_rate = successful_insertions / total_time if total_time > 0 else 0
        
        print(
            f"Batch insertion completed: "
            f"{successful_insertions}/{total_rows} total rows inserted | "
            f"Time taken: {total_time:.2f} seconds | "
            f"Insertion rate: {insertion_rate:.2f} rows/second"
        )
        return successful_insertions, total_rows
    
    except Exception as e:
        conn.rollback()
        print(f"Overall insertion process failed: {e}")
        return successful_insertions, total_rows
    

def load_data():

    try:
        df = pd.read_csv("scraped_data.csv")
        print(f"Loaded {len(df)} rows from scraped_data.csv")
    except FileNotFoundError:
        print("scraped_data.csv not found")
        return
    
    db_connection = start_connection()
    if not db_connection:
        return
    
    conn, cursor = db_connection
    
    try:
        if not create_table(cursor):
            return
        
        successful, total = insert(df, cursor, conn)
        print(f"Insertion summary: {successful}/{total} rows inserted successfully")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")


def url_exists_in_db(url, cursor):

    close_connection = False
    try:
        if cursor is None:
            conn_result = start_connection()
            if not conn_result:
                print("Failed to establish database connection")
                return False
            
            conn, cursor = conn_result
            close_connection = True
        
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM articles WHERE url = %s)", 
            (url,)
        )
        
        exists = cursor.fetchone()[0]
        print(f"URL check for {url}: {'Exists' if exists else 'Does not exist'}")
        
        return exists
    
    except psycopg2.Error as e:
        print(f"Error checking URL existence: {e}")
        return False
    
    finally:
        if close_connection:
            try:
                cursor.close()
                conn.close()
                print("Database connection closed.")
            except:
                pass


