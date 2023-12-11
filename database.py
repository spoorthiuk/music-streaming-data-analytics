import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

def connect():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=DATABASE,
            user=USER,
            password=PASSWORD)
        cursor = conn.cursor()
        cursor.execute('SELECT version()')
        db_version = cursor.fetchone()
        print(db_version)
        return(conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_table(cursor, tablename, columns):
    for col in columns:
        query = f"CREATE TABLE {tablename} ({col})"
        cursor.execute(query)

def display_table(cursor, table_name, columns):
    for col in columns:
        query = f"SELECT {col} FROM {table_name}"
        print(query)
        cursor.execute(query)
        reply = cursor.fetchone()
        print(reply)

def insert_table():
    query = "INSERT INTO Test (test1) VALUES (1);"
    cursor.execute(query)

conn = psycopg2.connect(
            host="localhost",
            database=DATABASE,
            user=USER,
            password=PASSWORD)
cursor = conn.cursor()
create_table(cursor, 'Test', ['test1 int'])
display_table(cursor, 'Test', ['*'])
insert_table()
display_table(cursor, 'Test', ['*'])