
import os
import mysql.connector
from mysql.connector import Error
import sqlite3
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import Config

DB_TYPE = Config.DB_TYPE
DATABASE = Config.SQLITE_DATABASE

if DB_TYPE == 'mysql':
    DB_CONFIG = {
        'host': Config.DB_HOST,
        'port': Config.DB_PORT,
        'database': Config.DB_NAME,
        'user': Config.DB_USER,
        'password': Config.DB_PASSWORD,
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
else:
    DB_CONFIG = {}

def get_db_connection():
    """Get a database connection with appropriate configuration"""
    if DB_TYPE == 'mysql':
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except Error as e:
            print(f"MySQL connection error: {e}")
            return None
    else:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn
