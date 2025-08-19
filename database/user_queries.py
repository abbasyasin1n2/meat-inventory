
from .connection import get_db_connection, DB_TYPE

def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()

        if DB_TYPE == 'mysql':
            cursor.execute(query, params or ())

            if fetch_one:
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    result = dict(zip(columns, result))
            elif fetch_all:
                results = cursor.fetchall()
                if results:
                    columns = [desc[0] for desc in cursor.description]
                    result = [dict(zip(columns, row)) for row in results]
                else:
                    result = []
            else:
                result = cursor.rowcount
        else:
            cursor = conn.execute(query, params or ())

            if fetch_one:
                result = cursor.fetchone()
            elif fetch_all:
                result = cursor.fetchall()
            else:
                result = cursor.rowcount

        conn.commit()
        return result

    except Exception as e:
        conn.rollback()
        print(f"Database error: {e}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_user_by_id(user_id):
    """Get user by ID"""
    return execute_query(
        'SELECT * FROM users WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM users WHERE id = ?',
        (user_id,),
        fetch_one=True
    )

def get_user_by_username(username):
    """Get user by username"""
    return execute_query(
        'SELECT * FROM users WHERE username = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM users WHERE username = ?',
        (username,),
        fetch_one=True
    )

def create_user(username, email, password_hash):
    """Create a new user"""
    return execute_query(
        'INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)' if DB_TYPE == 'mysql'
        else 'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
        (username, email, password_hash)
    )

def get_user_stats():
    """Get basic application statistics"""
    total_users = execute_query('SELECT COUNT(*) as count FROM users', fetch_one=True)
    total_products = execute_query('SELECT COUNT(*) as count FROM products', fetch_one=True)
    total_suppliers = execute_query('SELECT COUNT(*) as count FROM suppliers', fetch_one=True)

    return {
        'total_users': total_users['count'] if total_users else 0,
        'total_products': total_products['count'] if total_products else 0,
        'total_suppliers': total_suppliers['count'] if total_suppliers else 0,
    }
