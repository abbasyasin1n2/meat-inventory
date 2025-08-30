
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
    try:
        # Try to get user with is_admin column
        return execute_query(
            'SELECT id, username, email, password_hash, COALESCE(is_admin, FALSE) as is_admin FROM users WHERE id = %s' if DB_TYPE == 'mysql' 
            else 'SELECT id, username, email, password_hash, COALESCE(is_admin, 0) as is_admin FROM users WHERE id = ?',
            (user_id,),
            fetch_one=True
        )
    except Exception as e:
        # Fallback: get user without is_admin column if it doesn't exist yet
        print(f"Fallback query for user {user_id}: {e}")
        user_data = execute_query(
            'SELECT id, username, email, password_hash FROM users WHERE id = %s' if DB_TYPE == 'mysql' 
            else 'SELECT id, username, email, password_hash FROM users WHERE id = ?',
            (user_id,),
            fetch_one=True
        )
        if user_data:
            # Add is_admin field manually (False by default)
            user_data['is_admin'] = False
        return user_data

def get_user_by_username(username):
    """Get user by username"""
    try:
        # Try to get user with is_admin column
        return execute_query(
            'SELECT id, username, email, password_hash, COALESCE(is_admin, FALSE) as is_admin FROM users WHERE username = %s' if DB_TYPE == 'mysql' 
            else 'SELECT id, username, email, password_hash, COALESCE(is_admin, 0) as is_admin FROM users WHERE username = ?',
            (username,),
            fetch_one=True
        )
    except Exception as e:
        # Fallback: get user without is_admin column if it doesn't exist yet
        print(f"Fallback query for username {username}: {e}")
        user_data = execute_query(
            'SELECT id, username, email, password_hash FROM users WHERE username = %s' if DB_TYPE == 'mysql' 
            else 'SELECT id, username, email, password_hash FROM users WHERE username = ?',
            (username,),
            fetch_one=True
        )
        if user_data:
            # Add is_admin field manually (False by default, True for abbasyasin)
            user_data['is_admin'] = (username == 'abbasyasin')
        return user_data

def create_user(username, email, password_hash):
    """Create a new user"""
    return execute_query(
        'INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)' if DB_TYPE == 'mysql'
        else 'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
        (username, email, password_hash)
    )

def migrate_add_admin_column():
    """Safely add is_admin column to users table if it doesn't exist"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if column already exists
        if DB_TYPE == 'mysql':
            cursor.execute("SHOW COLUMNS FROM users LIKE 'is_admin'")
            if cursor.fetchone():
                print("is_admin column already exists")
                return True
                
            # Add column
            cursor.execute("ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT FALSE")
            # Set abbasyasin as admin
            cursor.execute("UPDATE users SET is_admin = TRUE WHERE username = 'abbasyasin'")
        else:
            # SQLite - check if column exists
            cursor.execute("PRAGMA table_info(users)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'is_admin' in columns:
                print("is_admin column already exists")
                return True
                
            # Add column
            cursor.execute("ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT 0")
            # Set abbasyasin as admin
            cursor.execute("UPDATE users SET is_admin = 1 WHERE username = 'abbasyasin'")
        
        conn.commit()
        print("Successfully added is_admin column and set abbasyasin as admin")
        return True
        
    except Exception as e:
        print(f"Error adding is_admin column: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

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
