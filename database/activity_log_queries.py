
from .connection import get_db_connection, DB_TYPE
from .user_queries import execute_query

def log_activity(user_id, action, description=None, ip_address=None):
    """Log user activity"""
    return execute_query(
        'INSERT INTO activity_log (user_id, action, description, ip_address) VALUES (%s, %s, %s, %s)' if DB_TYPE == 'mysql'
        else 'INSERT INTO activity_log (user_id, action, description, ip_address) VALUES (?, ?, ?, ?)',
        (user_id, action, description, ip_address)
    )

def get_recent_activity(user_id=None, limit=10):
    """Get recent activity logs"""
    placeholder = '%s' if DB_TYPE == 'mysql' else '?'

    if user_id:
        query = f'''
            SELECT al.*, u.username
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.user_id = {placeholder}
            ORDER BY al.created_at DESC
            LIMIT {placeholder}
        '''
        params = (user_id, limit)
    else:
        query = f'''
            SELECT al.*, u.username
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            ORDER BY al.created_at DESC
            LIMIT {placeholder}
        '''
        params = (limit,)

    return execute_query(query, params, fetch_all=True)
