
from .user_queries import execute_query
from .connection import DB_TYPE

def add_supplier(name, contact_person, phone, email, address):
    """Add a new supplier"""
    query = (
        'INSERT INTO suppliers (name, contact_person, phone, email, address) VALUES (%s, %s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO suppliers (name, contact_person, phone, email, address) VALUES (?, ?, ?, ?, ?)'
    )
    params = (name, contact_person, phone, email, address)
    return execute_query(query, params)

def get_all_suppliers():
    """Get all suppliers"""
    return execute_query('SELECT * FROM suppliers ORDER BY name', fetch_all=True)

def get_supplier_by_id(supplier_id):
    """Get a single supplier by ID"""
    query = 'SELECT * FROM suppliers WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM suppliers WHERE id = ?'
    return execute_query(query, (supplier_id,), fetch_one=True)

def update_supplier(supplier_id, name, contact_person, phone, email, address):
    """Update an existing supplier"""
    query = (
        'UPDATE suppliers SET name = %s, contact_person = %s, phone = %s, email = %s, address = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'UPDATE suppliers SET name = ?, contact_person = ?, phone = ?, email = ?, address = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    )
    params = (name, contact_person, phone, email, address, supplier_id)
    return execute_query(query, params)

def delete_supplier(supplier_id):
    """Delete a supplier"""
    query = 'DELETE FROM suppliers WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM suppliers WHERE id = ?'
    return execute_query(query, (supplier_id,))


def search_suppliers(search_text):
    """Search suppliers by name, contact person, phone, email, or address."""
    if not search_text:
        return get_all_suppliers()

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    query = f'''
        SELECT *
        FROM suppliers
        WHERE LOWER(name) LIKE LOWER({placeholder})
           OR LOWER(contact_person) LIKE LOWER({placeholder})
           OR LOWER(phone) LIKE LOWER({placeholder})
           OR LOWER(email) LIKE LOWER({placeholder})
           OR LOWER(address) LIKE LOWER({placeholder})
        ORDER BY name
    '''
    params = (pattern, pattern, pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)
