
from .user_queries import execute_query
from .connection import DB_TYPE

def add_product(name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id):
    """Add a new product"""
    query = (
        'INSERT INTO products (name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO products (name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    )
    params = (name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id)
    return execute_query(query, params)

def get_all_products():
    """Get all products with supplier information"""
    query = '''
        SELECT p.*, s.name as supplier_name
        FROM products p
        LEFT JOIN suppliers s ON p.supplier_id = s.id
        ORDER BY p.name
    '''
    return execute_query(query, fetch_all=True)

def get_product_by_id(product_id):
    """Get a single product by ID"""
    query = 'SELECT * FROM products WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM products WHERE id = ?'
    return execute_query(query, (product_id,), fetch_one=True)

def update_product(product_id, name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id):
    """Update an existing product"""
    query = (
        'UPDATE products SET name = %s, animal_type = %s, cut_type = %s, processing_date = %s, storage_requirements = %s, shelf_life = %s, packaging_details = %s, supplier_id = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'UPDATE products SET name = ?, animal_type = ?, cut_type = ?, processing_date = ?, storage_requirements = ?, shelf_life = ?, packaging_details = ?, supplier_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    )
    params = (name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id, product_id)
    return execute_query(query, params)

def delete_product(product_id):
    """Delete a product"""
    query = 'DELETE FROM products WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM products WHERE id = ?'
    return execute_query(query, (product_id,))

def get_product_counts_by_animal_type():
    """Get the count of products for each animal type"""
    query = """
        SELECT animal_type, COUNT(id) as product_count
        FROM products
        GROUP BY animal_type
        ORDER BY product_count DESC
    """
    return execute_query(query, fetch_all=True)


def search_products(search_text):
    """Search products by name, animal type, cut type, storage requirements, packaging, or supplier name."""
    if not search_text:
        return get_all_products()

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    query = f'''
        SELECT p.*, s.name as supplier_name
        FROM products p
        LEFT JOIN suppliers s ON p.supplier_id = s.id
        WHERE LOWER(p.name) LIKE LOWER({placeholder})
           OR LOWER(p.animal_type) LIKE LOWER({placeholder})
           OR LOWER(p.cut_type) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(p.storage_requirements, '')) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(p.packaging_details, '')) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(s.name, '')) LIKE LOWER({placeholder})
        ORDER BY p.name
    '''
    params = (pattern, pattern, pattern, pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)
