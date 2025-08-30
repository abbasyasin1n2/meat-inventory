from .user_queries import execute_query
from .connection import DB_TYPE


def add_outbound_shipment(shipment_number, destination_name, destination_type, scheduled_date, created_by, status='planned', notes=None):
    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    query = f'''
        INSERT INTO outbound_shipments (shipment_number, destination_name, destination_type, scheduled_date, status, notes, created_by)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
    '''
    return execute_query(query, (shipment_number, destination_name, destination_type, scheduled_date, status, notes, created_by))


def get_all_shipments(status=None):
    if status:
        query = (
            'SELECT * FROM outbound_shipments WHERE status = %s ORDER BY scheduled_date DESC'
            if DB_TYPE == 'mysql'
            else 'SELECT * FROM outbound_shipments WHERE status = ? ORDER BY scheduled_date DESC'
        )
        return execute_query(query, (status,), fetch_all=True)
    return execute_query('SELECT * FROM outbound_shipments ORDER BY scheduled_date DESC', fetch_all=True)


def get_shipment_by_id(shipment_id):
    query = 'SELECT * FROM outbound_shipments WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM outbound_shipments WHERE id = ?'
    return execute_query(query, (shipment_id,), fetch_one=True)


def update_shipment_status(shipment_id, status, notes=None):
    if DB_TYPE == 'mysql':
        query = 'UPDATE outbound_shipments SET status = %s, notes = COALESCE(%s, notes), updated_at = CURRENT_TIMESTAMP WHERE id = %s'
    else:
        query = 'UPDATE outbound_shipments SET status = ?, notes = COALESCE(?, notes), updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    return execute_query(query, (status, notes, shipment_id))


def add_shipment_line(shipment_id, batch_id, quantity_shipped, picked_strategy='FIFO'):
    query = (
        'INSERT INTO shipment_lines (shipment_id, batch_id, quantity_shipped, picked_strategy) VALUES (%s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO shipment_lines (shipment_id, batch_id, quantity_shipped, picked_strategy) VALUES (?, ?, ?, ?)'
    )
    return execute_query(query, (shipment_id, batch_id, quantity_shipped, picked_strategy))


def get_shipment_lines(shipment_id):
    query = '''
        SELECT sl.*, b.batch_number, b.expiration_date, b.arrival_date, p.name as product_name
        FROM shipment_lines sl
        JOIN inventory_batches b ON sl.batch_id = b.id
        JOIN products p ON b.product_id = p.id
        WHERE sl.shipment_id = %s
        ORDER BY sl.id
    ''' if DB_TYPE == 'mysql' else '''
        SELECT sl.*, b.batch_number, b.expiration_date, b.arrival_date, p.name as product_name
        FROM shipment_lines sl
        JOIN inventory_batches b ON sl.batch_id = b.id
        JOIN products p ON b.product_id = p.id
        WHERE sl.shipment_id = ?
        ORDER BY sl.id
    '''
    return execute_query(query, (shipment_id,), fetch_all=True)

def get_shipment_line_by_id(line_id):
    query = (
        'SELECT sl.*, b.batch_number, b.product_id FROM shipment_lines sl JOIN inventory_batches b ON sl.batch_id=b.id WHERE sl.id = %s'
        if DB_TYPE == 'mysql' else
        'SELECT sl.*, b.batch_number, b.product_id FROM shipment_lines sl JOIN inventory_batches b ON sl.batch_id=b.id WHERE sl.id = ?'
    )
    return execute_query(query, (line_id,), fetch_one=True)

def update_shipment_line_quantity(line_id, new_qty):
    query = 'UPDATE shipment_lines SET quantity_shipped = %s WHERE id = %s' if DB_TYPE == 'mysql' else 'UPDATE shipment_lines SET quantity_shipped = ? WHERE id = ?'
    return execute_query(query, (new_qty, line_id))

def delete_shipment_line(line_id):
    query = 'DELETE FROM shipment_lines WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM shipment_lines WHERE id = ?'
    return execute_query(query, (line_id,))


def delete_shipment_lines(shipment_id):
    """Delete all lines for a shipment (used when cancelling and reversing stock)."""
    query = 'DELETE FROM shipment_lines WHERE shipment_id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM shipment_lines WHERE shipment_id = ?'
    return execute_query(query, (shipment_id,))

def delete_outbound_shipment(shipment_id):
    """Delete shipment header (lines are removed by FK cascade or must be removed beforehand)."""
    query = 'DELETE FROM outbound_shipments WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM outbound_shipments WHERE id = ?'
    return execute_query(query, (shipment_id,))

def record_restorations(shipment_id, allocations):
    """Persist restored allocations so they can be reapplied if status switches back to planned."""
    if not allocations:
        return 0
    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    values = []
    for a in allocations:
        values.append((shipment_id, a['batch_id'], a['quantity']))
    if DB_TYPE == 'mysql':
        query = f"INSERT INTO shipment_restorations (shipment_id, batch_id, quantity) VALUES ({placeholder}, {placeholder}, {placeholder})"
        count = 0
        for v in values:
            res = execute_query(query, v)
            count += 1 if res is not None else 0
        return count
    else:
        # For SQLite, executemany via loop using execute_query (which commits per call)
        count = 0
        query = f"INSERT INTO shipment_restorations (shipment_id, batch_id, quantity) VALUES ({placeholder}, {placeholder}, {placeholder})"
        for v in values:
            res = execute_query(query, v)
            count += 1 if res is not None else 0
        return count

def get_restorations(shipment_id):
    query = 'SELECT batch_id, quantity FROM shipment_restorations WHERE shipment_id = %s' if DB_TYPE == 'mysql' else 'SELECT batch_id, quantity FROM shipment_restorations WHERE shipment_id = ?'
    return execute_query(query, (shipment_id,), fetch_all=True)

def clear_restorations(shipment_id):
    query = 'DELETE FROM shipment_restorations WHERE shipment_id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM shipment_restorations WHERE shipment_id = ?'
    return execute_query(query, (shipment_id,))


def get_current_stock_by_product():
    query = '''
        SELECT p.id as product_id, p.name, SUM(b.quantity) as total_qty
        FROM products p
        LEFT JOIN inventory_batches b ON b.product_id = p.id
        GROUP BY p.id, p.name
        ORDER BY p.name
    '''
    return execute_query(query, fetch_all=True)


def get_product_current_stock(product_id):
    """Get current total stock for a specific product"""
    query = '''
        SELECT COALESCE(SUM(quantity), 0) as current_stock
        FROM inventory_batches
        WHERE product_id = %s
    ''' if DB_TYPE == 'mysql' else '''
        SELECT COALESCE(SUM(quantity), 0) as current_stock
        FROM inventory_batches
        WHERE product_id = ?
    '''
    result = execute_query(query, (product_id,), fetch_one=True)
    return float(result.get('current_stock', 0)) if result else 0.0


def get_restock_suggestions():
    query = '''
        SELECT rr.id as rule_id, p.id as product_id, p.name,
               COALESCE(SUM(b.quantity), 0) as current_qty,
               rr.min_qty, rr.target_qty,
               CASE WHEN COALESCE(SUM(b.quantity), 0) < rr.min_qty
                    THEN (rr.target_qty - COALESCE(SUM(b.quantity), 0))
                    ELSE 0 END as suggested_restock
        FROM reorder_rules rr
        JOIN products p ON rr.product_id = p.id
        LEFT JOIN inventory_batches b ON b.product_id = p.id
        WHERE rr.active = %s
        GROUP BY rr.id, p.id, p.name, rr.min_qty, rr.target_qty
        HAVING COALESCE(SUM(b.quantity), 0) < rr.min_qty
        ORDER BY suggested_restock DESC
    ''' if DB_TYPE == 'mysql' else '''
        SELECT rr.id as rule_id, p.id as product_id, p.name,
               COALESCE(SUM(b.quantity), 0) as current_qty,
               rr.min_qty, rr.target_qty,
               CASE WHEN COALESCE(SUM(b.quantity), 0) < rr.min_qty
                    THEN (rr.target_qty - COALESCE(SUM(b.quantity), 0))
                    ELSE 0 END as suggested_restock
        FROM reorder_rules rr
        JOIN products p ON rr.product_id = p.id
        LEFT JOIN inventory_batches b ON b.product_id = p.id
        WHERE rr.active = 1
        GROUP BY rr.id, p.id, p.name, rr.min_qty, rr.target_qty
        HAVING COALESCE(SUM(b.quantity), 0) < rr.min_qty
        ORDER BY suggested_restock DESC
    '''
    params = (1,) if DB_TYPE == 'mysql' else ()
    return execute_query(query, params, fetch_all=True)


def get_picklist(product_id, required_qty, strategy='FIFO'):
    if strategy == 'FEFO':
        order_clause = " (expiration_date IS NULL), expiration_date ASC "
    else:
        order_clause = " arrival_date ASC "

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    query = f'''
        SELECT id, quantity, batch_number, arrival_date, expiration_date
        FROM inventory_batches
        WHERE product_id = {placeholder} AND quantity > 0
        ORDER BY {order_clause}
    '''
    batches = execute_query(query, (product_id,), fetch_all=True)
    try:
        remaining = float(required_qty)
    except Exception:
        remaining = 0.0
    allocations = []
    for b in (batches or []):
        if remaining <= 0:
            break
        available = float(b['quantity']) if b.get('quantity') is not None else 0.0
        take = min(available, remaining)
        if take > 0:
            allocations.append({
                'batch_id': b['id'],
                'batch_number': b['batch_number'],
                'expiration_date': b.get('expiration_date'),
                'arrival_date': b.get('arrival_date'),
                'quantity': take
            })
            remaining -= take
    return {'allocations': allocations, 'remaining': remaining}



# Reorder Rules Management
def get_reorder_rules(search_text=None):
    """List reorder rules joined with product names, optional search by product name."""
    if search_text:
        placeholder = '%s' if DB_TYPE == 'mysql' else '?'
        pattern = f"%{search_text}%"
        query = f'''
            SELECT rr.*, p.name AS product_name
            FROM reorder_rules rr
            JOIN products p ON rr.product_id = p.id
            WHERE LOWER(p.name) LIKE LOWER({placeholder})
            ORDER BY p.name
        '''
        return execute_query(query, (pattern,), fetch_all=True)
    query = '''
        SELECT rr.*, p.name AS product_name
        FROM reorder_rules rr
        JOIN products p ON rr.product_id = p.id
        ORDER BY p.name
    '''
    return execute_query(query, fetch_all=True)


def get_reorder_rule_by_id(rule_id):
    query = (
        'SELECT rr.*, p.name AS product_name FROM reorder_rules rr JOIN products p ON rr.product_id=p.id WHERE rr.id = %s'
        if DB_TYPE == 'mysql' else
        'SELECT rr.*, p.name AS product_name FROM reorder_rules rr JOIN products p ON rr.product_id=p.id WHERE rr.id = ?'
    )
    return execute_query(query, (rule_id,), fetch_one=True)


def add_reorder_rule(product_id, min_qty, target_qty, active=True):
    query = (
        'INSERT INTO reorder_rules (product_id, min_qty, target_qty, active) VALUES (%s, %s, %s, %s)'
        if DB_TYPE == 'mysql' else
        'INSERT INTO reorder_rules (product_id, min_qty, target_qty, active) VALUES (?, ?, ?, ?)'
    )
    active_val = 1 if active else 0
    return execute_query(query, (product_id, float(min_qty), float(target_qty), active_val))


def update_reorder_rule(rule_id, product_id=None, min_qty=None, target_qty=None, active=None):
    updates = []
    params = []
    if product_id is not None:
        updates.append('product_id = %s' if DB_TYPE == 'mysql' else 'product_id = ?')
        params.append(product_id)
    if min_qty is not None:
        updates.append('min_qty = %s' if DB_TYPE == 'mysql' else 'min_qty = ?')
        params.append(float(min_qty))
    if target_qty is not None:
        updates.append('target_qty = %s' if DB_TYPE == 'mysql' else 'target_qty = ?')
        params.append(float(target_qty))
    if active is not None:
        updates.append('active = %s' if DB_TYPE == 'mysql' else 'active = ?')
        params.append(1 if active else 0)
    if not updates:
        return True
    updates.append('updated_at = CURRENT_TIMESTAMP')
    params.append(rule_id)
    query = f"UPDATE reorder_rules SET {', '.join(updates)} WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}"
    return execute_query(query, tuple(params))


def delete_reorder_rule(rule_id):
    query = 'DELETE FROM reorder_rules WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM reorder_rules WHERE id = ?'
    return execute_query(query, (rule_id,))

