from .user_queries import execute_query
from .connection import DB_TYPE

# --- Internal helpers to gracefully support both legacy text column and new FK column ---
_STORAGE_FK_AVAILABLE: bool | None = None

def _has_column(table_name: str, column_name: str) -> bool:
    """Return True if the given column exists. Works for both MySQL and SQLite."""
    try:
        if DB_TYPE == 'mysql':
            query = (
                "SELECT COUNT(*) AS cnt FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s"
            )
            row = execute_query(query, (table_name, column_name), fetch_one=True)
            return bool(row and (row.get('cnt') or list(row.values())[0]))
        else:
            query = f"PRAGMA table_info({table_name})"
            rows = execute_query(query, fetch_all=True)
            if not rows:
                return False
            # SQLite returns list of columns; name field is 'name'
            return any((r.get('name') == column_name) for r in rows)
    except Exception:
        return False

def _storage_fk_available() -> bool:
    global _STORAGE_FK_AVAILABLE
    if _STORAGE_FK_AVAILABLE is None:
        _STORAGE_FK_AVAILABLE = _has_column('inventory_batches', 'storage_location_id')
    return _STORAGE_FK_AVAILABLE

def add_batch(product_id, batch_number, quantity, arrival_date, expiration_date, storage_location):
    """Add a new inventory batch.

    storage_location is the select value from the UI. Historically it stored the text in
    `storage_location` (VARCHAR). Newer schema may have `storage_location_id` (INT FK).
    We support both: write to FK if available, else to the legacy text column.
    """
    if _storage_fk_available():
        # Prefer writing the FK; also backfill legacy text with the same value when feasible
        if DB_TYPE == 'mysql':
            query = (
                'INSERT INTO inventory_batches (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location_id, storage_location) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s)'
            )
        else:
            query = (
                'INSERT INTO inventory_batches (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location_id, storage_location) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)'
            )
        params = (
            product_id, batch_number, quantity, arrival_date, expiration_date,
            storage_location,  # this is the chosen id from the form
            str(storage_location),  # keep legacy text populated for backward compatibility
        )
        return execute_query(query, params)
    else:
        query = (
            'INSERT INTO inventory_batches (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location) VALUES (%s, %s, %s, %s, %s, %s)'
            if DB_TYPE == 'mysql'
            else 'INSERT INTO inventory_batches (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location) VALUES (?, ?, ?, ?, ?, ?)'
        )
        params = (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location)
        return execute_query(query, params)

def get_all_batches():
    """Get all inventory batches with product and storage location information"""
    if _storage_fk_available():
        query = '''
            SELECT b.*, p.name as product_name, sl.name as storage_name, sl.location_type as storage_type
            FROM inventory_batches b
            JOIN products p ON b.product_id = p.id
            LEFT JOIN storage_locations sl ON (
                sl.id = b.storage_location_id OR (b.storage_location_id IS NULL AND sl.id = b.storage_location)
            )
            ORDER BY b.arrival_date DESC
        '''
    else:
        # Legacy schema: try to match numeric text IDs or names
        query = '''
            SELECT b.*, p.name as product_name, sl.name as storage_name, sl.location_type as storage_type
            FROM inventory_batches b
            JOIN products p ON b.product_id = p.id
            LEFT JOIN storage_locations sl ON (sl.id = b.storage_location OR sl.name = b.storage_location)
            ORDER BY b.arrival_date DESC
        '''
    return execute_query(query, fetch_all=True)

def get_batch_by_id(batch_id):
    """Get a single inventory batch by ID with storage information"""
    if _storage_fk_available():
        query = '''
            SELECT b.*, sl.name as storage_name, sl.location_type as storage_type, sl.capacity as storage_capacity
            FROM inventory_batches b
            LEFT JOIN storage_locations sl ON (
                sl.id = b.storage_location_id OR (b.storage_location_id IS NULL AND sl.id = b.storage_location)
            )
            WHERE b.id = %s
        ''' if DB_TYPE == 'mysql' else '''
            SELECT b.*, sl.name as storage_name, sl.location_type as storage_type, sl.capacity as storage_capacity
            FROM inventory_batches b
            LEFT JOIN storage_locations sl ON (
                sl.id = b.storage_location_id OR (b.storage_location_id IS NULL AND sl.id = b.storage_location)
            )
            WHERE b.id = ?
        '''
    else:
        query = '''
            SELECT b.*, sl.name as storage_name, sl.location_type as storage_type, sl.capacity as storage_capacity
            FROM inventory_batches b
            LEFT JOIN storage_locations sl ON (sl.id = b.storage_location OR sl.name = b.storage_location)
            WHERE b.id = %s
        ''' if DB_TYPE == 'mysql' else '''
            SELECT b.*, sl.name as storage_name, sl.location_type as storage_type, sl.capacity as storage_capacity
            FROM inventory_batches b
            LEFT JOIN storage_locations sl ON (sl.id = b.storage_location OR sl.name = b.storage_location)
            WHERE b.id = ?
        '''
    return execute_query(query, (batch_id,), fetch_one=True)

def update_batch(batch_id, product_id, batch_number, quantity, arrival_date, expiration_date, storage_location):
    """Update an existing inventory batch. Writes to FK if available, else legacy text."""
    if _storage_fk_available():
        query = (
            'UPDATE inventory_batches SET product_id = %s, batch_number = %s, quantity = %s, arrival_date = %s, expiration_date = %s, storage_location_id = %s, storage_location = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
            if DB_TYPE == 'mysql'
            else 'UPDATE inventory_batches SET product_id = ?, batch_number = ?, quantity = ?, arrival_date = ?, expiration_date = ?, storage_location_id = ?, storage_location = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
        )
        params = (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location, str(storage_location), batch_id)
        return execute_query(query, params)
    else:
        query = (
            'UPDATE inventory_batches SET product_id = %s, batch_number = %s, quantity = %s, arrival_date = %s, expiration_date = %s, storage_location = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
            if DB_TYPE == 'mysql'
            else 'UPDATE inventory_batches SET product_id = ?, batch_number = ?, quantity = ?, arrival_date = ?, expiration_date = ?, storage_location = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
        )
        params = (product_id, batch_number, quantity, arrival_date, expiration_date, storage_location, batch_id)
        return execute_query(query, params)

def update_batch_quantity(batch_id, quantity_change):
    """Update the quantity of an inventory batch
    
    Args:
        batch_id: ID of the batch to update
        quantity_change: Positive value to increase, negative value to decrease
    """
    query = (
        'UPDATE inventory_batches SET quantity = quantity + %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'UPDATE inventory_batches SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    )
    params = (quantity_change, batch_id)
    return execute_query(query, params)

def delete_batch(batch_id):
    """Delete an inventory batch - checks for dependencies first"""
    # Check if batch is used in processing inputs
    input_check = 'SELECT COUNT(*) as count FROM processing_inputs WHERE batch_id = %s' if DB_TYPE == 'mysql' else 'SELECT COUNT(*) as count FROM processing_inputs WHERE batch_id = ?'
    input_result = execute_query(input_check, (batch_id,), fetch_one=True)
    
    if input_result and input_result.get('count', 0) > 0:
        raise Exception(f"Cannot delete batch: Used in {input_result['count']} processing session(s). Delete processing records first.")
    
    # Check if batch is in shipment lines
    shipment_check = 'SELECT COUNT(*) as count FROM shipment_lines WHERE batch_id = %s' if DB_TYPE == 'mysql' else 'SELECT COUNT(*) as count FROM shipment_lines WHERE batch_id = ?'
    shipment_result = execute_query(shipment_check, (batch_id,), fetch_one=True)
    
    if shipment_result and shipment_result.get('count', 0) > 0:
        raise Exception(f"Cannot delete batch: Used in {shipment_result['count']} shipment(s). Remove from shipments first.")
    
    # Check if batch is in compliance incidents
    incident_check = 'SELECT COUNT(*) as count FROM incident_batches WHERE batch_id = %s' if DB_TYPE == 'mysql' else 'SELECT COUNT(*) as count FROM incident_batches WHERE batch_id = ?'
    incident_result = execute_query(incident_check, (batch_id,), fetch_one=True)
    
    if incident_result and incident_result.get('count', 0) > 0:
        raise Exception(f"Cannot delete batch: Involved in {incident_result['count']} compliance incident(s). Resolve incidents first.")
    
    # Check if batch is in ACTIVE recalls only (ignore cancelled/completed)
    recall_check = '''SELECT br.recall_number, br.status, br.title
                      FROM recall_batches rb 
                      JOIN batch_recalls br ON rb.recall_id = br.id 
                      WHERE rb.batch_id = %s AND br.status NOT IN ('cancelled', 'completed')''' if DB_TYPE == 'mysql' else '''SELECT br.recall_number, br.status, br.title
                      FROM recall_batches rb 
                      JOIN batch_recalls br ON rb.recall_id = br.id 
                      WHERE rb.batch_id = ? AND br.status NOT IN ('cancelled', 'completed')'''
    recall_results = execute_query(recall_check, (batch_id,), fetch_all=True)
    
    if recall_results and len(recall_results) > 0:
        recall_info = []
        for recall in recall_results:
            recall_info.append(f"{recall['recall_number']} ({recall['status']})")
        raise Exception(f"Cannot delete batch: Referenced in active recall(s): {', '.join(recall_info)}. Go to Compliance → Recalls to manage these recalls first.")
    
    # Clean up cancelled/completed recall references before deletion
    cleanup_query = '''DELETE FROM recall_batches 
                       WHERE batch_id = %s AND recall_id IN (
                           SELECT id FROM batch_recalls 
                           WHERE status IN ('cancelled', 'completed')
                       )''' if DB_TYPE == 'mysql' else '''DELETE FROM recall_batches 
                       WHERE batch_id = ? AND recall_id IN (
                           SELECT id FROM batch_recalls 
                           WHERE status IN ('cancelled', 'completed')
                       )'''
    execute_query(cleanup_query, (batch_id,))
    
    # If no dependencies, delete the batch
    query = 'DELETE FROM inventory_batches WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM inventory_batches WHERE id = ?'
    result = execute_query(query, (batch_id,))
    
    # Debug: Check if the batch was actually deleted
    check_query = 'SELECT COUNT(*) as count FROM inventory_batches WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT COUNT(*) as count FROM inventory_batches WHERE id = ?'
    check_result = execute_query(check_query, (batch_id,), fetch_one=True)
    
    if check_result and check_result.get('count', 0) > 0:
        raise Exception(f"Batch deletion failed: Batch {batch_id} still exists in database after delete attempt.")
    
    return result

def get_expired_batches():
    """Get all batches that have passed their expiration date"""
    query = """
        SELECT b.*, p.name as product_name
        FROM inventory_batches b
        JOIN products p ON b.product_id = p.id
        WHERE b.expiration_date < CURDATE() AND b.quantity > 0
        ORDER BY b.expiration_date ASC
    """ if DB_TYPE == 'mysql' else """
        SELECT b.*, p.name as product_name
        FROM inventory_batches b
        JOIN products p ON b.product_id = p.id
        WHERE b.expiration_date < DATE('now') AND b.quantity > 0
        ORDER BY b.expiration_date ASC
    """
    return execute_query(query, fetch_all=True)

def get_soon_to_expire_batches(days=7):
    """Get all batches that will expire within a certain number of days"""
    query = """
        SELECT b.*, p.name as product_name
        FROM inventory_batches b
        JOIN products p ON b.product_id = p.id
        WHERE b.expiration_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL %s DAY) AND b.quantity > 0
        ORDER BY b.expiration_date ASC
    """ if DB_TYPE == 'mysql' else """
        SELECT b.*, p.name as product_name
        FROM inventory_batches b
        JOIN products p ON b.product_id = p.id
        WHERE b.expiration_date BETWEEN DATE('now') AND DATE('now', '+{} days') AND b.quantity > 0
        ORDER BY b.expiration_date ASC
    """.format(days)
    params = (days,) if DB_TYPE == 'mysql' else ()
    return execute_query(query, params, fetch_all=True)

def get_inventory_over_time():
    """Get the sum of inventory quantities grouped by arrival date"""
    query = """
        SELECT arrival_date, SUM(quantity) as total_quantity
        FROM inventory_batches
        GROUP BY arrival_date
        ORDER BY arrival_date ASC
    """
    return execute_query(query, fetch_all=True)


def get_batch_compliance_status(batch_id):
    """Get comprehensive compliance status for a batch"""
    from datetime import datetime, timedelta
    
    # Get batch details
    batch = get_batch_by_id(batch_id)
    if not batch:
        return {'status': 'unknown', 'issues': ['Batch not found']}
    
    issues = []
    status = 'normal'
    
    # Check expiration status
    if batch.get('expiration_date'):
        exp_date = batch['expiration_date']
        if isinstance(exp_date, str):
            exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
        
        today = datetime.now().date()
        days_to_expire = (exp_date - today).days
        
        if days_to_expire < 0:
            status = 'critical'
            issues.append(f'Expired {abs(days_to_expire)} days ago')
        elif days_to_expire <= 7:
            status = 'warning' if status == 'normal' else status
            issues.append(f'Expires in {days_to_expire} days')
    
    # Check for recalls
    try:
        from database.recall_queries import get_batch_recall_history
        recall_history = get_batch_recall_history(batch_id)
        if recall_history:
            active_recalls = [r for r in recall_history if r.get('status') in ['initiated', 'in_progress']]
            if active_recalls:
                status = 'critical'
                issues.append(f'{len(active_recalls)} active recall(s)')
    except ImportError:
        pass  # Recall functionality not available
    
    # Check storage alerts (if storage location is available)
    if batch.get('storage_location'):
        try:
            from database.storage_queries import get_alert_readings
            alerts = get_alert_readings()
            storage_alerts = [a for a in alerts if str(a.get('storage_id')) == str(batch['storage_location'])]
            if storage_alerts:
                status = 'warning' if status == 'normal' else status
                issues.append(f'{len(storage_alerts)} storage alert(s)')
        except ImportError:
            pass  # Storage functionality not available
    
    # Check for food safety incidents
    incidents = []
    try:
        from database.compliance_queries import get_incident_batches_by_batch
        # Get all incidents this batch is involved in
        incident_batches = get_incident_batches_by_batch(batch_id)
        if incident_batches:
            active_incidents = [ib for ib in incident_batches if ib.get('incident_status') in ['open', 'investigating']]
            if active_incidents:
                status = 'critical' if status == 'normal' else status
                issues.append(f'{len(active_incidents)} active food safety incident(s)')
            
            # Add all incidents to the response
            incidents = incident_batches
    except ImportError:
        pass  # Compliance functionality not available
    
    return {
        'status': status,
        'issues': issues,
        'batch_id': batch_id,
        'incidents': incidents
    }


def search_batches(search_text):
    """Search inventory batches by batch number, product name, or storage name."""
    if not search_text:
        return get_all_batches()

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    if _storage_fk_available():
        query = f'''
            SELECT b.*, p.name as product_name, sl.name as storage_name, sl.location_type as storage_type
            FROM inventory_batches b
            JOIN products p ON b.product_id = p.id
            LEFT JOIN storage_locations sl ON (
                sl.id = b.storage_location_id OR (b.storage_location_id IS NULL AND sl.id = b.storage_location)
            )
            WHERE LOWER(b.batch_number) LIKE LOWER({placeholder})
               OR LOWER(p.name) LIKE LOWER({placeholder})
               OR LOWER(COALESCE(sl.name, '')) LIKE LOWER({placeholder})
            ORDER BY b.arrival_date DESC
        '''
    else:
        query = f'''
            SELECT b.*, p.name as product_name, sl.name as storage_name, sl.location_type as storage_type
            FROM inventory_batches b
            JOIN products p ON b.product_id = p.id
            LEFT JOIN storage_locations sl ON (sl.id = b.storage_location OR sl.name = b.storage_location)
            WHERE LOWER(b.batch_number) LIKE LOWER({placeholder})
               OR LOWER(p.name) LIKE LOWER({placeholder})
               OR LOWER(COALESCE(sl.name, '')) LIKE LOWER({placeholder})
            ORDER BY b.arrival_date DESC
        '''
    params = (pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)
