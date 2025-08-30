"""
Batch Recall Database Queries

This module handles all database operations related to:
- Batch recall initiation and management
- Recall batch tracking
- Recall status updates
- Recall reporting and notifications

Integrates with existing traceability system to identify affected products.
"""

from .user_queries import execute_query
from .connection import DB_TYPE
from datetime import datetime


# Recall Management Operations
def add_batch_recall(recall_number, title, reason, severity_level, initiated_by, notes=None):
    """Initiate a new batch recall"""
    query = (
        '''INSERT INTO batch_recalls 
           (recall_number, title, reason, severity_level, initiated_by, notes) 
           VALUES (%s, %s, %s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO batch_recalls 
                (recall_number, title, reason, severity_level, initiated_by, notes) 
                VALUES (?, ?, ?, ?, ?, ?)'''
    )
    params = (recall_number, title, reason, severity_level, initiated_by, notes)
    result = execute_query(query, params)
    
    # Return the recall ID for linking batches
    if result:
        return get_recall_by_number(recall_number)
    return None


def get_all_batch_recalls(status=None):
    """Get all batch recalls, optionally filtered by status"""
    if status:
        query = (
            '''SELECT br.*, u.username as initiated_by_name,
                      COUNT(rb.id) as affected_batches_count
               FROM batch_recalls br
               LEFT JOIN users u ON br.initiated_by = u.id
               LEFT JOIN recall_batches rb ON br.id = rb.recall_id
               WHERE br.status = %s
               GROUP BY br.id
               ORDER BY br.initiated_date DESC'''
            if DB_TYPE == 'mysql'
            else '''SELECT br.*, u.username as initiated_by_name,
                           COUNT(rb.id) as affected_batches_count
                    FROM batch_recalls br
                    LEFT JOIN users u ON br.initiated_by = u.id
                    LEFT JOIN recall_batches rb ON br.id = rb.recall_id
                    WHERE br.status = ?
                    GROUP BY br.id
                    ORDER BY br.initiated_date DESC'''
        )
        params = (status,)
    else:
        query = '''SELECT br.*, u.username as initiated_by_name,
                          COUNT(rb.id) as affected_batches_count
                   FROM batch_recalls br
                   LEFT JOIN users u ON br.initiated_by = u.id
                   LEFT JOIN recall_batches rb ON br.id = rb.recall_id
                   GROUP BY br.id
                   ORDER BY br.initiated_date DESC'''
        params = ()
    
    return execute_query(query, params, fetch_all=True)


def get_recall_by_id(recall_id):
    """Get a single recall by ID with detailed information"""
    query = (
        '''SELECT br.*, u.username as initiated_by_name,
                  COUNT(rb.id) as affected_batches_count,
                  SUM(rb.quantity_affected) as total_quantity_affected
           FROM batch_recalls br
           LEFT JOIN users u ON br.initiated_by = u.id
           LEFT JOIN recall_batches rb ON br.id = rb.recall_id
           WHERE br.id = %s
           GROUP BY br.id'''
        if DB_TYPE == 'mysql'
        else '''SELECT br.*, u.username as initiated_by_name,
                        COUNT(rb.id) as affected_batches_count,
                        SUM(rb.quantity_affected) as total_quantity_affected
                FROM batch_recalls br
                LEFT JOIN users u ON br.initiated_by = u.id
                LEFT JOIN recall_batches rb ON br.id = rb.recall_id
                WHERE br.id = ?
                GROUP BY br.id'''
    )
    return execute_query(query, (recall_id,), fetch_one=True)


def get_recall_by_number(recall_number):
    """Get a recall by its unique recall number"""
    query = (
        '''SELECT br.*, u.username as initiated_by_name
           FROM batch_recalls br
           LEFT JOIN users u ON br.initiated_by = u.id
           WHERE br.recall_number = %s'''
        if DB_TYPE == 'mysql'
        else '''SELECT br.*, u.username as initiated_by_name
                FROM batch_recalls br
                LEFT JOIN users u ON br.initiated_by = u.id
                WHERE br.recall_number = ?'''
    )
    return execute_query(query, (recall_number,), fetch_one=True)


def update_recall_status(recall_id, status, notes=None):
    """Update recall status and completion information"""
    # Get current status first
    current_query = (
        'SELECT status FROM batch_recalls WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'SELECT status FROM batch_recalls WHERE id = ?'
    )
    current_recall = execute_query(current_query, (recall_id,), fetch_one=True)
    current_status = current_recall['status'] if current_recall else None
    
    print(f"DEBUG: Changing recall {recall_id} status from '{current_status}' to '{status}'")
    
    # Handle quantity adjustments based on status changes
    if status == 'cancelled' and current_status != 'cancelled':
        # Cancelling: restore quantities to batches
        print(f"DEBUG: Status change detected: {current_status} -> {status}, restoring quantities")
        restore_recall_quantities(recall_id)
    elif status in ['initiated', 'in_progress'] and current_status == 'cancelled':
        # Reopening a cancelled recall: re-deduct quantities
        print(f"DEBUG: Status change detected: {current_status} -> {status}, re-deducting quantities")
        re_deduct_recall_quantities(recall_id)
    else:
        print(f"DEBUG: Status change {current_status} -> {status}, no quantity adjustment needed")
    
    updates = ['status = %s' if DB_TYPE == 'mysql' else 'status = ?']
    params = [status]
    
    if status == 'completed':
        updates.append('completed_date = CURRENT_TIMESTAMP')
    else:
        # Clear completed_date if reopening or cancelling
        updates.append('completed_date = NULL')
    
    if notes:
        updates.append('notes = %s' if DB_TYPE == 'mysql' else 'notes = ?')
        params.append(notes)
    
    updates.append('updated_at = CURRENT_TIMESTAMP')
    params.append(recall_id)
    
    query = f'''UPDATE batch_recalls SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)

def restore_recall_quantities(recall_id):
    """Restore quantities to batches when a recall is cancelled (preserves recall history)"""
    try:
        print(f"DEBUG: Restoring quantities for cancelled recall {recall_id}")
        
        # Get all batches in this recall
        get_batches_query = (
            'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = %s'
            if DB_TYPE == 'mysql'
            else 'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = ?'
        )
        recall_batches = execute_query(get_batches_query, (recall_id,), fetch_all=True)
        print(f"DEBUG: Found {len(recall_batches)} batches to restore for recall {recall_id}")
        
        # Restore quantities to each batch
        for batch_record in recall_batches:
            batch_id = batch_record['batch_id']
            quantity_to_restore = float(batch_record['quantity_affected'] or 0)
            
            # Get current batch quantity BEFORE restoration
            batch_query = (
                'SELECT quantity FROM inventory_batches WHERE id = %s'
                if DB_TYPE == 'mysql'
                else 'SELECT quantity FROM inventory_batches WHERE id = ?'
            )
            batch_info = execute_query(batch_query, (batch_id,), fetch_one=True)
            current_quantity = float(batch_info['quantity']) if batch_info else 0
            
            print(f"DEBUG: Batch {batch_id} - Current: {current_quantity}, Restoring: {quantity_to_restore}")
            
            if quantity_to_restore > 0:
                new_quantity = current_quantity + quantity_to_restore
                
                restore_query = (
                    'UPDATE inventory_batches SET quantity = %s WHERE id = %s'
                    if DB_TYPE == 'mysql'
                    else 'UPDATE inventory_batches SET quantity = ? WHERE id = ?'
                )
                execute_query(restore_query, (new_quantity, batch_id))
                print(f"DEBUG: Batch {batch_id} quantity updated: {current_quantity} + {quantity_to_restore} = {new_quantity}")
                
                # Verify the update
                verify_query = (
                    'SELECT quantity FROM inventory_batches WHERE id = %s'
                    if DB_TYPE == 'mysql'
                    else 'SELECT quantity FROM inventory_batches WHERE id = ?'
                )
                verify_info = execute_query(verify_query, (batch_id,), fetch_one=True)
                final_quantity = float(verify_info['quantity']) if verify_info else 0
                print(f"DEBUG: Batch {batch_id} final quantity verified: {final_quantity}")
        
        return True
        
    except Exception as e:
        print(f"DEBUG: Error restoring recall quantities: {str(e)}")
        raise e

def re_deduct_recall_quantities(recall_id):
    """Re-deduct quantities from batches when a cancelled recall is reopened"""
    try:
        print(f"DEBUG: Re-deducting quantities for reopened recall {recall_id}")
        
        # Get all batches in this recall
        get_batches_query = (
            'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = %s'
            if DB_TYPE == 'mysql'
            else 'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = ?'
        )
        recall_batches = execute_query(get_batches_query, (recall_id,), fetch_all=True)
        
        # Re-deduct quantities from each batch
        for batch_record in recall_batches:
            batch_id = batch_record['batch_id']
            quantity_to_deduct = float(batch_record['quantity_affected'] or 0)
            
            if quantity_to_deduct > 0:
                # Check if batch has enough quantity
                batch_query = (
                    'SELECT quantity FROM inventory_batches WHERE id = %s'
                    if DB_TYPE == 'mysql'
                    else 'SELECT quantity FROM inventory_batches WHERE id = ?'
                )
                batch_info = execute_query(batch_query, (batch_id,), fetch_one=True)
                
                if batch_info:
                    current_quantity = float(batch_info['quantity'])
                    
                    if current_quantity >= quantity_to_deduct:
                        deduct_query = (
                            'UPDATE inventory_batches SET quantity = quantity - %s WHERE id = %s'
                            if DB_TYPE == 'mysql'
                            else 'UPDATE inventory_batches SET quantity = quantity - ? WHERE id = ?'
                        )
                        execute_query(deduct_query, (quantity_to_deduct, batch_id))
                        print(f"DEBUG: Re-deducted {quantity_to_deduct} units from batch {batch_id} (recall reopened)")
                    else:
                        print(f"WARNING: Batch {batch_id} only has {current_quantity} units, cannot re-deduct {quantity_to_deduct}")
        
        return True
        
    except Exception as e:
        print(f"DEBUG: Error re-deducting recall quantities: {str(e)}")
        raise e

def delete_recall_completely(recall_id):
    """Completely delete a recall and restore all batch quantities"""
    # Check if recall is already cancelled (quantities already restored)
    recall_status_query = (
        'SELECT status FROM batch_recalls WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'SELECT status FROM batch_recalls WHERE id = ?'
    )
    recall_info = execute_query(recall_status_query, (recall_id,), fetch_one=True)
    current_status = recall_info['status'] if recall_info else None
    
    print(f"DEBUG: delete_recall_completely called for recall {recall_id} with status '{current_status}'")
    
    # Only restore quantities if recall is NOT already cancelled
    if current_status != 'cancelled':
        print(f"DEBUG: Recall {recall_id} is '{current_status}', restoring quantities before deletion")
        
        # First get all batches in this recall
        get_batches_query = (
            'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = %s'
            if DB_TYPE == 'mysql'
            else 'SELECT batch_id, quantity_affected FROM recall_batches WHERE recall_id = ?'
        )
        recall_batches = execute_query(get_batches_query, (recall_id,), fetch_all=True)
        
        # Restore quantities to each batch
        for batch_record in recall_batches:
            batch_id = batch_record['batch_id']
            quantity_to_restore = float(batch_record['quantity_affected'] or 0)
            
            if quantity_to_restore > 0:
                restore_query = (
                    'UPDATE inventory_batches SET quantity = quantity + %s WHERE id = %s'
                    if DB_TYPE == 'mysql'
                    else 'UPDATE inventory_batches SET quantity = quantity + ? WHERE id = ?'
                )
                execute_query(restore_query, (quantity_to_restore, batch_id))
                print(f"DEBUG: Restored {quantity_to_restore} units to batch {batch_id} (complete deletion)")
    else:
        print(f"DEBUG: Recall {recall_id} is already cancelled, quantities already restored - skipping restoration")
    
    # Delete all recall batch records
    delete_batches_query = (
        'DELETE FROM recall_batches WHERE recall_id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM recall_batches WHERE recall_id = ?'
    )
    execute_query(delete_batches_query, (recall_id,))
    
    # Delete the recall itself
    delete_recall_query = (
        'DELETE FROM batch_recalls WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM batch_recalls WHERE id = ?'
    )
    return execute_query(delete_recall_query, (recall_id,))


def update_recall_notifications(recall_id, customer_sent=None, regulatory_sent=None):
    """Update notification status for a recall"""
    updates = []
    params = []
    
    if customer_sent is not None:
        updates.append('customer_notification_sent = %s' if DB_TYPE == 'mysql' else 'customer_notification_sent = ?')
        params.append(customer_sent)
    
    if regulatory_sent is not None:
        updates.append('regulatory_notification_sent = %s' if DB_TYPE == 'mysql' else 'regulatory_notification_sent = ?')
        params.append(regulatory_sent)
    
    if not updates:
        return True
    
    updates.append('updated_at = CURRENT_TIMESTAMP')
    params.append(recall_id)
    
    query = f'''UPDATE batch_recalls SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)


# Recall Batch Management
def add_recall_batch(recall_id, batch_id, quantity_affected=None, notes=None):
    """Add a batch to a recall and reduce inventory quantity"""
    # First, get the current batch quantity
    batch_query = (
        'SELECT quantity FROM inventory_batches WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'SELECT quantity FROM inventory_batches WHERE id = ?'
    )
    batch_info = execute_query(batch_query, (batch_id,), fetch_one=True)
    
    if not batch_info:
        raise Exception(f"Batch {batch_id} not found")
    
    current_quantity = float(batch_info['quantity'])
    
    # Require quantity_affected to be specified
    if not quantity_affected or quantity_affected == '' or quantity_affected == '0':
        raise Exception(f"Recall quantity must be specified and greater than 0")
    
    recall_quantity = float(quantity_affected)
    
    # Validate recall quantity
    if recall_quantity <= 0:
        raise Exception(f"Recall quantity must be greater than 0")
    
    if recall_quantity > current_quantity:
        raise Exception(f"Cannot recall {recall_quantity} units - only {current_quantity} available in batch")
    
    # Insert recall record
    recall_query = (
        '''INSERT INTO recall_batches (recall_id, batch_id, quantity_affected, notes) 
           VALUES (%s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO recall_batches (recall_id, batch_id, quantity_affected, notes) 
                VALUES (?, ?, ?, ?)'''
    )
    recall_result = execute_query(recall_query, (recall_id, batch_id, recall_quantity, notes))
    
    if recall_result:
        # Reduce batch quantity by recalled amount
        new_quantity = current_quantity - recall_quantity
        update_query = (
            'UPDATE inventory_batches SET quantity = %s WHERE id = %s'
            if DB_TYPE == 'mysql'
            else 'UPDATE inventory_batches SET quantity = ? WHERE id = ?'
        )
        execute_query(update_query, (new_quantity, batch_id))
        
        print(f"DEBUG: Reduced batch {batch_id} quantity from {current_quantity} to {new_quantity} (recalled: {recall_quantity})")
    
    return recall_result


def get_recall_batches(recall_id):
    """Get all batches associated with a recall"""
    query = (
        '''SELECT rb.*, b.batch_number, b.arrival_date, b.expiration_date, b.quantity,
                  p.name as product_name, p.animal_type, p.cut_type,
                  s.name as supplier_name
           FROM recall_batches rb
           JOIN inventory_batches b ON rb.batch_id = b.id
           JOIN products p ON b.product_id = p.id
           LEFT JOIN suppliers s ON p.supplier_id = s.id
           WHERE rb.recall_id = %s
           ORDER BY rb.created_at DESC'''
        if DB_TYPE == 'mysql'
        else '''SELECT rb.*, b.batch_number, b.arrival_date, b.expiration_date, b.quantity,
                        p.name as product_name, p.animal_type, p.cut_type,
                        s.name as supplier_name
                FROM recall_batches rb
                JOIN inventory_batches b ON rb.batch_id = b.id
                JOIN products p ON b.product_id = p.id
                LEFT JOIN suppliers s ON p.supplier_id = s.id
                WHERE rb.recall_id = ?
                ORDER BY rb.created_at DESC'''
    )
    return execute_query(query, (recall_id,), fetch_all=True)


def update_batch_recovery_status(recall_batch_id, recovery_status, notes=None):
    """Update the recovery status of a recalled batch"""
    updates = ['recovery_status = %s' if DB_TYPE == 'mysql' else 'recovery_status = ?']
    params = [recovery_status]
    
    if recovery_status == 'recovered':
        updates.append('recovery_date = CURRENT_TIMESTAMP')
    
    if notes:
        updates.append('notes = %s' if DB_TYPE == 'mysql' else 'notes = ?')
        params.append(notes)
    
    params.append(recall_batch_id)
    
    query = f'''UPDATE recall_batches SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)


def update_batch_recovery_details(recall_batch_id, **kwargs):
    """Update comprehensive recovery details of a recalled batch"""
    try:
        print(f"DEBUG: update_batch_recovery_details called with recall_batch_id={recall_batch_id}, kwargs={kwargs}")
        
        updates = []
        params = []
        
        # Handle quantity affected changes with batch quantity adjustment
        if 'quantity_affected' in kwargs and kwargs['quantity_affected'] is not None:
            print(f"DEBUG: Processing quantity_affected update to {kwargs['quantity_affected']}")
            
            # Get current recall info
            current_query = (
                'SELECT batch_id, quantity_affected FROM recall_batches WHERE id = %s'
                if DB_TYPE == 'mysql'
                else 'SELECT batch_id, quantity_affected FROM recall_batches WHERE id = ?'
            )
            current_info = execute_query(current_query, (recall_batch_id,), fetch_one=True)
            print(f"DEBUG: Current recall info: {current_info}")
            
            if current_info:
                batch_id = current_info['batch_id']
                old_quantity = float(current_info['quantity_affected'] or 0)
                new_quantity = float(kwargs['quantity_affected'])
                
                # Calculate adjustment needed
                quantity_diff = new_quantity - old_quantity
                print(f"DEBUG: Quantity change: {old_quantity} -> {new_quantity} (diff: {quantity_diff})")
                
                # Get current batch quantity
                batch_query = (
                    'SELECT quantity FROM inventory_batches WHERE id = %s'
                    if DB_TYPE == 'mysql'
                    else 'SELECT quantity FROM inventory_batches WHERE id = ?'
                )
                batch_info = execute_query(batch_query, (batch_id,), fetch_one=True)
                print(f"DEBUG: Current batch info: {batch_info}")
                
                if batch_info:
                    current_batch_qty = float(batch_info['quantity'])
                    
                    # Check if we have enough to increase recall
                    if quantity_diff > 0 and quantity_diff > current_batch_qty:
                        raise Exception(f"Cannot increase recall by {quantity_diff} - only {current_batch_qty} available in batch")
                    
                    # Adjust batch quantity (subtract the difference)
                    new_batch_qty = current_batch_qty - quantity_diff
                    
                    if new_batch_qty < 0:
                        raise Exception(f"Invalid quantity adjustment would result in negative batch quantity")
                    
                    # Update batch quantity
                    update_batch_query = (
                        'UPDATE inventory_batches SET quantity = %s WHERE id = %s'
                        if DB_TYPE == 'mysql'
                        else 'UPDATE inventory_batches SET quantity = ? WHERE id = ?'
                    )
                    execute_query(update_batch_query, (new_batch_qty, batch_id))
                    
                    print(f"DEBUG: Adjusted batch {batch_id} quantity by {-quantity_diff} (from {current_batch_qty} to {new_batch_qty})")
            
            updates.append('quantity_affected = %s' if DB_TYPE == 'mysql' else 'quantity_affected = ?')
            params.append(new_quantity)
        
        # Handle recovery status
        if 'recovery_status' in kwargs and kwargs['recovery_status']:
            updates.append('recovery_status = %s' if DB_TYPE == 'mysql' else 'recovery_status = ?')
            params.append(kwargs['recovery_status'])
        
        # Handle recovery date
        if 'recovery_date' in kwargs and kwargs['recovery_date']:
            updates.append('recovery_date = %s' if DB_TYPE == 'mysql' else 'recovery_date = ?')
            params.append(kwargs['recovery_date'])
        elif 'recovery_status' in kwargs and kwargs['recovery_status'] == 'recovered':
            # Auto-set recovery date when status changes to recovered
            updates.append('recovery_date = CURRENT_TIMESTAMP')
        
        # Handle notes
        if 'notes' in kwargs and kwargs['notes'] is not None:
            updates.append('notes = %s' if DB_TYPE == 'mysql' else 'notes = ?')
            params.append(kwargs['notes'])
        
        if not updates:
            print("DEBUG: No updates needed")
            return True  # No updates needed
        
        params.append(recall_batch_id)
        
        query = f'''UPDATE recall_batches SET {', '.join(updates)} 
                    WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
        
        print(f"DEBUG: Executing query: {query} with params: {params}")
        result = execute_query(query, params)
        print(f"DEBUG: Query result: {result}")
        return result
        
    except Exception as e:
        print(f"DEBUG: Exception in update_batch_recovery_details: {str(e)}")
        raise e


def remove_batch_from_recall(recall_batch_id):
    """Remove a batch from a recall (if added in error)"""
    query = (
        'DELETE FROM recall_batches WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM recall_batches WHERE id = ?'
    )
    return execute_query(query, (recall_batch_id,))

def remove_batch_from_all_recalls(batch_id):
    """Remove a batch from ALL recalls and restore quantities (use with caution - for cleanup purposes)"""
    # First get all recall quantities to restore
    get_recalls_query = (
        'SELECT quantity_affected FROM recall_batches WHERE batch_id = %s'
        if DB_TYPE == 'mysql'
        else 'SELECT quantity_affected FROM recall_batches WHERE batch_id = ?'
    )
    recall_records = execute_query(get_recalls_query, (batch_id,), fetch_all=True)
    
    # Calculate total quantity to restore
    total_to_restore = sum(float(record['quantity_affected'] or 0) for record in recall_records)
    
    if total_to_restore > 0:
        # Restore quantity to batch
        restore_query = (
            'UPDATE inventory_batches SET quantity = quantity + %s WHERE id = %s'
            if DB_TYPE == 'mysql'
            else 'UPDATE inventory_batches SET quantity = quantity + ? WHERE id = ?'
        )
        execute_query(restore_query, (total_to_restore, batch_id))
        print(f"DEBUG: Restored {total_to_restore} units to batch {batch_id}")
    
    # Remove recall records
    delete_query = (
        'DELETE FROM recall_batches WHERE batch_id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM recall_batches WHERE batch_id = ?'
    )
    return execute_query(delete_query, (batch_id,))


# Recall Traceability and Impact Analysis
def get_downstream_products_for_batch(batch_id, max_depth=5):
    """
    Get all downstream products that were created from a recalled batch
    This traces through the processing system to find affected products
    """
    downstream_products = []
    
    # Get processing sessions that used this batch
    query = (
        '''SELECT DISTINCT ps.id, ps.session_name, ps.session_date
           FROM processing_sessions ps
           JOIN processing_inputs pi ON ps.id = pi.session_id
           WHERE pi.batch_id = %s'''
        if DB_TYPE == 'mysql'
        else '''SELECT DISTINCT ps.id, ps.session_name, ps.session_date
                FROM processing_sessions ps
                JOIN processing_inputs pi ON ps.id = pi.session_id
                WHERE pi.batch_id = ?'''
    )
    
    sessions = execute_query(query, (batch_id,), fetch_all=True)
    
    for session in sessions:
        # Get outputs from each session
        output_query = (
            '''SELECT po.*, p.name as product_name, p.animal_type, p.cut_type
               FROM processing_outputs po
               JOIN products p ON po.product_id = p.id
               WHERE po.session_id = %s'''
            if DB_TYPE == 'mysql'
            else '''SELECT po.*, p.name as product_name, p.animal_type, p.cut_type
                    FROM processing_outputs po
                    JOIN products p ON po.product_id = p.id
                    WHERE po.session_id = ?'''
        )
        
        outputs = execute_query(output_query, (session['id'],), fetch_all=True)
        
        for output in outputs:
            downstream_products.append({
                'session_id': session['id'],
                'session_name': session['session_name'],
                'session_date': session['session_date'],
                'output_id': output['id'],
                'product_name': output['product_name'],
                'animal_type': output['animal_type'],
                'cut_type': output['cut_type'],
                'output_type': output['output_type'],
                'weight': output['weight'],
                'created_at': output['created_at']
            })
    
    return downstream_products


def get_recall_impact_summary(recall_id):
    """Get comprehensive impact summary for a recall"""
    recall = get_recall_by_id(recall_id)
    if not recall:
        return None
    
    # Get all recalled batches
    recalled_batches = get_recall_batches(recall_id)
    
    # Get downstream impact for each batch
    total_downstream_products = 0
    affected_sessions = set()
    
    for batch in recalled_batches:
        downstream = get_downstream_products_for_batch(batch['batch_id'])
        total_downstream_products += len(downstream)
        
        for product in downstream:
            affected_sessions.add(product['session_id'])
    
    # Calculate recovery statistics
    recovered_batches = sum(1 for batch in recalled_batches if batch['recovery_status'] == 'recovered')
    pending_recovery = sum(1 for batch in recalled_batches if batch['recovery_status'] == 'pending')
    
    return {
        'recall': recall,
        'total_batches': len(recalled_batches),
        'recovered_batches': recovered_batches,
        'pending_recovery': pending_recovery,
        'total_downstream_products': total_downstream_products,
        'affected_processing_sessions': len(affected_sessions),
        'customer_notified': recall['customer_notification_sent'],
        'regulatory_notified': recall['regulatory_notification_sent']
    }


# Recall Reporting Functions
def get_recall_statistics():
    """Get recall statistics for dashboard and reporting"""
    stats = {}
    
    # Total recalls by status
    query = '''SELECT status, COUNT(*) as count FROM batch_recalls GROUP BY status'''
    status_counts = execute_query(query, fetch_all=True)
    stats['by_status'] = {row['status']: row['count'] for row in status_counts} if status_counts else {}
    
    # Recalls by severity level
    query = '''SELECT severity_level, COUNT(*) as count FROM batch_recalls GROUP BY severity_level'''
    severity_counts = execute_query(query, fetch_all=True)
    stats['by_severity'] = {row['severity_level']: row['count'] for row in severity_counts} if severity_counts else {}
    
    # Recent recalls (last 30 days)
    if DB_TYPE == 'mysql':
        query = '''SELECT COUNT(*) as count FROM batch_recalls 
                   WHERE initiated_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)'''
    else:
        query = '''SELECT COUNT(*) as count FROM batch_recalls 
                   WHERE initiated_date >= date('now', '-30 days')'''
    
    result = execute_query(query, fetch_one=True)
    stats['recent_recalls'] = result['count'] if result else 0
    
    # Active recalls
    query = '''SELECT COUNT(*) as count FROM batch_recalls WHERE status = 'initiated' '''
    result = execute_query(query, fetch_one=True)
    stats['active_recalls'] = result['count'] if result else 0
    
    return stats


def search_batch_recalls(search_text, status=None):
    """Search batch recalls by recall number, title, severity level, or status."""
    if not search_text:
        return get_all_batch_recalls(status)

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    base = '''
        SELECT br.*, u.username as initiated_by_name,
               COUNT(rb.id) as affected_batches_count
        FROM batch_recalls br
        LEFT JOIN users u ON br.initiated_by = u.id
        LEFT JOIN recall_batches rb ON br.id = rb.recall_id
    '''
    where = []
    params = []
    if status:
        where.append(f"br.status = {placeholder}")
        params.append(status)
    where.append(f'''(
        LOWER(br.recall_number) LIKE LOWER({placeholder}) OR
        LOWER(br.title) LIKE LOWER({placeholder}) OR
        LOWER(br.severity_level) LIKE LOWER({placeholder}) OR
        LOWER(br.status) LIKE LOWER({placeholder})
    )''')
    params.extend([pattern, pattern, pattern, pattern])
    query = base + (" WHERE " + " AND ".join(where) if where else "") + " GROUP BY br.id ORDER BY br.initiated_date DESC"
    return execute_query(query, tuple(params), fetch_all=True)


def search_batches_for_recall(search_criteria):
    """
    Search for batches that might need to be recalled based on various criteria
    Returns batches matching supplier, product, date range, etc.
    """
    conditions = []
    params = []
    
    base_query = '''
        SELECT b.*, p.name as product_name, p.animal_type, p.cut_type,
               s.name as supplier_name, b.storage_location as storage_name
        FROM inventory_batches b
        JOIN products p ON b.product_id = p.id
        LEFT JOIN suppliers s ON p.supplier_id = s.id
    '''
    
    # Add search conditions based on criteria
    if search_criteria.get('supplier_id'):
        conditions.append('p.supplier_id = %s' if DB_TYPE == 'mysql' else 'p.supplier_id = ?')
        params.append(search_criteria['supplier_id'])
    
    if search_criteria.get('product_id'):
        conditions.append('b.product_id = %s' if DB_TYPE == 'mysql' else 'b.product_id = ?')
        params.append(search_criteria['product_id'])
    
    if search_criteria.get('arrival_date_from'):
        conditions.append('b.arrival_date >= %s' if DB_TYPE == 'mysql' else 'b.arrival_date >= ?')
        params.append(search_criteria['arrival_date_from'])
    
    if search_criteria.get('arrival_date_to'):
        conditions.append('b.arrival_date <= %s' if DB_TYPE == 'mysql' else 'b.arrival_date <= ?')
        params.append(search_criteria['arrival_date_to'])
    
    if search_criteria.get('batch_number_pattern'):
        conditions.append('b.batch_number LIKE %s' if DB_TYPE == 'mysql' else 'b.batch_number LIKE ?')
        params.append(f"%{search_criteria['batch_number_pattern']}%")
    
    # Exclude batches currently in an active recall only (allow reuse if prior recall is completed or cancelled)
    conditions.append('''b.id NOT IN (
        SELECT DISTINCT rb.batch_id FROM recall_batches rb 
        JOIN batch_recalls br ON rb.recall_id = br.id 
        WHERE br.status = 'initiated'
    )''')
    
    if conditions:
        query = base_query + ' WHERE ' + ' AND '.join(conditions)
    else:
        query = base_query
    
    query += ' ORDER BY b.arrival_date DESC'
    
    return execute_query(query, params, fetch_all=True)


def get_batch_recall_history(batch_id):
    """Get recall history for a specific batch"""
    query = (
        '''SELECT br.*, rb.quantity_affected, rb.recovery_status, rb.recovery_date, rb.notes as batch_notes,
                  u.username as initiated_by_name
           FROM recall_batches rb
           JOIN batch_recalls br ON rb.recall_id = br.id
           LEFT JOIN users u ON br.initiated_by = u.id
           WHERE rb.batch_id = %s
           ORDER BY br.initiated_date DESC'''
        if DB_TYPE == 'mysql'
        else '''SELECT br.*, rb.quantity_affected, rb.recovery_status, rb.recovery_date, rb.notes as batch_notes,
                        u.username as initiated_by_name
                FROM recall_batches rb
                JOIN batch_recalls br ON rb.recall_id = br.id
                LEFT JOIN users u ON br.initiated_by = u.id
                WHERE rb.batch_id = ?
                ORDER BY br.initiated_date DESC'''
    )
    return execute_query(query, (batch_id,), fetch_all=True)