"""
Compliance and Regulatory Database Queries

This module handles all database operations related to:
- Compliance records (certificates, permits, inspections)
- Food safety incidents
- Compliance audits
- Regulatory documentation

All functions follow the existing pattern and use the shared connection/query infrastructure.
"""

from .user_queries import execute_query
from .connection import DB_TYPE
from datetime import datetime, timedelta


# Compliance Records Operations
def add_compliance_record(record_type, title, description=None, certificate_number=None,
                         issuing_authority=None, issue_date=None, expiration_date=None, 
                         file_path=None, created_by=None):
    """Add a new compliance record (certificate, permit, inspection report, etc.)"""
    query = (
        '''INSERT INTO compliance_records 
           (record_type, title, description, certificate_number, issuing_authority, 
            issue_date, expiration_date, file_path, created_by) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO compliance_records 
                (record_type, title, description, certificate_number, issuing_authority, 
                 issue_date, expiration_date, file_path, created_by) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    )
    params = (record_type, title, description, certificate_number, issuing_authority,
              issue_date, expiration_date, file_path, created_by)
    return execute_query(query, params)


def get_all_compliance_records(status='active'):
    """Get all compliance records, optionally filtered by status"""
    query = (
        '''SELECT cr.*, u.username as created_by_name 
           FROM compliance_records cr
           LEFT JOIN users u ON cr.created_by = u.id
           WHERE cr.status = %s
           ORDER BY cr.expiration_date ASC, cr.created_at DESC'''
        if DB_TYPE == 'mysql'
        else '''SELECT cr.*, u.username as created_by_name 
                FROM compliance_records cr
                LEFT JOIN users u ON cr.created_by = u.id
                WHERE cr.status = ?
                ORDER BY cr.expiration_date ASC, cr.created_at DESC'''
    )
    return execute_query(query, (status,), fetch_all=True)


def get_compliance_record_by_id(record_id):
    """Get a single compliance record by ID"""
    query = (
        '''SELECT cr.*, u.username as created_by_name 
           FROM compliance_records cr
           LEFT JOIN users u ON cr.created_by = u.id
           WHERE cr.id = %s'''
        if DB_TYPE == 'mysql'
        else '''SELECT cr.*, u.username as created_by_name 
                FROM compliance_records cr
                LEFT JOIN users u ON cr.created_by = u.id
                WHERE cr.id = ?'''
    )
    return execute_query(query, (record_id,), fetch_one=True)


def get_expiring_compliance_records(days_ahead=30):
    """Get compliance records expiring within specified days"""
    if DB_TYPE == 'mysql':
        query = '''SELECT cr.*, u.username as created_by_name 
                   FROM compliance_records cr
                   LEFT JOIN users u ON cr.created_by = u.id
                   WHERE cr.status = 'active' 
                   AND cr.expiration_date IS NOT NULL
                   AND cr.expiration_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                   ORDER BY cr.expiration_date ASC'''
    else:
        query = '''SELECT cr.*, u.username as created_by_name 
                   FROM compliance_records cr
                   LEFT JOIN users u ON cr.created_by = u.id
                   WHERE cr.status = 'active' 
                   AND cr.expiration_date IS NOT NULL
                   AND cr.expiration_date <= date('now', '+' || ? || ' days')
                   ORDER BY cr.expiration_date ASC'''
    
    return execute_query(query, (days_ahead,), fetch_all=True)


def update_compliance_record(record_id, record_type=None, title=None, description=None,
                           certificate_number=None, issuing_authority=None, 
                           issue_date=None, expiration_date=None, status=None, file_path=None):
    """Update an existing compliance record"""
    # Build dynamic update query based on provided parameters
    updates = []
    params = []
    
    if record_type is not None:
        updates.append('record_type = %s' if DB_TYPE == 'mysql' else 'record_type = ?')
        params.append(record_type)
    if title is not None:
        updates.append('title = %s' if DB_TYPE == 'mysql' else 'title = ?')
        params.append(title)
    if description is not None:
        updates.append('description = %s' if DB_TYPE == 'mysql' else 'description = ?')
        params.append(description)
    if certificate_number is not None:
        updates.append('certificate_number = %s' if DB_TYPE == 'mysql' else 'certificate_number = ?')
        params.append(certificate_number)
    if issuing_authority is not None:
        updates.append('issuing_authority = %s' if DB_TYPE == 'mysql' else 'issuing_authority = ?')
        params.append(issuing_authority)
    if issue_date is not None:
        updates.append('issue_date = %s' if DB_TYPE == 'mysql' else 'issue_date = ?')
        params.append(issue_date)
    if expiration_date is not None:
        updates.append('expiration_date = %s' if DB_TYPE == 'mysql' else 'expiration_date = ?')
        params.append(expiration_date)
    if status is not None:
        updates.append('status = %s' if DB_TYPE == 'mysql' else 'status = ?')
        params.append(status)
    if file_path is not None:
        updates.append('file_path = %s' if DB_TYPE == 'mysql' else 'file_path = ?')
        params.append(file_path)
    
    if not updates:
        return True  # Nothing to update
    
    # Add updated_at timestamp
    if DB_TYPE == 'mysql':
        updates.append('updated_at = CURRENT_TIMESTAMP')
    else:
        updates.append('updated_at = CURRENT_TIMESTAMP')
    
    params.append(record_id)
    
    query = f'''UPDATE compliance_records SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)


def delete_compliance_record(record_id):
    """Soft delete a compliance record by setting status to 'deleted'"""
    query = (
        '''UPDATE compliance_records SET status = 'deleted', 
           updated_at = CURRENT_TIMESTAMP WHERE id = %s'''
        if DB_TYPE == 'mysql'
        else '''UPDATE compliance_records SET status = 'deleted', 
                updated_at = CURRENT_TIMESTAMP WHERE id = ?'''
    )
    return execute_query(query, (record_id,))


def delete_food_safety_incident(incident_id):
    """Hard delete a food safety incident. Related incident_batches are removed via ON DELETE CASCADE."""
    query = (
        'DELETE FROM food_safety_incidents WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM food_safety_incidents WHERE id = ?'
    )
    return execute_query(query, (incident_id,))


def delete_compliance_audit(audit_id):
    """Hard delete a compliance audit record."""
    query = (
        'DELETE FROM compliance_audits WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM compliance_audits WHERE id = ?'
    )
    return execute_query(query, (audit_id,))


# Food Safety Incidents Operations
def add_food_safety_incident(incident_number, incident_type, title, description, 
                           severity_level, reported_by):
    """Add a new food safety incident"""
    query = (
        '''INSERT INTO food_safety_incidents 
           (incident_number, incident_type, title, description, severity_level, reported_by) 
           VALUES (%s, %s, %s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO food_safety_incidents 
                (incident_number, incident_type, title, description, severity_level, reported_by) 
                VALUES (?, ?, ?, ?, ?, ?)'''
    )
    params = (incident_number, incident_type, title, description, severity_level, reported_by)
    return execute_query(query, params)


def get_all_food_safety_incidents(status=None):
    """Get all food safety incidents, optionally filtered by status"""
    if status:
        query = (
            '''SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
               FROM food_safety_incidents fsi
               LEFT JOIN users u1 ON fsi.reported_by = u1.id
               LEFT JOIN users u2 ON fsi.closed_by = u2.id
               WHERE fsi.status = %s
               ORDER BY fsi.reported_date DESC'''
            if DB_TYPE == 'mysql'
            else '''SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
                    FROM food_safety_incidents fsi
                    LEFT JOIN users u1 ON fsi.reported_by = u1.id
                    LEFT JOIN users u2 ON fsi.closed_by = u2.id
                    WHERE fsi.status = ?
                    ORDER BY fsi.reported_date DESC'''
        )
        params = (status,)
    else:
        query = '''SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
                   FROM food_safety_incidents fsi
                   LEFT JOIN users u1 ON fsi.reported_by = u1.id
                   LEFT JOIN users u2 ON fsi.closed_by = u2.id
                   ORDER BY fsi.reported_date DESC'''
        params = ()
    
    return execute_query(query, params, fetch_all=True)


def get_food_safety_incident_by_id(incident_id):
    """Get a single food safety incident by ID"""
    query = (
        '''SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
           FROM food_safety_incidents fsi
           LEFT JOIN users u1 ON fsi.reported_by = u1.id
           LEFT JOIN users u2 ON fsi.closed_by = u2.id
           WHERE fsi.id = %s'''
        if DB_TYPE == 'mysql'
        else '''SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
                FROM food_safety_incidents fsi
                LEFT JOIN users u1 ON fsi.reported_by = u1.id
                LEFT JOIN users u2 ON fsi.closed_by = u2.id
                WHERE fsi.id = ?'''
    )
    return execute_query(query, (incident_id,), fetch_one=True)


def update_food_safety_incident(incident_id, **kwargs):
    """Update a food safety incident with provided fields"""
    # Build dynamic update query
    updates = []
    params = []
    
    allowed_fields = [
        'incident_type', 'title', 'description', 'severity_level', 'status',
        'investigation_notes', 'corrective_actions', 'root_cause', 'closed_by',
        'regulatory_reported', 'regulatory_report_date'
    ]
    
    for field, value in kwargs.items():
        if field in allowed_fields and value is not None:
            updates.append(f'{field} = %s' if DB_TYPE == 'mysql' else f'{field} = ?')
            params.append(value)
    
    if not updates:
        return True  # Nothing to update
    
    # Add updated_at timestamp
    updates.append('updated_at = CURRENT_TIMESTAMP')
    
    # Handle closed_date for status changes
    if 'status' in kwargs and kwargs['status'] == 'closed':
        updates.append('closed_date = CURRENT_TIMESTAMP')
    
    params.append(incident_id)
    
    query = f'''UPDATE food_safety_incidents SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)


def add_incident_batch(incident_id, batch_id, involvement_level, notes=None):
    """Link a batch to a food safety incident"""
    query = (
        '''INSERT INTO incident_batches (incident_id, batch_id, involvement_level, notes) 
           VALUES (%s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO incident_batches (incident_id, batch_id, involvement_level, notes) 
                VALUES (?, ?, ?, ?)'''
    )
    return execute_query(query, (incident_id, batch_id, involvement_level, notes))


def get_incident_batches(incident_id):
    """Get all batches associated with a food safety incident"""
    query = (
        '''SELECT ib.*, b.batch_number, p.name as product_name
           FROM incident_batches ib
           JOIN inventory_batches b ON ib.batch_id = b.id
           JOIN products p ON b.product_id = p.id
           WHERE ib.incident_id = %s
           ORDER BY ib.created_at DESC'''
        if DB_TYPE == 'mysql'
        else '''SELECT ib.*, b.batch_number, p.name as product_name
                FROM incident_batches ib
                JOIN inventory_batches b ON ib.batch_id = b.id
                JOIN products p ON b.product_id = p.id
                WHERE ib.incident_id = ?
                ORDER BY ib.created_at DESC'''
    )
    return execute_query(query, (incident_id,), fetch_all=True)


def remove_incident_batch(incident_id, batch_id):
    """Remove a batch from a food safety incident"""
    query = (
        'DELETE FROM incident_batches WHERE incident_id = %s AND batch_id = %s'
        if DB_TYPE == 'mysql'
        else 'DELETE FROM incident_batches WHERE incident_id = ? AND batch_id = ?'
    )
    return execute_query(query, (incident_id, batch_id))


def get_incident_batches_by_batch(batch_id):
    """Get all incidents a batch is involved in"""
    query = (
        '''SELECT ib.*, fsi.incident_number, fsi.title, fsi.status as incident_status, 
                  fsi.severity_level, fsi.reported_date
           FROM incident_batches ib
           JOIN food_safety_incidents fsi ON ib.incident_id = fsi.id
           WHERE ib.batch_id = %s
           ORDER BY fsi.reported_date DESC'''
        if DB_TYPE == 'mysql'
        else '''SELECT ib.*, fsi.incident_number, fsi.title, fsi.status as incident_status, 
                     fsi.severity_level, fsi.reported_date
                FROM incident_batches ib
                JOIN food_safety_incidents fsi ON ib.incident_id = fsi.id
                WHERE ib.batch_id = ?
                ORDER BY fsi.reported_date DESC'''
    )
    return execute_query(query, (batch_id,), fetch_all=True)


# Compliance Audits Operations
def add_compliance_audit(audit_type, auditor_name, audit_date, conducted_by, 
                        scope=None, findings=None, recommendations=None, 
                        overall_rating=None, report_file_path=None):
    """Add a new compliance audit record"""
    query = (
        '''INSERT INTO compliance_audits 
           (audit_type, auditor_name, audit_date, scope, findings, recommendations, 
            overall_rating, report_file_path, conducted_by) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        if DB_TYPE == 'mysql'
        else '''INSERT INTO compliance_audits 
                (audit_type, auditor_name, audit_date, scope, findings, recommendations, 
                 overall_rating, report_file_path, conducted_by) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    )
    params = (audit_type, auditor_name, audit_date, scope, findings, recommendations,
              overall_rating, report_file_path, conducted_by)
    return execute_query(query, params)


def get_all_compliance_audits():
    """Get all compliance audits"""
    query = '''SELECT ca.*, u.username as conducted_by_name
               FROM compliance_audits ca
               LEFT JOIN users u ON ca.conducted_by = u.id
               ORDER BY ca.audit_date DESC'''
    return execute_query(query, fetch_all=True)


def get_compliance_audit_by_id(audit_id):
    """Get a single compliance audit by ID"""
    query = (
        '''SELECT ca.*, u.username as conducted_by_name
           FROM compliance_audits ca
           LEFT JOIN users u ON ca.conducted_by = u.id
           WHERE ca.id = %s'''
        if DB_TYPE == 'mysql'
        else '''SELECT ca.*, u.username as conducted_by_name
                FROM compliance_audits ca
                LEFT JOIN users u ON ca.conducted_by = u.id
                WHERE ca.id = ?'''
    )
    return execute_query(query, (audit_id,), fetch_one=True)


def update_compliance_audit(audit_id, **kwargs):
    """Update a compliance audit with provided fields"""
    # Build dynamic update query
    updates = []
    params = []
    
    allowed_fields = [
        'audit_type', 'auditor_name', 'audit_date', 'scope', 'findings',
        'recommendations', 'overall_rating', 'follow_up_required', 'follow_up_date'
    ]
    
    for field, value in kwargs.items():
        if field in allowed_fields and value is not None:
            updates.append(f'{field} = %s' if DB_TYPE == 'mysql' else f'{field} = ?')
            params.append(value)
    
    if not updates:
        return True  # Nothing to update
    
    # Add updated_at timestamp
    updates.append('updated_at = CURRENT_TIMESTAMP')
    
    params.append(audit_id)
    
    query = f'''UPDATE compliance_audits SET {', '.join(updates)} 
                WHERE id = {'%s' if DB_TYPE == 'mysql' else '?'}'''
    
    return execute_query(query, params)


# Dashboard and Statistics Functions
def get_compliance_dashboard_stats():
    """Get compliance statistics for dashboard"""
    stats = {}
    
    # Active compliance records
    query = '''SELECT COUNT(*) as total FROM compliance_records WHERE status = 'active' '''
    result = execute_query(query, fetch_one=True)
    stats['active_records'] = result['total'] if result else 0
    
    # Expiring records (next 30 days)
    expiring_records = get_expiring_compliance_records(30)
    stats['expiring_records'] = len(expiring_records) if expiring_records else 0
    
    # Open incidents
    query = '''SELECT COUNT(*) as total FROM food_safety_incidents WHERE status = 'open' '''
    result = execute_query(query, fetch_one=True)
    stats['open_incidents'] = result['total'] if result else 0
    
    # Recent audits (last 12 months)
    if DB_TYPE == 'mysql':
        query = '''SELECT COUNT(*) as total FROM compliance_audits 
                   WHERE audit_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)'''
    else:
        query = '''SELECT COUNT(*) as total FROM compliance_audits 
                   WHERE audit_date >= date('now', '-12 months')'''
    
    result = execute_query(query, fetch_one=True)
    stats['recent_audits'] = result['total'] if result else 0
    
    return stats


def generate_incident_number():
    """Generate a unique incident number"""
    # Get current year and count of incidents this year
    current_year = datetime.now().year
    
    if DB_TYPE == 'mysql':
        query = '''SELECT COUNT(*) as count FROM food_safety_incidents 
                   WHERE YEAR(reported_date) = %s'''
    else:
        query = '''SELECT COUNT(*) as count FROM food_safety_incidents 
                   WHERE strftime('%Y', reported_date) = ?'''
    
    result = execute_query(query, (str(current_year),), fetch_one=True)
    count = result['count'] if result else 0
    
    return f"INC-{current_year}-{count + 1:04d}"


def generate_recall_number():
    """Generate a unique recall number"""
    # Get current year and count of recalls this year
    current_year = datetime.now().year
    
    if DB_TYPE == 'mysql':
        query = '''SELECT COUNT(*) as count FROM batch_recalls 
                   WHERE YEAR(initiated_date) = %s'''
    else:
        query = '''SELECT COUNT(*) as count FROM batch_recalls 
                   WHERE strftime('%Y', initiated_date) = ?'''
    
    result = execute_query(query, (str(current_year),), fetch_one=True)
    count = result['count'] if result else 0
    
    return f"RCL-{current_year}-{count + 1:04d}"


def search_compliance_records(search_text, status='active'):
    """Search compliance records by title, type, certificate number, or issuing authority."""
    if not search_text:
        return get_all_compliance_records(status)

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    query = f'''
        SELECT cr.*, u.username as created_by_name 
        FROM compliance_records cr
        LEFT JOIN users u ON cr.created_by = u.id
        WHERE cr.status = {placeholder}
          AND (
            LOWER(cr.title) LIKE LOWER({placeholder}) OR
            LOWER(cr.record_type) LIKE LOWER({placeholder}) OR
            LOWER(COALESCE(cr.certificate_number, '')) LIKE LOWER({placeholder}) OR
            LOWER(COALESCE(cr.issuing_authority, '')) LIKE LOWER({placeholder})
          )
        ORDER BY cr.expiration_date ASC, cr.created_at DESC
    '''
    params = (status, pattern, pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)


def search_food_safety_incidents(search_text, status=None):
    """Search food safety incidents by number, title, type, or severity."""
    if not search_text:
        return get_all_food_safety_incidents(status)

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    base = '''
        SELECT fsi.*, u1.username as reported_by_name, u2.username as closed_by_name
        FROM food_safety_incidents fsi
        LEFT JOIN users u1 ON fsi.reported_by = u1.id
        LEFT JOIN users u2 ON fsi.closed_by = u2.id
    '''
    where = []
    params = []
    if status:
        where.append(f"fsi.status = {placeholder}")
        params.append(status)
    where.append(f'''(
        LOWER(fsi.incident_number) LIKE LOWER({placeholder}) OR
        LOWER(fsi.title) LIKE LOWER({placeholder}) OR
        LOWER(fsi.incident_type) LIKE LOWER({placeholder}) OR
        LOWER(fsi.severity_level) LIKE LOWER({placeholder})
    )''')
    params.extend([pattern, pattern, pattern, pattern])
    query = base + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY fsi.reported_date DESC"
    return execute_query(query, tuple(params), fetch_all=True)


def search_compliance_audits(search_text):
    """Search compliance audits by audit type, auditor, rating, or status."""
    if not search_text:
        return get_all_compliance_audits()

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    query = f'''
        SELECT ca.*, u.username as conducted_by_name
        FROM compliance_audits ca
        LEFT JOIN users u ON ca.conducted_by = u.id
        WHERE LOWER(ca.audit_type) LIKE LOWER({placeholder})
           OR LOWER(ca.auditor_name) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(ca.overall_rating, '')) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(ca.status, '')) LIKE LOWER({placeholder})
        ORDER BY ca.audit_date DESC
    '''
    params = (pattern, pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)