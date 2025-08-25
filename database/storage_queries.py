from .user_queries import execute_query
from .connection import DB_TYPE

# Storage Location Operations
def add_storage_location(name, description, location_type, capacity):
    """Add a new storage location"""
    query = (
        'INSERT INTO storage_locations (name, description, location_type, capacity) VALUES (%s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO storage_locations (name, description, location_type, capacity) VALUES (?, ?, ?, ?)'
    )
    params = (name, description, location_type, capacity)
    return execute_query(query, params)

def get_all_storage_locations():
    """Get all storage locations with sensor count"""
    query = '''
        SELECT sl.*, COUNT(s.id) as sensor_count
        FROM storage_locations sl
        LEFT JOIN storage_sensors s ON sl.id = s.storage_id
        GROUP BY sl.id
        ORDER BY sl.name
    '''
    return execute_query(query, fetch_all=True)

def get_storage_location_by_id(storage_id):
    """Get a single storage location by ID"""
    query = 'SELECT * FROM storage_locations WHERE id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM storage_locations WHERE id = ?'
    return execute_query(query, (storage_id,), fetch_one=True)

def update_storage_location(storage_id, name, description, location_type, capacity):
    """Update an existing storage location"""
    query = (
        'UPDATE storage_locations SET name = %s, description = %s, location_type = %s, capacity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'UPDATE storage_locations SET name = ?, description = ?, location_type = ?, capacity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    )
    params = (name, description, location_type, capacity, storage_id)
    return execute_query(query, params)

def delete_storage_location(storage_id):
    """Delete a storage location"""
    query = 'DELETE FROM storage_locations WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM storage_locations WHERE id = ?'
    return execute_query(query, (storage_id,))

# Storage Sensor Operations
def add_storage_sensor(storage_id, sensor_type, sensor_id, status='active'):
    """Add a new sensor to a storage location"""
    query = (
        'INSERT INTO storage_sensors (storage_id, sensor_type, sensor_id, status) VALUES (%s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO storage_sensors (storage_id, sensor_type, sensor_id, status) VALUES (?, ?, ?, ?)'
    )
    params = (storage_id, sensor_type, sensor_id, status)
    return execute_query(query, params)

def get_sensors_for_storage(storage_id):
    """Get all sensors for a specific storage location"""
    query = 'SELECT * FROM storage_sensors WHERE storage_id = %s' if DB_TYPE == 'mysql' else 'SELECT * FROM storage_sensors WHERE storage_id = ?'
    return execute_query(query, (storage_id,), fetch_all=True)

def update_sensor_status(sensor_id, status):
    """Update sensor status"""
    query = (
        'UPDATE storage_sensors SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
        if DB_TYPE == 'mysql'
        else 'UPDATE storage_sensors SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
    )
    params = (status, sensor_id)
    return execute_query(query, params)

def delete_sensor(sensor_id):
    """Delete a sensor"""
    query = 'DELETE FROM storage_sensors WHERE id = %s' if DB_TYPE == 'mysql' else 'DELETE FROM storage_sensors WHERE id = ?'
    return execute_query(query, (sensor_id,))

# Sensor Readings Operations
def add_sensor_reading(sensor_id, temperature, humidity, alert_status='normal'):
    """Add a new sensor reading"""
    query = (
        'INSERT INTO sensor_readings (sensor_id, temperature, humidity, alert_status) VALUES (%s, %s, %s, %s)'
        if DB_TYPE == 'mysql'
        else 'INSERT INTO sensor_readings (sensor_id, temperature, humidity, alert_status) VALUES (?, ?, ?, ?)'
    )
    params = (sensor_id, temperature, humidity, alert_status)
    return execute_query(query, params)

def get_latest_readings_for_storage(storage_id):
    """Get latest sensor readings for a storage location"""
    query = '''
        SELECT sr.*, ss.sensor_type, ss.sensor_id as device_id
        FROM sensor_readings sr
        JOIN storage_sensors ss ON sr.sensor_id = ss.id
        WHERE ss.storage_id = %s
        AND sr.timestamp = (
            SELECT MAX(timestamp) 
            FROM sensor_readings sr2 
            WHERE sr2.sensor_id = sr.sensor_id
        )
        ORDER BY ss.sensor_type
    ''' if DB_TYPE == 'mysql' else '''
        SELECT sr.*, ss.sensor_type, ss.sensor_id as device_id
        FROM sensor_readings sr
        JOIN storage_sensors ss ON sr.sensor_id = ss.id
        WHERE ss.storage_id = ?
        AND sr.timestamp = (
            SELECT MAX(timestamp) 
            FROM sensor_readings sr2 
            WHERE sr2.sensor_id = sr.sensor_id
        )
        ORDER BY ss.sensor_type
    '''
    return execute_query(query, (storage_id,), fetch_all=True)

def get_readings_history(sensor_id, limit=24):
    """Get historical readings for a sensor (last 24 readings by default)"""
    query = (
        'SELECT * FROM sensor_readings WHERE sensor_id = %s ORDER BY timestamp DESC LIMIT %s'
        if DB_TYPE == 'mysql'
        else 'SELECT * FROM sensor_readings WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT ?'
    )
    params = (sensor_id, limit)
    return execute_query(query, params, fetch_all=True)

def get_alert_readings():
    """Get all readings with alerts"""
    query = "SELECT sr.*, ss.sensor_type, sl.name as storage_name FROM sensor_readings sr JOIN storage_sensors ss ON sr.sensor_id = ss.id JOIN storage_locations sl ON ss.storage_id = sl.id WHERE sr.alert_status != 'normal' ORDER BY sr.timestamp DESC"
    return execute_query(query, fetch_all=True)

# Storage Statistics
def get_storage_stats():
    """Get storage statistics for dashboard"""
    query = '''
        SELECT 
            COUNT(sl.id) as total_locations,
            COUNT(ss.id) as total_sensors,
            COUNT(CASE WHEN sr.alert_status != 'normal' THEN 1 END) as active_alerts
        FROM storage_locations sl
        LEFT JOIN storage_sensors ss ON sl.id = ss.storage_id
        LEFT JOIN sensor_readings sr ON ss.id = sr.sensor_id AND sr.timestamp = (
            SELECT MAX(timestamp) FROM sensor_readings sr2 WHERE sr2.sensor_id = sr.sensor_id
        )
    '''
    return execute_query(query, fetch_one=True)

def get_enhanced_storage_stats():
    """Get enhanced storage statistics for dashboard"""
    # Get basic stats
    basic_stats = get_storage_stats()
    
    # Get average temperature from latest readings
    temp_query = '''
        SELECT AVG(sr.temperature) as avg_temperature
        FROM sensor_readings sr
        JOIN storage_sensors ss ON sr.sensor_id = ss.id
        WHERE sr.timestamp = (
            SELECT MAX(timestamp) FROM sensor_readings sr2 WHERE sr2.sensor_id = sr.sensor_id
        )
        AND sr.temperature IS NOT NULL
    '''
    temp_result = execute_query(temp_query, fetch_one=True)
    
    # Get storage locations with their status
    locations_query = '''
        SELECT 
            sl.id,
            sl.name,
            sl.location_type,
            sl.capacity,
            COUNT(ss.id) as sensor_count,
            COUNT(CASE WHEN sr.alert_status != 'normal' THEN 1 END) as alert_count,
            AVG(sr.temperature) as avg_temp,
            AVG(sr.humidity) as avg_humidity
        FROM storage_locations sl
        LEFT JOIN storage_sensors ss ON sl.id = ss.storage_id
        LEFT JOIN sensor_readings sr ON ss.id = sr.sensor_id AND sr.timestamp = (
            SELECT MAX(timestamp) FROM sensor_readings sr2 WHERE sr2.sensor_id = sr.sensor_id
        )
        GROUP BY sl.id, sl.name, sl.location_type, sl.capacity
        ORDER BY sl.name
    '''
    locations = execute_query(locations_query, fetch_all=True)
    
    return {
        'total_locations': basic_stats['total_locations'] if basic_stats else 0,
        'total_sensors': basic_stats['total_sensors'] if basic_stats else 0,
        'active_alerts': basic_stats['active_alerts'] if basic_stats else 0,
        'avg_temperature': temp_result['avg_temperature'] if temp_result and temp_result['avg_temperature'] else None,
        'locations': locations or []
    }

def get_storage_chart_data(storage_id, hours=24):
    """Get chart data for temperature and humidity trends"""
    query = '''
        SELECT 
            sr.temperature,
            sr.humidity,
            sr.timestamp,
            sr.alert_status
        FROM sensor_readings sr
        JOIN storage_sensors ss ON sr.sensor_id = ss.id
        WHERE ss.storage_id = %s
        AND sr.timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        ORDER BY sr.timestamp ASC
    ''' if DB_TYPE == 'mysql' else '''
        SELECT 
            sr.temperature,
            sr.humidity,
            sr.timestamp,
            sr.alert_status
        FROM sensor_readings sr
        JOIN storage_sensors ss ON sr.sensor_id = ss.id
        WHERE ss.storage_id = ?
        AND sr.timestamp >= datetime('now', '-%s hours')
        ORDER BY sr.timestamp ASC
    '''
    
    readings = execute_query(query, (storage_id, hours), fetch_all=True)
    
    if not readings:
        return {
            'temperature': {'categories': [], 'series': []},
            'humidity': {'categories': [], 'series': []},
            'alerts': []
        }
    
    # Process data for charts
    temp_categories = []
    temp_series = []
    humidity_categories = []
    humidity_series = []
    alerts = []
    
    for reading in readings:
        timestamp = reading['timestamp']
        if hasattr(timestamp, 'strftime'):
            time_str = timestamp.strftime('%H:%M')
        else:
            time_str = str(timestamp)[-8:-3]  # Extract HH:MM from string
        
        if reading['temperature'] is not None:
            temp_categories.append(time_str)
            temp_series.append(float(reading['temperature']))
        
        if reading['humidity'] is not None:
            humidity_categories.append(time_str)
            humidity_series.append(float(reading['humidity']))
        
        if reading['alert_status'] != 'normal':
            alerts.append({
                'time': time_str,
                'status': reading['alert_status'],
                'temperature': reading['temperature'],
                'humidity': reading['humidity']
            })
    
    return {
        'temperature': {
            'categories': temp_categories,
            'series': temp_series
        },
        'humidity': {
            'categories': humidity_categories,
            'series': humidity_series
        },
        'alerts': alerts
    } 


def search_storage_locations(search_text):
    """Search storage locations by name, type, or description (with sensor count)."""
    if not search_text:
        return get_all_storage_locations()

    placeholder = '%s' if DB_TYPE == 'mysql' else '?'
    pattern = f"%{search_text}%"
    query = f'''
        SELECT sl.*, COUNT(s.id) as sensor_count
        FROM storage_locations sl
        LEFT JOIN storage_sensors s ON sl.id = s.storage_id
        WHERE LOWER(sl.name) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(sl.location_type, '')) LIKE LOWER({placeholder})
           OR LOWER(COALESCE(sl.description, '')) LIKE LOWER({placeholder})
        GROUP BY sl.id
        ORDER BY sl.name
    '''
    params = (pattern, pattern, pattern)
    return execute_query(query, params, fetch_all=True)