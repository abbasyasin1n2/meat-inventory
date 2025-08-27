from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from database import (
    get_all_storage_locations, add_storage_location, get_storage_location_by_id, 
    update_storage_location, delete_storage_location,
    add_storage_sensor, get_sensors_for_storage, update_sensor_status, delete_sensor,
    add_sensor_reading, get_latest_readings_for_storage, get_readings_history,
    get_alert_readings, get_storage_stats, search_storage_locations
)
import random
from datetime import datetime, timedelta

storage_bp = Blueprint('storage', __name__, template_folder='templates')

# Storage Location Routes
@storage_bp.route('/')
@login_required
def list_storage():
    q = request.args.get('q', '').strip()
    storage_locations = search_storage_locations(q) if q else get_all_storage_locations()
    return render_template('storage/list_storage.html', storage_locations=storage_locations, q=q)

@storage_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_storage():
    if request.method == 'POST':
        name = request.form['name']
        description = request.form['description']
        location_type = request.form['location_type']
        capacity = request.form['capacity']
        
        add_storage_location(name, description, location_type, capacity)
        flash('Storage location added successfully!', 'success')
        return redirect(url_for('storage.list_storage'))
    
    return render_template('storage/add_storage.html')

@storage_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_storage(id):
    storage = get_storage_location_by_id(id)
    if request.method == 'POST':
        name = request.form['name']
        description = request.form['description']
        location_type = request.form['location_type']
        capacity = request.form['capacity']
        
        update_storage_location(id, name, description, location_type, capacity)
        flash('Storage location updated successfully!', 'success')
        return redirect(url_for('storage.list_storage'))
    
    return render_template('storage/edit_storage.html', storage=storage)

@storage_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
def delete_storage_route(id):
    delete_storage_location(id)
    flash('Storage location deleted successfully!', 'success')
    return redirect(url_for('storage.list_storage'))

@storage_bp.route('/view/<int:id>')
@login_required
def view_storage(id):
    storage = get_storage_location_by_id(id)
    sensors = get_sensors_for_storage(id)
    latest_readings = get_latest_readings_for_storage(id)
    
    return render_template('storage/view_storage.html', 
                         storage=storage, 
                         sensors=sensors, 
                         latest_readings=latest_readings)

# Sensor Management Routes
@storage_bp.route('/<int:storage_id>/add_sensor', methods=['POST'])
@login_required
def add_sensor(storage_id):
    sensor_type = request.form['sensor_type']
    sensor_id = request.form['sensor_id']
    status = request.form.get('status', 'active')
    
    add_storage_sensor(storage_id, sensor_type, sensor_id, status)
    flash('Sensor added successfully!', 'success')
    return redirect(url_for('storage.view_storage', id=storage_id))

@storage_bp.route('/sensor/<int:sensor_id>/update_status', methods=['POST'])
@login_required
def update_sensor_route(sensor_id):
    status = request.form['status']
    update_sensor_status(sensor_id, status)
    flash('Sensor status updated successfully!', 'success')
    return redirect(request.referrer or url_for('storage.list_storage'))

@storage_bp.route('/sensor/<int:sensor_id>/delete', methods=['POST'])
@login_required
def delete_sensor_route(sensor_id):
    delete_sensor(sensor_id)
    flash('Sensor deleted successfully!', 'success')
    return redirect(request.referrer or url_for('storage.list_storage'))

# Sensor Data Routes
@storage_bp.route('/sensor/<int:sensor_id>/readings')
@login_required
def sensor_readings(sensor_id):
    readings = get_readings_history(sensor_id, limit=50)
    return render_template('storage/sensor_readings.html', readings=readings, sensor_id=sensor_id)

# API Routes for Sensor Integration
@storage_bp.route('/api/sensor/<int:sensor_id>/reading', methods=['POST'])
def add_sensor_reading_api(sensor_id):
    """API endpoint for adding sensor readings (for real sensor integration)"""
    try:
        data = request.get_json()
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        
        # Determine alert status based on thresholds
        alert_status = 'normal'
        if temperature is not None:
            if temperature < -2 or temperature > 4:  # Meat storage temperature range
                alert_status = 'temperature_alert'
        if humidity is not None:
            if humidity < 85 or humidity > 95:  # Meat storage humidity range
                alert_status = 'humidity_alert'
        
        add_sensor_reading(sensor_id, temperature, humidity, alert_status)
        return jsonify({'status': 'success', 'message': 'Reading added successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@storage_bp.route('/api/storage/<int:storage_id>/readings')
@login_required
def get_storage_readings_api(storage_id):
    """API endpoint for getting latest readings for a storage location"""
    readings = get_latest_readings_for_storage(storage_id)
    return jsonify(readings)

@storage_bp.route('/api/alerts')
@login_required
def get_alerts_api():
    """API endpoint for getting all active alerts"""
    alerts = get_alert_readings()
    return jsonify(alerts)

# Sensor Simulation (for testing without real sensors)
@storage_bp.route('/simulate/<int:sensor_id>')
@login_required
def simulate_sensor_reading(sensor_id):
    """Simulate a sensor reading for testing purposes"""
    # Generate realistic temperature and humidity values
    temperature = round(random.uniform(-1, 3), 2)  # Meat storage temperature range
    humidity = round(random.uniform(88, 92), 2)    # Meat storage humidity range
    
    # Determine alert status
    alert_status = 'normal'
    if temperature < -2 or temperature > 4:
        alert_status = 'temperature_alert'
    if humidity < 85 or humidity > 95:
        alert_status = 'humidity_alert'
    
    add_sensor_reading(sensor_id, temperature, humidity, alert_status)
    flash(f'Simulated reading added: {temperature}°C, {humidity}% humidity', 'info')
    return redirect(request.referrer or url_for('storage.list_storage'))

# Dashboard integration
@storage_bp.route('/stats')
@login_required
def storage_stats():
    """Get storage statistics for dashboard"""
    stats = get_storage_stats()
    return jsonify(stats) 