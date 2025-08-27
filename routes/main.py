
from flask import Blueprint, render_template, jsonify
from flask_login import login_required
from database import (
    get_user_stats, 
    get_expired_batches, 
    get_soon_to_expire_batches,
    get_product_counts_by_animal_type,
    get_inventory_over_time
)
from database import get_current_stock_by_product
from database.storage_queries import get_enhanced_storage_stats
import json
from decimal import Decimal

main_bp = Blueprint('main', __name__, template_folder='templates')

def default_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

@main_bp.route('/')
def index():
    return render_template('index.html')

@main_bp.route('/dashboard')
@login_required
def dashboard():
    stats = get_user_stats()
    expired_batches = get_expired_batches()
    soon_to_expire_batches = get_soon_to_expire_batches()
    storage_stats = get_enhanced_storage_stats()
    stock = get_current_stock_by_product() or []
    zero_stock = [s for s in stock if (s.get('total_qty') or 0) == 0]

    # Data for Pie Chart
    animal_counts = get_product_counts_by_animal_type() or []
    pie_chart_data = {
        "labels": [item['animal_type'] for item in animal_counts] if animal_counts else [],
        "series": [item['product_count'] for item in animal_counts] if animal_counts else []
    }

    # Data for Line Chart
    inventory_data = get_inventory_over_time() or []
    line_chart_data = {
        "categories": [item['arrival_date'].strftime('%Y-%m-%d') for item in inventory_data] if inventory_data else [],
        "series": [item['total_quantity'] for item in inventory_data] if inventory_data else []
    }

    return render_template(
        'dashboard.html', 
        stats=stats, 
        storage_stats=storage_stats,
        expired_batches=expired_batches, 
        soon_to_expire_batches=soon_to_expire_batches,
        pie_chart_data=json.dumps(pie_chart_data),
        line_chart_data=json.dumps(line_chart_data, default=default_serializer),
        zero_stock=zero_stock
    )

@main_bp.route('/api/storage/<int:storage_id>/chart-data')
@login_required
def get_storage_chart_data(storage_id):
    """API endpoint to get chart data for a specific storage location"""
    from database.storage_queries import get_storage_chart_data
    chart_data = get_storage_chart_data(storage_id)
    return jsonify(chart_data)

@main_bp.route('/api/storage/alerts')
@login_required 
def get_storage_alerts():
    """API endpoint to get storage alerts"""
    from database.storage_queries import get_alert_readings, get_all_storage_locations
    
    alerts = get_alert_readings()
    locations = get_all_storage_locations()
    
    # Format alerts for frontend
    formatted_alerts = []
    for alert in alerts:
        formatted_alerts.append({
            'id': alert['id'],
            'storage_name': alert['storage_name'],
            'sensor_type': alert['sensor_type'],
            'temperature': alert['temperature'],
            'humidity': alert['humidity'],
            'alert_status': alert['alert_status'],
            'timestamp': alert['timestamp'].isoformat() if hasattr(alert['timestamp'], 'isoformat') else str(alert['timestamp'])
        })
    
    # Add normal status locations
    for location in locations:
        # Check if this location has any alerts
        has_alerts = any(alert['storage_name'] == location['name'] for alert in alerts)
        if not has_alerts:
            formatted_alerts.append({
                'id': f"normal_{location['id']}",
                'storage_name': location['name'],
                'sensor_type': 'system',
                'temperature': None,
                'humidity': None,
                'alert_status': 'normal',
                'timestamp': None
            })
    
    return jsonify(formatted_alerts)
