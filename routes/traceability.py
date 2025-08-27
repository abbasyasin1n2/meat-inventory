from flask import Blueprint, render_template, request
from flask_login import login_required
import json
from database import (
    get_storage_location_by_id,
    get_batch_by_id,
    get_processing_outputs_for_session,
    get_product_by_id,
    get_supplier_by_id,
    get_all_batches,
    get_processing_sessions_for_batch,
    get_incident_batches_by_batch
)

traceability_bp = Blueprint('traceability', __name__, template_folder='templates')

@traceability_bp.route('/report', methods=['GET', 'POST'])
@login_required
def traceability_report():
    selected_batch_id = request.form.get('batch_id') if request.method == 'POST' else request.args.get('batch_id')
    
    batch = None
    product = None
    supplier = None
    storage_location = None
    usage_details = []
    incident_batches = []

    if selected_batch_id:
        batch = get_batch_by_id(selected_batch_id)
        if batch:
            product = get_product_by_id(batch['product_id'])
            if product and product['supplier_id']:
                supplier = get_supplier_by_id(product['supplier_id'])
            
            # Storage location information is now included in the batch data
            
            # Get incident information for this batch
            incident_batches = get_incident_batches_by_batch(selected_batch_id)
            
            # Use the new efficient query
            sessions = get_processing_sessions_for_batch(selected_batch_id)
            for session in sessions:
                outputs = get_processing_outputs_for_session(session['id'])
                
                # Prepare data for the donut chart
                chart_labels = [o['output_type'] for o in outputs]
                chart_series = [float(o['weight']) for o in outputs]
                chart_data = json.dumps({'labels': chart_labels, 'series': chart_series})

                usage_details.append({
                    'session': session,
                    'outputs': outputs,
                    'chart_data': chart_data
                })

    all_batches = get_all_batches()
    return render_template(
        'inventory/traceability_report.html',
        batches=all_batches,
        selected_batch_id=int(selected_batch_id) if selected_batch_id else None,
        batch=batch,
        product=product,
        supplier=supplier,
        storage_location=batch,
        usage_details=usage_details,
        incident_batches=incident_batches
    )
