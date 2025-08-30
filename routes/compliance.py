"""
Regulatory Compliance Routes

This module handles all web routes for:
- Compliance dashboard and overview
- Compliance records management (certificates, permits, inspections)
- Food safety incident tracking and management
- Batch recall initiation and monitoring
- Compliance audits and reporting

Follows the same patterns as existing routes with proper authentication and error handling.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from database.compliance_queries import (
    add_compliance_record, get_all_compliance_records, get_compliance_record_by_id,
    get_expiring_compliance_records, update_compliance_record, delete_compliance_record,
    add_food_safety_incident, get_all_food_safety_incidents, get_food_safety_incident_by_id,
    update_food_safety_incident, add_incident_batch, get_incident_batches, remove_incident_batch, delete_food_safety_incident,
    add_compliance_audit, get_all_compliance_audits, get_compliance_audit_by_id,
    update_compliance_audit, delete_compliance_audit, get_compliance_dashboard_stats, generate_incident_number, generate_recall_number,
    search_compliance_records, search_food_safety_incidents, search_compliance_audits
)
from database.recall_queries import (
    add_batch_recall, get_all_batch_recalls, get_recall_by_id, update_recall_status,
    update_recall_notifications, add_recall_batch, get_recall_batches,
    update_batch_recovery_status, update_batch_recovery_details, get_recall_impact_summary, get_recall_statistics,
    search_batches_for_recall, get_batch_recall_history, search_batch_recalls, delete_recall_completely
)
from database import get_all_batches, get_all_suppliers, get_all_products
from datetime import datetime
import json

compliance_bp = Blueprint('compliance', __name__, template_folder='templates')

# Dashboard and Overview Routes
@compliance_bp.route('/')
@compliance_bp.route('/dashboard')
@login_required
def dashboard():
    """Compliance dashboard with overview statistics"""
    stats = get_compliance_dashboard_stats()
    recall_stats = get_recall_statistics()
    expiring_records = get_expiring_compliance_records(30)
    recent_incidents = get_all_food_safety_incidents('open')[:5] # Latest 5 open incidents
    active_recalls = get_all_batch_recalls('initiated')[:5] # Latest 5 active recalls
    
    return render_template(
        'compliance/dashboard.html',
        stats=stats,
        recall_stats=recall_stats,
        expiring_records=expiring_records,
        recent_incidents=recent_incidents,
        active_recalls=active_recalls
    )

# Compliance Records Routes
@compliance_bp.route('/records')
@login_required
def list_compliance_records():
    """List all compliance records"""
    status = request.args.get('status', 'active')
    q = request.args.get('q', '').strip()
    records = search_compliance_records(q, status) if q else get_all_compliance_records(status)
    return render_template('compliance/list_records.html', records=records, current_status=status, q=q)

@compliance_bp.route('/records/add', methods=['GET', 'POST'])
@login_required
def add_compliance_record_route():
    """Add a new compliance record"""
    if request.method == 'POST':
        record_type = request.form['record_type']
        title = request.form['title']
        description = request.form.get('description')
        certificate_number = request.form.get('certificate_number')
        issuing_authority = request.form.get('issuing_authority')
        issue_date = request.form.get('issue_date') or None
        expiration_date = request.form.get('expiration_date') or None
        
        result = add_compliance_record(
            record_type, title, description, certificate_number,
            issuing_authority, issue_date, expiration_date, 
            None, current_user.id  # file_path, created_by
        )
        
        if result:
            flash('Compliance record added successfully!', 'success')
            return redirect(url_for('compliance.list_compliance_records'))
        else:
            flash('Error adding compliance record.', 'error')
    
    return render_template('compliance/add_record.html')

@compliance_bp.route('/records/<int:record_id>')
@login_required
def view_compliance_record(record_id):
    """View details of a compliance record"""
    record = get_compliance_record_by_id(record_id)
    if not record:
        flash('Compliance record not found.', 'error')
        return redirect(url_for('compliance.list_compliance_records'))
    
    return render_template('compliance/view_record.html', record=record)

@compliance_bp.route('/records/<int:record_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_compliance_record(record_id):
    """Edit a compliance record"""
    record = get_compliance_record_by_id(record_id)
    if not record:
        flash('Compliance record not found.', 'error')
        return redirect(url_for('compliance.list_compliance_records'))
    
    if request.method == 'POST':
        updates = {
            'record_type': request.form.get('record_type'),
            'title': request.form.get('title'),
            'description': request.form.get('description'),
            'certificate_number': request.form.get('certificate_number'),
            'issuing_authority': request.form.get('issuing_authority'),
            'issue_date': request.form.get('issue_date') or None,
            'expiration_date': request.form.get('expiration_date') or None,
            'status': request.form.get('status')
        }
        
        result = update_compliance_record(record_id, **updates)
        
        if result:
            flash('Compliance record updated successfully!', 'success')
            return redirect(url_for('compliance.view_compliance_record', record_id=record_id))
        else:
            flash('Error updating compliance record.', 'error')
    
    return render_template('compliance/edit_record.html', record=record)

@compliance_bp.route('/records/<int:record_id>/delete', methods=['POST'])
@login_required
def delete_compliance_record_route(record_id):
    """Delete (soft delete) a compliance record"""
    result = delete_compliance_record(record_id)
    if result:
        flash('Compliance record deleted successfully!', 'success')
    else:
        flash('Error deleting compliance record.', 'error')
    
    return redirect(url_for('compliance.list_compliance_records'))


# Delete Food Safety Incident
@compliance_bp.route('/incidents/<int:incident_id>/delete', methods=['POST'])
@login_required
def delete_food_safety_incident_route(incident_id):
    result = delete_food_safety_incident(incident_id)
    if result:
        flash('Food safety incident deleted successfully!', 'success')
    else:
        flash('Error deleting food safety incident.', 'error')
    return redirect(url_for('compliance.list_food_safety_incidents'))

# Food Safety Incidents Routes
@compliance_bp.route('/incidents')
@login_required
def list_food_safety_incidents():
    """List all food safety incidents"""
    status = request.args.get('status')
    q = request.args.get('q', '').strip()
    incidents = search_food_safety_incidents(q, status) if q else get_all_food_safety_incidents(status)
    return render_template('compliance/list_incidents.html', incidents=incidents, current_status=status, q=q)

@compliance_bp.route('/incidents/add', methods=['GET', 'POST'])
@login_required
def add_food_safety_incident_route():
    """Add a new food safety incident"""
    if request.method == 'POST':
        incident_number = generate_incident_number()
        incident_type = request.form['incident_type']
        title = request.form['title']
        description = request.form['description']
        severity_level = request.form['severity_level']
        
        result = add_food_safety_incident(
            incident_number, incident_type, title, description,
            severity_level, current_user.id
        )
        
        if result:
            flash(f'Food safety incident {incident_number} created successfully!', 'success')
            return redirect(url_for('compliance.list_food_safety_incidents'))
        else:
            flash('Error creating food safety incident.', 'error')
    
    return render_template('compliance/add_incident.html')

@compliance_bp.route('/incidents/<int:incident_id>')
@login_required
def view_food_safety_incident(incident_id):
    """View details of a food safety incident"""
    incident = get_food_safety_incident_by_id(incident_id)
    if not incident:
        flash('Food safety incident not found.', 'error')
        return redirect(url_for('compliance.list_food_safety_incidents'))
    
    # Get associated batches
    incident_batches = get_incident_batches(incident_id)
    
    # Get available batches for adding to incident (exclude already linked ones)
    all_batches = get_all_batches()
    linked_batch_ids = {batch['batch_id'] for batch in incident_batches}
    available_batches = [b for b in all_batches if b['id'] not in linked_batch_ids]
    
    return render_template('compliance/view_incident.html', 
                         incident=incident, 
                         incident_batches=incident_batches,
                         available_batches=available_batches)

@compliance_bp.route('/incidents/<int:incident_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_food_safety_incident(incident_id):
    """Edit a food safety incident"""
    incident = get_food_safety_incident_by_id(incident_id)
    if not incident:
        flash('Food safety incident not found.', 'error')
        return redirect(url_for('compliance.list_food_safety_incidents'))
    
    if request.method == 'POST':
        updates = {
            'incident_type': request.form.get('incident_type'),
            'title': request.form.get('title'),
            'description': request.form.get('description'),
            'severity_level': request.form.get('severity_level'),
            'status': request.form.get('status'),
            'investigation_notes': request.form.get('investigation_notes'),
            'corrective_actions': request.form.get('corrective_actions'),
            'root_cause': request.form.get('root_cause')
        }
        
        # Handle status change to closed
        if updates['status'] == 'closed':
            updates['closed_by'] = current_user.id
        
        result = update_food_safety_incident(incident_id, **updates)
        
        if result:
            flash('Food safety incident updated successfully!', 'success')
            return redirect(url_for('compliance.view_food_safety_incident', incident_id=incident_id))
        else:
            flash('Error updating food safety incident.', 'error')
    
    return render_template('compliance/edit_incident.html', incident=incident)

@compliance_bp.route('/incidents/<int:incident_id>/add_batch', methods=['POST'])
@login_required
def add_batch_to_incident(incident_id):
    """Add a batch to a food safety incident"""
    batch_id = request.form['batch_id']
    involvement_level = request.form['involvement_level']
    notes = request.form.get('notes')
    
    result = add_incident_batch(incident_id, batch_id, involvement_level, notes)
    
    if result:
        flash('Batch added to incident successfully!', 'success')
    else:
        flash('Error adding batch to incident.', 'error')
    
    return redirect(url_for('compliance.view_food_safety_incident', incident_id=incident_id))


@compliance_bp.route('/incidents/<int:incident_id>/remove_batch', methods=['POST'])
@login_required
def remove_batch_from_incident(incident_id):
    """Remove a batch from a food safety incident"""
    batch_id = request.form['batch_id']
    
    result = remove_incident_batch(incident_id, batch_id)
    
    if result:
        flash('Batch removed from incident successfully!', 'success')
    else:
        flash('Error removing batch from incident.', 'error')
    
    return redirect(url_for('compliance.view_food_safety_incident', incident_id=incident_id))


# Batch Recall Routes
@compliance_bp.route('/recalls')
@login_required
def list_batch_recalls():
    """List all batch recalls"""
    status = request.args.get('status')
    q = request.args.get('q', '').strip()
    recalls = search_batch_recalls(q, status) if q else get_all_batch_recalls(status)
    return render_template('compliance/list_recalls.html', recalls=recalls, current_status=status, q=q)

@compliance_bp.route('/recalls/initiate', methods=['GET', 'POST'])
@login_required
def initiate_batch_recall():
    """Initiate a new batch recall"""
    if request.method == 'POST':
        recall_number = generate_recall_number()
        title = request.form['title']
        reason = request.form['reason']
        severity_level = request.form['severity_level']
        notes = request.form.get('notes')
        
        # Create the recall
        recall = add_batch_recall(recall_number, title, reason, severity_level, current_user.id, notes)
        
        if recall:
            # Add selected batches to the recall
            batch_ids = request.form.getlist('batch_ids')
            for batch_id in batch_ids:
                quantity_affected = request.form.get(f'quantity_{batch_id}')
                batch_notes = request.form.get(f'notes_{batch_id}')
                add_recall_batch(recall['id'], batch_id, quantity_affected, batch_notes)
            
            flash(f'Batch recall {recall_number} initiated successfully!', 'success')
            return redirect(url_for('compliance.view_batch_recall', recall_id=recall['id']))
        else:
            flash('Error initiating batch recall.', 'error')
    
    # For GET request, provide batch search functionality
    search_results = []
    if request.args.get('search'):
        search_criteria = {
            'supplier_id': request.args.get('supplier_id'),
            'product_id': request.args.get('product_id'),
            'arrival_date_from': request.args.get('arrival_date_from'),
            'arrival_date_to': request.args.get('arrival_date_to'),
            'batch_number_pattern': request.args.get('batch_number_pattern')
        }
        # Remove None values
        search_criteria = {k: v for k, v in search_criteria.items() if v}
        search_results = search_batches_for_recall(search_criteria)
    
    suppliers = get_all_suppliers()
    products = get_all_products()
    
    return render_template('compliance/initiate_recall.html',
                         suppliers=suppliers,
                         products=products,
                         search_results=search_results)

@compliance_bp.route('/recalls/<int:recall_id>')
@login_required
def view_batch_recall(recall_id):
    """View details of a batch recall"""
    recall = get_recall_by_id(recall_id)
    if not recall:
        flash('Batch recall not found.', 'error')
        return redirect(url_for('compliance.list_batch_recalls'))
    
    recall_batches = get_recall_batches(recall_id)
    impact_summary = get_recall_impact_summary(recall_id)
    
    return render_template('compliance/view_recall.html',
                         recall=recall,
                         recall_batches=recall_batches,
                         impact_summary=impact_summary)

@compliance_bp.route('/recalls/<int:recall_id>/update_status', methods=['POST'])
@login_required
def update_batch_recall_status(recall_id):
    """Update the status of a batch recall"""
    status = request.form['status']
    notes = request.form.get('notes')
    
    result = update_recall_status(recall_id, status, notes)
    
    if result:
        flash(f'Recall status updated to {status}!', 'success')
    else:
        flash('Error updating recall status.', 'error')
    
    return redirect(url_for('compliance.view_batch_recall', recall_id=recall_id))

@compliance_bp.route('/recalls/<int:recall_id>/notifications', methods=['POST'])
@login_required
def update_recall_notifications(recall_id):
    """Update notification status for a recall"""
    customer_sent = request.form.get('customer_notification') == 'on'
    regulatory_sent = request.form.get('regulatory_notification') == 'on'
    
    result = update_recall_notifications(recall_id, customer_sent, regulatory_sent)
    
    if result:
        flash('Notification status updated!', 'success')
    else:
        flash('Error updating notification status.', 'error')
    
    return redirect(url_for('compliance.view_batch_recall', recall_id=recall_id))


# Safe delete for recalls: cancel instead of removing to preserve traceability history
@compliance_bp.route('/recalls/<int:recall_id>/delete', methods=['POST'])
@login_required
def delete_batch_recall_route(recall_id):
    """Cancel a recall (soft delete) to keep history visible in traceability and batch views."""
    print(f"DEBUG: Cancel recall route called for recall {recall_id}")
    try:
        result = update_recall_status(recall_id, 'cancelled', notes='Cancelled via delete action')
        if result:
            flash('Recall cancelled successfully (history preserved).', 'success')
        else:
            flash('Error cancelling recall.', 'error')
    except Exception as e:
        print(f"DEBUG: Error in cancel recall route: {str(e)}")
        flash(f'Error cancelling recall: {str(e)}', 'error')
    return redirect(url_for('compliance.list_batch_recalls'))

# Complete recall removal with quantity restoration
@compliance_bp.route('/recalls/<int:recall_id>/delete_completely', methods=['POST'])
@login_required
def delete_recall_completely_route(recall_id):
    """Completely delete a recall and restore all batch quantities."""
    try:
        result = delete_recall_completely(recall_id)
        if result:
            flash('Recall completely deleted and batch quantities restored!', 'success')
        else:
            flash('Error completely deleting recall.', 'error')
    except Exception as e:
        flash(f'Error deleting recall: {str(e)}', 'error')
    return redirect(url_for('compliance.list_batch_recalls'))

@compliance_bp.route('/recalls/batch/<int:recall_batch_id>/recovery', methods=['POST'])
@login_required
def update_batch_recovery(recall_batch_id):
    """Update recovery details of a recalled batch"""
    try:
        # Collect all possible update fields
        update_data = {}
        
        print(f"DEBUG: Updating recall batch {recall_batch_id}")
        print(f"DEBUG: Form data: {dict(request.form)}")
        
        if 'recovery_status' in request.form and request.form['recovery_status']:
            update_data['recovery_status'] = request.form['recovery_status']
        
        if 'quantity_affected' in request.form and request.form['quantity_affected']:
            try:
                update_data['quantity_affected'] = float(request.form['quantity_affected'])
            except (ValueError, TypeError):
                flash('Invalid quantity value.', 'error')
                return redirect(request.referrer or url_for('compliance.list_batch_recalls'))
        
        if 'recovery_date' in request.form and request.form['recovery_date']:
            update_data['recovery_date'] = request.form['recovery_date']
        
        if 'notes' in request.form:
            update_data['notes'] = request.form['notes']
        
        print(f"DEBUG: Update data: {update_data}")
        
        # Use the comprehensive update function
        result = update_batch_recovery_details(recall_batch_id, **update_data)
        
        if result:
            # Create a more specific success message
            updated_fields = []
            if 'recovery_status' in update_data:
                updated_fields.append('status')
            if 'quantity_affected' in update_data:
                updated_fields.append('quantity')
            if 'recovery_date' in update_data:
                updated_fields.append('date')
            if 'notes' in update_data:
                updated_fields.append('notes')
            
            field_text = ', '.join(updated_fields) if updated_fields else 'details'
            flash(f'Batch recovery {field_text} updated successfully!', 'success')
        else:
            flash('Error updating batch recovery details.', 'error')
    
    except Exception as e:
        print(f"DEBUG: Exception in update_batch_recovery: {str(e)}")
        flash(f'Error updating batch recovery details: {str(e)}', 'error')
    
    return redirect(request.referrer or url_for('compliance.list_batch_recalls'))

# Compliance Audits Routes
@compliance_bp.route('/audits')
@login_required
def list_compliance_audits():
    """List all compliance audits"""
    q = request.args.get('q', '').strip()
    audits = search_compliance_audits(q) if q else get_all_compliance_audits()
    return render_template('compliance/list_audits.html', audits=audits, q=q)

@compliance_bp.route('/audits/add', methods=['GET', 'POST'])
@login_required
def add_compliance_audit_route():
    """Add a new compliance audit"""
    if request.method == 'POST':
        audit_type = request.form['audit_type']
        auditor_name = request.form['auditor_name']
        audit_date = request.form['audit_date']
        scope = request.form.get('scope')
        findings = request.form.get('findings')
        recommendations = request.form.get('recommendations')
        overall_rating = request.form.get('overall_rating')
        
        result = add_compliance_audit(
            audit_type, auditor_name, audit_date, current_user.id,
            scope, findings, recommendations, overall_rating
        )
        
        if result:
            flash('Compliance audit added successfully!', 'success')
            return redirect(url_for('compliance.list_compliance_audits'))
        else:
            flash('Error adding compliance audit.', 'error')
    
    return render_template('compliance/add_audit.html')

@compliance_bp.route('/audits/<int:audit_id>')
@login_required
def view_compliance_audit(audit_id):
    """View details of a compliance audit"""
    audit = get_compliance_audit_by_id(audit_id)
    if not audit:
        flash('Compliance audit not found.', 'error')
        return redirect(url_for('compliance.list_compliance_audits'))
    
    return render_template('compliance/view_audit.html', audit=audit)

@compliance_bp.route('/audits/<int:audit_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_compliance_audit(audit_id):
    """Edit a compliance audit"""
    audit = get_compliance_audit_by_id(audit_id)
    if not audit:
        flash('Compliance audit not found.', 'error')
        return redirect(url_for('compliance.list_compliance_audits'))
    
    if request.method == 'POST':
        # Collect form data
        update_data = {}
        
        if 'audit_type' in request.form:
            update_data['audit_type'] = request.form['audit_type']
        if 'auditor_name' in request.form:
            update_data['auditor_name'] = request.form['auditor_name']
        if 'audit_date' in request.form:
            update_data['audit_date'] = request.form['audit_date']
        if 'scope' in request.form:
            update_data['scope'] = request.form['scope']
        if 'findings' in request.form:
            update_data['findings'] = request.form['findings']
        if 'recommendations' in request.form:
            update_data['recommendations'] = request.form['recommendations']
        if 'overall_rating' in request.form:
            update_data['overall_rating'] = request.form['overall_rating']
        if 'follow_up_required' in request.form:
            update_data['follow_up_required'] = request.form['follow_up_required'] == 'on'
        if 'follow_up_date' in request.form and request.form['follow_up_date']:
            update_data['follow_up_date'] = request.form['follow_up_date']
        
        result = update_compliance_audit(audit_id, **update_data)
        
        if result:
            flash('Compliance audit updated successfully!', 'success')
            return redirect(url_for('compliance.view_compliance_audit', audit_id=audit_id))
        else:
            flash('Error updating compliance audit.', 'error')
    
    return render_template('compliance/edit_audit.html', audit=audit)


# Delete Compliance Audit
@compliance_bp.route('/audits/<int:audit_id>/delete', methods=['POST'])
@login_required
def delete_compliance_audit_route(audit_id):
    result = delete_compliance_audit(audit_id)
    if result:
        flash('Compliance audit deleted successfully!', 'success')
    else:
        flash('Error deleting compliance audit.', 'error')
    return redirect(url_for('compliance.list_compliance_audits'))

# API Routes for AJAX functionality
@compliance_bp.route('/api/batch/<int:batch_id>/recall_history')
@login_required
def get_batch_recall_history_api(batch_id):
    """API endpoint to get recall history for a batch (for traceability integration)"""
    history = get_batch_recall_history(batch_id)
    return jsonify(history)

@compliance_bp.route('/api/dashboard_stats')
@login_required
def get_dashboard_stats_api():
    """API endpoint for dashboard statistics"""
    stats = get_compliance_dashboard_stats()
    recall_stats = get_recall_statistics()
    
    return jsonify({
        'compliance': stats,
        'recalls': recall_stats
    })

@compliance_bp.route('/api/search_batches', methods=['POST'])
@login_required
def search_batches_api():
    """API endpoint for batch search (used in recall initiation)"""
    search_criteria = request.get_json()
    results = search_batches_for_recall(search_criteria)
    
    # Format results for JSON response
    formatted_results = []
    for batch in results:
        formatted_results.append({
            'id': batch['id'],
            'batch_number': batch['batch_number'],
            'product_name': batch['product_name'],
            'supplier_name': batch['supplier_name'],
            'arrival_date': batch['arrival_date'].isoformat() if hasattr(batch['arrival_date'], 'isoformat') else str(batch['arrival_date']),
            'quantity': float(batch['quantity']) if batch['quantity'] else 0,
            'animal_type': batch['animal_type'],
            'cut_type': batch['cut_type']
        })
    
    return jsonify(formatted_results)