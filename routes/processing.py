from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from decimal import Decimal
from database import (
    get_all_processing_sessions, get_processing_session_by_id, add_processing_session,
    get_processing_inputs_for_session, add_processing_input,
    get_processing_outputs_for_session, add_processing_output,
    get_all_products, get_all_batches, get_batch_by_id, update_batch_quantity,
    delete_processing_session
)

processing_bp = Blueprint('processing', __name__, template_folder='templates')

@processing_bp.route('/sessions')
@login_required
def list_sessions():
    sessions = get_all_processing_sessions()
    return render_template('processing/list_sessions.html', sessions=sessions)

@processing_bp.route('/sessions/<int:id>')
@login_required
def view_session(id):
    session = get_processing_session_by_id(id)
    inputs = get_processing_inputs_for_session(id)
    outputs = get_processing_outputs_for_session(id)
    
    # Data for modals
    all_products = get_all_products()
    available_batches = get_all_batches()

    total_input_weight = sum(i['quantity_used'] for i in inputs)
    total_output_weight = sum(o['weight'] for o in outputs)
    
    yield_percentage = 0
    if total_input_weight > 0:
        yield_percentage = (total_output_weight / total_input_weight) * 100

    return render_template(
        'processing/view_session.html', 
        session=session, 
        inputs=inputs, 
        outputs=outputs,
        total_input_weight=total_input_weight,
        total_output_weight=total_output_weight,
        yield_percentage=yield_percentage,
        all_products=all_products,
        available_batches=available_batches
    )

@processing_bp.route('/sessions/add', methods=['GET', 'POST'])
@login_required
def add_session():
    if request.method == 'POST':
        session_name = request.form['session_name']
        session_date = request.form['session_date']
        notes = request.form['notes']
        add_processing_session(session_name, session_date, notes)
        flash('Processing session added successfully!', 'success')
        return redirect(url_for('processing.list_sessions'))
    return render_template('processing/add_session.html')

@processing_bp.route('/sessions/<int:id>/add_input', methods=['POST'])
@login_required
def add_input(id):
    batch_id = request.form['batch_id']
    quantity_used = Decimal(request.form['quantity_used'])
    
    batch = get_batch_by_id(batch_id)
    if quantity_used <= 0:
        flash('Error: Quantity used must be a positive number.', 'error')
        return redirect(url_for('processing.view_session', id=id))

    if quantity_used > batch['quantity']:
        flash(f"Error: Quantity used ({quantity_used}) cannot be greater than the available quantity in the batch ({batch['quantity']}).", 'error')
        return redirect(url_for('processing.view_session', id=id))

    add_processing_input(id, batch_id, quantity_used)
    # Update batch quantity: reduce by the quantity used (negative value to decrease)
    update_batch_quantity(batch_id, -quantity_used)
    
    flash('Processing input added successfully!', 'success')
    return redirect(url_for('processing.view_session', id=id))

@processing_bp.route('/sessions/<int:id>/add_output', methods=['POST'])
@login_required
def add_output(id):
    inputs = get_processing_inputs_for_session(id)
    if not inputs:
        flash('Error: Cannot add an output before adding at least one input.', 'error')
        return redirect(url_for('processing.view_session', id=id))

    product_id = request.form['product_id']
    output_type = request.form['output_type']
    weight = Decimal(request.form['weight'])

    if weight <= 0:
        flash('Error: Weight must be a positive number.', 'error')
        return redirect(url_for('processing.view_session', id=id))

    outputs = get_processing_outputs_for_session(id)
    total_input_weight = sum(i['quantity_used'] for i in inputs)
    total_output_weight = sum(o['weight'] for o in outputs)

    if (total_output_weight + weight) > total_input_weight:
        flash(f"Error: Total output weight ({total_output_weight + weight}) cannot exceed total input weight ({total_input_weight}).", 'error')
        return redirect(url_for('processing.view_session', id=id))

    add_processing_output(id, product_id, output_type, weight)
    flash('Processing output added successfully!', 'success')
    return redirect(url_for('processing.view_session', id=id))

@processing_bp.route('/sessions/<int:id>/delete', methods=['POST'])
@login_required
def delete_session(id):
    """Delete a processing session"""
    session = get_processing_session_by_id(id)
    if not session:
        flash('Error: Processing session not found.', 'error')
        return redirect(url_for('processing.list_sessions'))
    
    # Check if session has any inputs or outputs
    inputs = get_processing_inputs_for_session(id)
    outputs = get_processing_outputs_for_session(id)
    
    if inputs:
        flash(f'Note: Deleting this session will restore {len(inputs)} batch quantities to inventory.', 'info')
    
    if outputs:
        flash('Warning: This session has processing outputs that will be permanently removed.', 'warning')
    
    result = delete_processing_session(id)
    
    if result:
        if inputs:
            flash('Processing session deleted successfully! Batch quantities have been restored to inventory.', 'success')
        else:
            flash('Processing session deleted successfully!', 'success')
    else:
        flash('Error: Failed to delete processing session.', 'error')
    
    return redirect(url_for('processing.list_sessions'))
