from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from database import (
    add_outbound_shipment, get_all_shipments, get_shipment_by_id, update_shipment_status,
    add_shipment_line, get_shipment_lines, get_shipment_line_by_id, update_shipment_line_quantity, delete_shipment_line, delete_shipment_lines, delete_outbound_shipment,
    record_restorations, get_restorations, clear_restorations,
    get_current_stock_by_product, get_restock_suggestions, get_picklist,
    get_reorder_rules, get_reorder_rule_by_id, add_reorder_rule, update_reorder_rule, delete_reorder_rule, get_product_current_stock,
    get_all_products
)
from database.batch_queries import update_batch_quantity


distribution_bp = Blueprint('distribution', __name__, template_folder='templates')


@distribution_bp.route('/shipments')
@login_required
def list_shipments():
    status = request.args.get('status')
    shipments = get_all_shipments(status)
    return render_template('distribution/list_shipments.html', shipments=shipments, current_status=status)


@distribution_bp.route('/shipments/add', methods=['GET', 'POST'])
@login_required
def add_shipment():
    if request.method == 'POST':
        shipment_number = request.form['shipment_number']
        destination_name = request.form['destination_name']
        destination_type = request.form['destination_type']
        scheduled_date = request.form.get('scheduled_date')
        notes = request.form.get('notes')
        res = add_outbound_shipment(
            shipment_number, destination_name, destination_type, scheduled_date, current_user.id, 'planned', notes
        )
        if res is not None:
            flash('Shipment created', 'success')
            shipments = get_all_shipments()
            ship = next((s for s in (shipments or []) if s.get('shipment_number') == shipment_number), None)
            return redirect(url_for('distribution.view_shipment', shipment_id=ship['id'])) if ship else redirect(url_for('distribution.list_shipments'))
        flash('Error creating shipment', 'error')
    return render_template('distribution/add_shipment.html')


@distribution_bp.route('/shipments/<int:shipment_id>', methods=['GET', 'POST'])
@login_required
def view_shipment(shipment_id):
    shipment = get_shipment_by_id(shipment_id)
    if not shipment:
        flash('Shipment not found', 'error')
        return redirect(url_for('distribution.list_shipments'))
    lines = get_shipment_lines(shipment_id) or []
    products = get_all_products() or []
    can_allocate = shipment.get('status') == 'planned'
    q_product_id = request.args.get('product_id')
    q_qty = request.args.get('qty')
    q_strategy = request.args.get('strategy', 'FIFO')
    pick = None
    if can_allocate and q_product_id and q_qty:
        try:
            pick = get_picklist(int(q_product_id), float(q_qty), q_strategy)
        except Exception:
            pick = None

    if request.method == 'POST' and request.form.get('action') == 'add_line':
        if not can_allocate:
            flash(f"Cannot add lines when shipment status is '{shipment.get('status')}'.", 'error')
            return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
        try:
            product_id = int(request.form['product_id'])
            qty = float(request.form['qty'])
            strategy = request.form.get('strategy', 'FIFO')
            allocation = get_picklist(product_id, qty, strategy)
            if allocation and allocation.get('allocations'):
                for a in allocation['allocations']:
                    add_shipment_line(shipment_id, a['batch_id'], a['quantity'], strategy)
                    update_batch_quantity(a['batch_id'], -a['quantity'])  # Negative to decrease
                flash('Shipment lines added and stock decremented', 'success')
                return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
            else:
                flash('Insufficient stock to allocate requested quantity', 'error')
        except Exception as e:
            flash(f'Error adding shipment lines: {e}', 'error')

    return render_template(
        'distribution/view_shipment.html', shipment=shipment, lines=lines, products=products, pick=pick, strategy=q_strategy, can_allocate=can_allocate
    )


@distribution_bp.route('/shipments/<int:shipment_id>/status', methods=['POST'])
@login_required
def update_shipment_status_route(shipment_id):
    status = request.form['status']
    notes = request.form.get('notes')
    # If cancelling, reverse any committed lines by re-adding stock and removing lines (idempotent)
    if status == 'cancelled':
        shipment = get_shipment_by_id(shipment_id)
        if shipment and shipment.get('status') != 'cancelled':
            lines = get_shipment_lines(shipment_id) or []
            restored_total = 0.0
            for l in lines:
                # Re-add quantity to the batch
                try:
                    qty = float(l['quantity_shipped']) if l.get('quantity_shipped') is not None else 0.0
                    update_batch_quantity(l['batch_id'], qty)  # Positive to increase (restore)
                    restored_total += qty
                except Exception:
                    pass
            # Delete lines after restoring stock
            delete_shipment_lines(shipment_id)
            # Persist for potential re-application
            record_restorations(shipment_id, [{'batch_id': l['batch_id'], 'quantity': float(l['quantity_shipped'])} for l in lines])
            flash(f"Cancelled shipment: restored {restored_total:.2f} units back to inventory.", 'success')
    elif status == 'planned':
        # If moving back to planned, re-apply any saved restorations by re-deducting and re-creating lines
        prior_restores = get_restorations(shipment_id) or []
        reapplied_total = 0.0
        for r in prior_restores:
            try:
                qty = float(r['quantity']) if r.get('quantity') is not None else 0.0
                # Deduct back from batches
                update_batch_quantity(r['batch_id'], -qty)  # Negative to decrease
                # Recreate shipment line with strategy FIFO by default
                add_shipment_line(shipment_id, r['batch_id'], qty, 'FIFO')
                reapplied_total += qty
            except Exception:
                pass
        if prior_restores:
            clear_restorations(shipment_id)
            flash(f"Reapplied {reapplied_total:.2f} units to shipment lines.", 'success')
    ok = update_shipment_status(shipment_id, status, notes)
    flash('Status updated' if ok is not None else 'Failed to update status', 'success' if ok is not None else 'error')
    return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))


@distribution_bp.route('/shipments/<int:shipment_id>/lines/<int:line_id>/update', methods=['POST'])
@login_required
def update_line(shipment_id, line_id):
    shipment = get_shipment_by_id(shipment_id)
    if not shipment or shipment.get('status') != 'planned':
        flash('Lines can only be edited while shipment is Planned.', 'error')
        return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
    line = get_shipment_line_by_id(line_id)
    if not line:
        flash('Line not found', 'error')
        return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
    try:
        new_qty = float(request.form['new_qty'])
        old_qty = float(line['quantity_shipped']) if line.get('quantity_shipped') is not None else 0.0
        delta = new_qty - old_qty
        # positive delta means we need to take more from batch; negative means return to batch
        if delta > 0:
            update_batch_quantity(line['batch_id'], -delta)  # Negative to decrease
        elif delta < 0:
            update_batch_quantity(line['batch_id'], -delta)  # Negative delta becomes positive to increase
        update_shipment_line_quantity(line_id, new_qty)
        flash('Line updated', 'success')
    except Exception as e:
        flash(f'Failed to update line: {e}', 'error')
    return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))


@distribution_bp.route('/shipments/<int:shipment_id>/lines/<int:line_id>/delete', methods=['POST'])
@login_required
def delete_line(shipment_id, line_id):
    shipment = get_shipment_by_id(shipment_id)
    if not shipment or shipment.get('status') != 'planned':
        flash('Lines can only be deleted while shipment is Planned.', 'error')
        return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
    line = get_shipment_line_by_id(line_id)
    if not line:
        flash('Line not found', 'error')
        return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))
    try:
        qty = float(line['quantity_shipped']) if line.get('quantity_shipped') is not None else 0.0
        # Return qty to batch then delete line
        update_batch_quantity(line['batch_id'], qty)  # Positive to increase (restore)
        delete_shipment_line(line_id)
        flash('Line deleted and stock restored', 'success')
    except Exception as e:
        flash(f'Failed to delete line: {e}', 'error')
    return redirect(url_for('distribution.view_shipment', shipment_id=shipment_id))


@distribution_bp.route('/restock')
@login_required
def restock_suggestions():
    stock = get_current_stock_by_product() or []
    suggestions = get_restock_suggestions() or []
    return render_template('distribution/restock.html', stock=stock, suggestions=suggestions)


# Reorder Rules Management
@distribution_bp.route('/reorder-rules')
@login_required
def list_reorder_rules():
    q = request.args.get('q', '').strip()
    rules = get_reorder_rules(q) if q else get_reorder_rules()
    return render_template('distribution/reorder_rules.html', rules=rules, q=q)


@distribution_bp.route('/reorder-rules/add', methods=['GET', 'POST'])
@login_required
def add_reorder_rule_route():
    products = get_all_products() or []
    if request.method == 'POST':
        product_id = int(request.form['product_id'])
        min_qty = float(request.form['min_qty'])
        target_qty = float(request.form['target_qty'])
        active = request.form.get('active') == 'on'
        
        # Get current stock for validation
        current_stock = get_product_current_stock(product_id)
        product_name = next((p.get('name', 'Unknown') for p in products if p.get('id') == product_id), 'Unknown')
        
        # Validation: min_qty should be greater than current stock for meaningful reorder rules
        if min_qty <= current_stock:
            flash(f'Warning: Minimum quantity ({min_qty:.2f}) should be greater than current stock ({current_stock:.2f}) for "{product_name}". Otherwise, no restock suggestions will be generated.', 'error')
            return render_template('distribution/rule_form.html', products=products, rule=None)
        
        # Validation: target_qty should be greater than min_qty
        if target_qty <= min_qty:
            flash(f'Error: Target quantity ({target_qty:.2f}) must be greater than minimum quantity ({min_qty:.2f}).', 'error')
            return render_template('distribution/rule_form.html', products=products, rule=None)
        
        ok = add_reorder_rule(product_id, min_qty, target_qty, active)
        flash('Rule added' if ok is not None else 'Failed to add rule', 'success' if ok is not None else 'error')
        return redirect(url_for('distribution.list_reorder_rules'))
    return render_template('distribution/rule_form.html', products=products, rule=None)


@distribution_bp.route('/reorder-rules/<int:rule_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_reorder_rule_route(rule_id):
    rule = get_reorder_rule_by_id(rule_id)
    if not rule:
        flash('Rule not found', 'error')
        return redirect(url_for('distribution.list_reorder_rules'))
    products = get_all_products() or []
    if request.method == 'POST':
        product_id = int(request.form['product_id'])
        min_qty = float(request.form['min_qty'])
        target_qty = float(request.form['target_qty'])
        active = request.form.get('active') == 'on'
        
        # Get current stock for validation
        current_stock = get_product_current_stock(product_id)
        product_name = next((p.get('name', 'Unknown') for p in products if p.get('id') == product_id), 'Unknown')
        
        # Validation: min_qty should be greater than current stock for meaningful reorder rules
        if min_qty <= current_stock:
            flash(f'Warning: Minimum quantity ({min_qty:.2f}) should be greater than current stock ({current_stock:.2f}) for "{product_name}". Otherwise, no restock suggestions will be generated.', 'error')
            return render_template('distribution/rule_form.html', products=products, rule=rule)
        
        # Validation: target_qty should be greater than min_qty
        if target_qty <= min_qty:
            flash(f'Error: Target quantity ({target_qty:.2f}) must be greater than minimum quantity ({min_qty:.2f}).', 'error')
            return render_template('distribution/rule_form.html', products=products, rule=rule)
        
        ok = update_reorder_rule(rule_id, product_id=product_id, min_qty=min_qty, target_qty=target_qty, active=active)
        flash('Rule updated' if ok is not None else 'Failed to update rule', 'success' if ok is not None else 'error')
        return redirect(url_for('distribution.list_reorder_rules'))
    return render_template('distribution/rule_form.html', products=products, rule=rule)


@distribution_bp.route('/reorder-rules/<int:rule_id>/delete', methods=['POST'])
@login_required
def delete_reorder_rule_route(rule_id):
    ok = delete_reorder_rule(rule_id)
    flash('Rule deleted' if ok is not None else 'Failed to delete rule', 'success' if ok is not None else 'error')
    return redirect(url_for('distribution.list_reorder_rules'))


@distribution_bp.route('/product-stock/<int:product_id>')
@login_required
def get_product_stock_api(product_id):
    """API endpoint to get current stock for a product (for AJAX calls)"""
    from flask import jsonify
    stock = get_product_current_stock(product_id)
    return jsonify({'stock': stock})


@distribution_bp.route('/shipments/<int:shipment_id>/delete', methods=['POST'])
@login_required
def delete_shipment(shipment_id):
    shipment = get_shipment_by_id(shipment_id)
    if not shipment:
        flash('Shipment not found', 'error')
        return redirect(url_for('distribution.list_shipments'))
    # If not cancelled, restore stock before deletion
    if shipment.get('status') != 'cancelled':
        lines = get_shipment_lines(shipment_id) or []
        for l in lines:
            try:
                update_batch_quantity(l['batch_id'], float(l['quantity_shipped']))  # Positive to increase (restore)
            except Exception:
                pass
        delete_shipment_lines(shipment_id)
    else:
        # ensure lines are gone to avoid orphan logic
        delete_shipment_lines(shipment_id)
    ok = delete_outbound_shipment(shipment_id)
    flash('Shipment deleted' if ok is not None else 'Failed to delete shipment', 'success' if ok is not None else 'error')
    return redirect(url_for('distribution.list_shipments'))


