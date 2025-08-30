
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from database import (
    get_all_storage_locations,
    get_all_suppliers, add_supplier, get_supplier_by_id, update_supplier, delete_supplier, search_suppliers,
    get_all_products, add_product, get_product_by_id, update_product, delete_product, search_products,
    get_all_batches, add_batch, get_batch_by_id, update_batch, delete_batch, search_batches,
    get_batch_compliance_status, log_activity
)
from database.recall_queries import remove_batch_from_all_recalls

inventory_bp = Blueprint('inventory', __name__, template_folder='templates')

# Supplier Routes
@inventory_bp.route('/suppliers')
@login_required
def list_suppliers():
    q = request.args.get('q', '').strip()
    suppliers = search_suppliers(q) if q else get_all_suppliers()
    return render_template('inventory/list_suppliers.html', suppliers=suppliers, q=q)

@inventory_bp.route('/suppliers/add', methods=['GET', 'POST'])
@login_required
def add_supplier_route():
    if request.method == 'POST':
        name = request.form['name']
        contact_person = request.form['contact_person']
        phone = request.form['phone']
        email = request.form['email']
        address = request.form['address']
        add_supplier(name, contact_person, phone, email, address)
        flash('Supplier added successfully!', 'success')
        return redirect(url_for('inventory.list_suppliers'))
    return render_template('inventory/add_supplier.html')

@inventory_bp.route('/suppliers/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_supplier(id):
    supplier = get_supplier_by_id(id)
    if request.method == 'POST':
        name = request.form['name']
        contact_person = request.form['contact_person']
        phone = request.form['phone']
        email = request.form['email']
        address = request.form['address']
        update_supplier(id, name, contact_person, phone, email, address)
        flash('Supplier updated successfully!', 'success')
        return redirect(url_for('inventory.list_suppliers'))
    return render_template('inventory/edit_supplier.html', supplier=supplier)

@inventory_bp.route('/suppliers/delete/<int:id>', methods=['POST'])
@login_required
def delete_supplier_route(id):
    delete_supplier(id)
    flash('Supplier deleted successfully!', 'success')
    return redirect(url_for('inventory.list_suppliers'))

# Product Routes
@inventory_bp.route('/products')
@login_required
def list_products():
    q = request.args.get('q', '').strip()
    products = search_products(q) if q else get_all_products()
    return render_template('inventory/list_products.html', products=products, q=q)

@inventory_bp.route('/products/add', methods=['GET', 'POST'])
@login_required
def add_product_route():
    if request.method == 'POST':
        name = request.form['name']
        animal_type = request.form['animal_type']
        cut_type = request.form['cut_type']
        processing_date = request.form['processing_date']
        storage_requirements = request.form['storage_requirements']
        shelf_life = request.form['shelf_life']
        packaging_details = request.form['packaging_details']
        supplier_id = request.form['supplier_id']
        add_product(name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id)
        log_activity(current_user.id, 'add_product', f'Added product: {name}', request.remote_addr)
        flash('Product added successfully!', 'success')
        return redirect(url_for('inventory.list_products'))
    
    suppliers = get_all_suppliers()
    return render_template('inventory/add_product.html', suppliers=suppliers)

@inventory_bp.route('/products/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_product(id):
    product = get_product_by_id(id)
    if request.method == 'POST':
        name = request.form['name']
        animal_type = request.form['animal_type']
        cut_type = request.form['cut_type']
        processing_date = request.form['processing_date']
        storage_requirements = request.form['storage_requirements']
        shelf_life = request.form['shelf_life']
        packaging_details = request.form['packaging_details']
        supplier_id = request.form['supplier_id']
        # Get old product data for change tracking
        old_product = get_product_by_id(id)
        
        update_product(id, name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id)
        
        # Log detailed changes
        changes = []
        if old_product['name'] != name:
            changes.append(f"Name: '{old_product['name']}' → '{name}'")
        if old_product['animal_type'] != animal_type:
            changes.append(f"Animal Type: '{old_product['animal_type']}' → '{animal_type}'")
        if str(old_product['shelf_life']) != shelf_life:
            changes.append(f"Shelf Life: {old_product['shelf_life']} → {shelf_life} days")
        
        change_summary = "; ".join(changes) if changes else "No significant changes"
        log_activity(current_user.id, 'update_product', f'Updated product "{name}" (ID: {id}) - {change_summary}', request.remote_addr)
        
        flash('Product updated successfully!', 'success')
        return redirect(url_for('inventory.list_products'))

    suppliers = get_all_suppliers()
    return render_template('inventory/edit_product.html', product=product, suppliers=suppliers)

@inventory_bp.route('/products/delete/<int:id>', methods=['POST'])
@login_required
def delete_product_route(id):
    try:
        # Get product info before deletion for logging
        product = get_product_by_id(id)
        product_name = product['name'] if product else f"ID:{id}"
        
        delete_product(id)
        log_activity(current_user.id, 'delete_product', f'🗑️ DELETED product "{product_name}" (ID: {id})', request.remote_addr)
        flash('Product deleted successfully!', 'success')
    except Exception as e:
        log_activity(current_user.id, 'delete_product_failed', f'❌ FAILED to delete product ID: {id} - {str(e)}', request.remote_addr)
        flash(f'Error deleting product: {str(e)}', 'error')
    return redirect(url_for('inventory.list_products'))

# Batch Routes
@inventory_bp.route('/batches')
@login_required
def list_batches():
    q = request.args.get('q', '').strip()
    batches = search_batches(q) if q else get_all_batches()
    
    # Add compliance information to each batch
    for batch in batches:
        batch['compliance'] = get_batch_compliance_status(batch['id'])
    
    return render_template('inventory/list_batches.html', batches=batches, q=q)

@inventory_bp.route('/batches/add', methods=['GET', 'POST'])
@login_required
def add_batch_route():
    if request.method == 'POST':
        product_id = request.form['product_id']
        batch_number = request.form['batch_number']
        quantity = request.form['quantity']
        arrival_date = request.form['arrival_date']
        expiration_date = request.form['expiration_date']
        storage_location = request.form['storage_location']
        add_batch(product_id, batch_number, quantity, arrival_date, expiration_date, storage_location)
        log_activity(current_user.id, 'add_batch', f'Added batch: {batch_number} (Qty: {quantity})', request.remote_addr)
        flash('Batch added successfully!', 'success')
        return redirect(url_for('inventory.list_batches'))

    products = get_all_products()
    storage_locations = get_all_storage_locations()
    return render_template('inventory/add_batch.html', products=products, storage_locations=storage_locations)

@inventory_bp.route('/batches/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_batch(id):
    batch = get_batch_by_id(id)
    if request.method == 'POST':
        product_id = request.form['product_id']
        batch_number = request.form['batch_number']
        quantity = request.form['quantity']
        arrival_date = request.form['arrival_date']
        expiration_date = request.form['expiration_date']
        storage_location = request.form['storage_location']
        
        # Track changes for detailed logging
        changes = []
        if batch['batch_number'] != batch_number:
            changes.append(f"Batch #: '{batch['batch_number']}' → '{batch_number}'")
        if str(batch['quantity']) != quantity:
            changes.append(f"Quantity: {batch['quantity']} → {quantity}")
        if str(batch['storage_location']) != storage_location:
            changes.append(f"Storage: '{batch['storage_location']}' → '{storage_location}'")
        if str(batch['expiration_date']) != expiration_date:
            changes.append(f"Expiry: {batch['expiration_date']} → {expiration_date}")
            
        update_batch(id, product_id, batch_number, quantity, arrival_date, expiration_date, storage_location)
        
        # Log detailed changes
        change_summary = "; ".join(changes) if changes else "No significant changes"
        log_activity(current_user.id, 'update_batch', f'Updated batch "{batch_number}" (ID: {id}) - {change_summary}', request.remote_addr)
        
        flash('Batch updated successfully!', 'success')
        return redirect(url_for('inventory.list_batches'))

    products = get_all_products()
    storage_locations = get_all_storage_locations()
    return render_template('inventory/edit_batch.html', batch=batch, products=products, storage_locations=storage_locations)

@inventory_bp.route('/batches/delete/<int:id>', methods=['POST'])
@login_required
def delete_batch_route(id):
    try:
        # Get batch info before deletion for logging
        batch = get_batch_by_id(id)
        batch_info = f"#{batch['batch_number']} (Qty: {batch['quantity']})" if batch else f"ID:{id}"
        
        print(f"DEBUG: Attempting to delete batch {id}")
        result = delete_batch(id)
        print(f"DEBUG: Delete result: {result}")
        
        log_activity(current_user.id, 'delete_batch', f'🗑️ DELETED batch {batch_info} (ID: {id})', request.remote_addr)
        flash('Batch deleted successfully!', 'success')
    except Exception as e:
        print(f"DEBUG: Delete failed with error: {str(e)}")
        log_activity(current_user.id, 'delete_batch_failed', f'❌ FAILED to delete batch ID: {id} - {str(e)}', request.remote_addr)
        flash(f'Error deleting batch: {str(e)}', 'error')
    return redirect(url_for('inventory.list_batches'))

@inventory_bp.route('/batches/<int:id>/force_delete', methods=['POST'])
@login_required
def force_delete_batch_route(id):
    """Force delete a batch by removing it from all recalls first"""
    try:
        print(f"DEBUG: Force deleting batch {id}")
        # First remove from all recalls
        remove_batch_from_all_recalls(id)
        print(f"DEBUG: Removed batch {id} from all recalls")
        # Then delete the batch
        result = delete_batch(id)
        print(f"DEBUG: Force delete result: {result}")
        flash('Batch force deleted successfully! (Removed from all recalls)', 'success')
    except Exception as e:
        print(f"DEBUG: Force delete failed with error: {str(e)}")
        flash(f'Error force deleting batch: {str(e)}', 'error')
    return redirect(url_for('inventory.list_batches'))

# API Routes
@inventory_bp.route('/api/batch/<int:batch_id>/compliance')
@login_required
def get_batch_compliance_api(batch_id):
    """API endpoint to get detailed compliance information for a batch"""
    compliance = get_batch_compliance_status(batch_id)
    return jsonify(compliance)
