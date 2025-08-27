
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from database import (
    get_all_storage_locations,
    get_all_suppliers, add_supplier, get_supplier_by_id, update_supplier, delete_supplier, search_suppliers,
    get_all_products, add_product, get_product_by_id, update_product, delete_product, search_products,
    get_all_batches, add_batch, get_batch_by_id, update_batch, delete_batch, search_batches,
    get_batch_compliance_status
)

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
        update_product(id, name, animal_type, cut_type, processing_date, storage_requirements, shelf_life, packaging_details, supplier_id)
        flash('Product updated successfully!', 'success')
        return redirect(url_for('inventory.list_products'))

    suppliers = get_all_suppliers()
    return render_template('inventory/edit_product.html', product=product, suppliers=suppliers)

@inventory_bp.route('/products/delete/<int:id>', methods=['POST'])
@login_required
def delete_product_route(id):
    delete_product(id)
    flash('Product deleted successfully!', 'success')
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
        update_batch(id, product_id, batch_number, quantity, arrival_date, expiration_date, storage_location)
        flash('Batch updated successfully!', 'success')
        return redirect(url_for('inventory.list_batches'))

    products = get_all_products()
    storage_locations = get_all_storage_locations()
    return render_template('inventory/edit_batch.html', batch=batch, products=products, storage_locations=storage_locations)

@inventory_bp.route('/batches/delete/<int:id>', methods=['POST'])
@login_required
def delete_batch_route(id):
    delete_batch(id)
    flash('Batch deleted successfully!', 'success')
    return redirect(url_for('inventory.list_batches'))

# API Routes
@inventory_bp.route('/api/batch/<int:batch_id>/compliance')
@login_required
def get_batch_compliance_api(batch_id):
    """API endpoint to get detailed compliance information for a batch"""
    compliance = get_batch_compliance_status(batch_id)
    return jsonify(compliance)
