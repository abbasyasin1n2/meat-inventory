
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, login_required, logout_user, current_user
import hashlib
from database import get_user_by_username, create_user, log_activity
from models import User

auth_bp = Blueprint('auth', __name__, template_folder='templates')

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = hash_password(password)

        user_data = get_user_by_username(username)

        if user_data and user_data['password_hash'] == password_hash:
            user_obj = User(user_data['id'], user_data['username'], user_data['email'], user_data['password_hash'])
            login_user(user_obj)
            log_activity(user_data['id'], 'login', 'User logged in', request.remote_addr)

            flash('Logged in successfully!', 'success')
            return redirect(url_for('main.dashboard'))
        else:
            flash('Invalid username or password!', 'error')

    return render_template('auth/login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        if password != confirm_password:
            flash('Passwords do not match!', 'error')
            return render_template('auth/register.html')

        if get_user_by_username(username):
            flash('Username already exists!', 'error')
            return render_template('auth/register.html')

        password_hash = hash_password(password)

        if create_user(username, email, password_hash):
            flash('Registration successful! Please log in.', 'success')
            return redirect(url_for('auth.login'))
        else:
            flash('Registration failed. Please try again.', 'error')

    return render_template('auth/register.html')

@auth_bp.route('/logout')
@login_required
def logout():
    log_activity(current_user.id, 'logout', 'User logged out', request.remote_addr)

    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('main.index'))
