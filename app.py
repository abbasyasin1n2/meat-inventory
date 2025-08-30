
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from flask_login import LoginManager
from config.config import Config
from database import init_database, test_connection, get_user_by_id
from routes import auth_bp, main_bp, inventory_bp, processing_bp, traceability_bp, storage_bp, compliance_bp, distribution_bp
from models import User

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'

    @login_manager.user_loader
    def load_user(user_id):
        user_data = get_user_by_id(user_id)
        if user_data:
            is_admin = user_data.get('is_admin', False)
            return User(user_data['id'], user_data['username'], user_data['email'], user_data['password_hash'], is_admin)
        return None

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(inventory_bp, url_prefix='/inventory')
    app.register_blueprint(processing_bp, url_prefix='/processing')
    app.register_blueprint(traceability_bp, url_prefix='/traceability')
    app.register_blueprint(storage_bp, url_prefix='/storage')
    app.register_blueprint(compliance_bp, url_prefix='/compliance')
    app.register_blueprint(distribution_bp, url_prefix='/distribution')

    return app
