
from .auth import auth_bp
from .main import main_bp
from .inventory import inventory_bp
from .processing import processing_bp
from .traceability import traceability_bp
from .storage import storage_bp
from .compliance import compliance_bp
from .distribution import distribution_bp

__all__ = ['auth_bp', 'main_bp', 'inventory_bp', 'processing_bp', 'traceability_bp', 'storage_bp', 'compliance_bp', 'distribution_bp']
