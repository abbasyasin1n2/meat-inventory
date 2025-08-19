
from .connection import get_db_connection, DB_TYPE
from .user_queries import execute_query
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import Config

def init_database():
    """Initialize the database with all required tables"""
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        return False

    try:
        cursor = conn.cursor()

        if DB_TYPE == 'mysql':
            # MySQL table creation
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS suppliers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    contact_person VARCHAR(100),
                    phone VARCHAR(20),
                    email VARCHAR(100),
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    animal_type VARCHAR(50),
                    cut_type VARCHAR(50),
                    processing_date DATE,
                    storage_requirements VARCHAR(255),
                    shelf_life INT,
                    packaging_details VARCHAR(255),
                    supplier_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS inventory_batches (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT NOT NULL,
                    batch_number VARCHAR(100),
                    quantity DECIMAL(10, 2) NOT NULL,
                    arrival_date DATE,
                    expiration_date DATE,
                    storage_location VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    session_name VARCHAR(100) NOT NULL,
                    session_date DATE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_inputs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    session_id INT NOT NULL,
                    batch_id INT NOT NULL,
                    quantity_used DECIMAL(10, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES processing_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_outputs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    session_id INT NOT NULL,
                    product_id INT NOT NULL,
                    output_type VARCHAR(50),
                    weight DECIMAL(10, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES processing_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT,
                    action VARCHAR(100) NOT NULL,
                    description TEXT,
                    ip_address VARCHAR(45),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            # Storage tables for MySQL
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS storage_locations (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    location_type VARCHAR(50),
                    capacity DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS storage_sensors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    storage_id INT NOT NULL,
                    sensor_type VARCHAR(50) NOT NULL,
                    sensor_id VARCHAR(100) NOT NULL,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (storage_id) REFERENCES storage_locations(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sensor_id INT NOT NULL,
                    temperature DECIMAL(5, 2),
                    humidity DECIMAL(5, 2),
                    alert_status VARCHAR(20) DEFAULT 'normal',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (sensor_id) REFERENCES storage_sensors(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            # Compliance and Regulatory Tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compliance_records (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    record_type VARCHAR(50) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    certificate_number VARCHAR(100),
                    issuing_authority VARCHAR(255),
                    issue_date DATE,
                    expiration_date DATE,
                    status VARCHAR(20) DEFAULT 'active',
                    file_path VARCHAR(500),
                    created_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users(id),
                    INDEX idx_record_type (record_type),
                    INDEX idx_expiration_date (expiration_date),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS batch_recalls (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    recall_number VARCHAR(50) UNIQUE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    reason TEXT NOT NULL,
                    severity_level VARCHAR(20) NOT NULL,
                    status VARCHAR(20) DEFAULT 'initiated',
                    initiated_by INT NOT NULL,
                    initiated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_date TIMESTAMP NULL,
                    customer_notification_sent BOOLEAN DEFAULT FALSE,
                    regulatory_notification_sent BOOLEAN DEFAULT FALSE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (initiated_by) REFERENCES users(id),
                    INDEX idx_recall_number (recall_number),
                    INDEX idx_severity_level (severity_level),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recall_batches (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    recall_id INT NOT NULL,
                    batch_id INT NOT NULL,
                    quantity_affected DECIMAL(10, 2),
                    recovery_status VARCHAR(20) DEFAULT 'pending',
                    recovery_date TIMESTAMP NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (recall_id) REFERENCES batch_recalls(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id),
                    UNIQUE KEY unique_recall_batch (recall_id, batch_id),
                    INDEX idx_recall_id (recall_id),
                    INDEX idx_batch_id (batch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS food_safety_incidents (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    incident_number VARCHAR(50) UNIQUE NOT NULL,
                    incident_type VARCHAR(50) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    severity_level VARCHAR(20) NOT NULL,
                    status VARCHAR(20) DEFAULT 'open',
                    reported_by INT NOT NULL,
                    reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    investigation_notes TEXT,
                    corrective_actions TEXT,
                    root_cause TEXT,
                    closed_date TIMESTAMP NULL,
                    closed_by INT NULL,
                    regulatory_reported BOOLEAN DEFAULT FALSE,
                    regulatory_report_date TIMESTAMP NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (reported_by) REFERENCES users(id),
                    FOREIGN KEY (closed_by) REFERENCES users(id),
                    INDEX idx_incident_number (incident_number),
                    INDEX idx_incident_type (incident_type),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS incident_batches (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    incident_id INT NOT NULL,
                    batch_id INT NOT NULL,
                    involvement_level VARCHAR(50) NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (incident_id) REFERENCES food_safety_incidents(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id),
                    UNIQUE KEY unique_incident_batch (incident_id, batch_id),
                    INDEX idx_incident_id (incident_id),
                    INDEX idx_batch_id (batch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compliance_audits (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    audit_type VARCHAR(50) NOT NULL,
                    auditor_name VARCHAR(255) NOT NULL,
                    audit_date DATE NOT NULL,
                    scope TEXT,
                    findings TEXT,
                    recommendations TEXT,
                    overall_rating VARCHAR(20),
                    status VARCHAR(20) DEFAULT 'completed',
                    follow_up_required BOOLEAN DEFAULT FALSE,
                    follow_up_date DATE NULL,
                    report_file_path VARCHAR(500),
                    conducted_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (conducted_by) REFERENCES users(id),
                    INDEX idx_audit_type (audit_type),
                    INDEX idx_audit_date (audit_date),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            # Distribution and Replenishment (MySQL)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outbound_shipments (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    shipment_number VARCHAR(50) UNIQUE NOT NULL,
                    destination_name VARCHAR(255) NOT NULL,
                    destination_type VARCHAR(50) NOT NULL,
                    scheduled_date DATE,
                    status VARCHAR(20) DEFAULT 'planned',
                    notes TEXT,
                    created_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS shipment_lines (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    shipment_id INT NOT NULL,
                    batch_id INT NOT NULL,
                    quantity_shipped DECIMAL(10,2) NOT NULL,
                    picked_strategy VARCHAR(10) DEFAULT 'FIFO',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shipment_id) REFERENCES outbound_shipments(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            # Track allocations that were restored on cancel so we can re-apply if moved back to planned
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS shipment_restorations (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    shipment_id INT NOT NULL,
                    batch_id INT NOT NULL,
                    quantity DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shipment_id) REFERENCES outbound_shipments(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reorder_rules (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT NOT NULL,
                    min_qty DECIMAL(10,2) NOT NULL,
                    target_qty DECIMAL(10,2) NOT NULL,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')

        else:
            # SQLite table creation
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    contact_person TEXT,
                    phone TEXT,
                    email TEXT,
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    animal_type TEXT,
                    cut_type TEXT,
                    processing_date DATE,
                    storage_requirements TEXT,
                    shelf_life INTEGER,
                    packaging_details TEXT,
                    supplier_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS inventory_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    batch_number TEXT,
                    quantity REAL NOT NULL,
                    arrival_date DATE,
                    expiration_date DATE,
                    storage_location TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_name TEXT NOT NULL,
                    session_date DATE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    quantity_used REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES processing_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_outputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    output_type TEXT,
                    weight REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES processing_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    description TEXT,
                    ip_address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
                )
            ''')

            # Storage tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS storage_locations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    location_type TEXT,
                    capacity REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS storage_sensors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    storage_id INTEGER NOT NULL,
                    sensor_type TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (storage_id) REFERENCES storage_locations(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_id INTEGER NOT NULL,
                    temperature REAL,
                    humidity REAL,
                    alert_status TEXT DEFAULT 'normal',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (sensor_id) REFERENCES storage_sensors(id) ON DELETE CASCADE
                )
            ''')

            # Compliance and Regulatory Tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compliance_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    certificate_number TEXT,
                    issuing_authority TEXT,
                    issue_date DATE,
                    expiration_date DATE,
                    status TEXT DEFAULT 'active',
                    file_path TEXT,
                    created_by INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS batch_recalls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recall_number TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    severity_level TEXT NOT NULL,
                    status TEXT DEFAULT 'initiated',
                    initiated_by INTEGER NOT NULL,
                    initiated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_date TIMESTAMP NULL,
                    customer_notification_sent BOOLEAN DEFAULT FALSE,
                    regulatory_notification_sent BOOLEAN DEFAULT FALSE,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (initiated_by) REFERENCES users(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recall_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recall_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    quantity_affected REAL,
                    recovery_status TEXT DEFAULT 'pending',
                    recovery_date TIMESTAMP NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (recall_id) REFERENCES batch_recalls(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id),
                    UNIQUE(recall_id, batch_id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS food_safety_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_number TEXT UNIQUE NOT NULL,
                    incident_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity_level TEXT NOT NULL,
                    status TEXT DEFAULT 'open',
                    reported_by INTEGER NOT NULL,
                    reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    investigation_notes TEXT,
                    corrective_actions TEXT,
                    root_cause TEXT,
                    closed_date TIMESTAMP NULL,
                    closed_by INTEGER NULL,
                    regulatory_reported BOOLEAN DEFAULT FALSE,
                    regulatory_report_date TIMESTAMP NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (reported_by) REFERENCES users(id),
                    FOREIGN KEY (closed_by) REFERENCES users(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS incident_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    involvement_level TEXT NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (incident_id) REFERENCES food_safety_incidents(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id),
                    UNIQUE(incident_id, batch_id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compliance_audits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    audit_type TEXT NOT NULL,
                    auditor_name TEXT NOT NULL,
                    audit_date DATE NOT NULL,
                    scope TEXT,
                    findings TEXT,
                    recommendations TEXT,
                    overall_rating TEXT,
                    status TEXT DEFAULT 'completed',
                    follow_up_required BOOLEAN DEFAULT FALSE,
                    follow_up_date DATE NULL,
                    report_file_path TEXT,
                    conducted_by INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (conducted_by) REFERENCES users(id)
                )
            ''')

            # Distribution and Replenishment (SQLite)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outbound_shipments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shipment_number TEXT UNIQUE NOT NULL,
                    destination_name TEXT NOT NULL,
                    destination_type TEXT NOT NULL,
                    scheduled_date DATE,
                    status TEXT DEFAULT 'planned',
                    notes TEXT,
                    created_by INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS shipment_lines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shipment_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    quantity_shipped REAL NOT NULL,
                    picked_strategy TEXT DEFAULT 'FIFO',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shipment_id) REFERENCES outbound_shipments(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id)
                )
            ''')

            # Track allocations that were restored on cancel so we can re-apply if moved back to planned
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS shipment_restorations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shipment_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    quantity REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (shipment_id) REFERENCES outbound_shipments(id) ON DELETE CASCADE,
                    FOREIGN KEY (batch_id) REFERENCES inventory_batches(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reorder_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    min_qty REAL NOT NULL,
                    target_qty REAL NOT NULL,
                    active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
            ''')

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        print(f"Database initialization error: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def test_connection():
    """Test database connection"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return True
    return False

def backup_database(backup_path=None):
    """Create a backup of the database"""
    if DB_TYPE == 'mysql':
        print("MySQL backup requires mysqldump utility. Please use XAMPP/phpMyAdmin backup features.")
        return False
    else:
        if not backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f'backup_{timestamp}.db'

        try:
            import shutil
            shutil.copy2(Config.SQLITE_DATABASE, backup_path)
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False

def get_table_info(table_name):
    """Get information about a table structure"""
    if DB_TYPE == 'mysql':
        return execute_query(f'DESCRIBE {table_name}', fetch_all=True)
    else:
        return execute_query(f'PRAGMA table_info({table_name})', fetch_all=True)

def get_all_tables():
    """Get list of all tables in the database"""
    if DB_TYPE == 'mysql':
        # For MySQL, information_schema requires the current database name; we use DATABASE from connection config via SQL()
        return execute_query(
            "SELECT table_name as name FROM information_schema.tables WHERE table_schema = DATABASE()",
            fetch_all=True
        )
    else:
        return execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            fetch_all=True
        )
