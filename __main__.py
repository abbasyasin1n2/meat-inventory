
from .app import create_app
from .database import test_connection, init_database

app = create_app()

if __name__ == '__main__':
    with app.app_context():
        if not test_connection():
            print("Warning: Database connection failed!")
            print("Please check your database configuration.")
        else:
            print("Database connection successful!")
            if init_database():
                print("Database initialized successfully!")
            else:
                print("Database initialization failed!")

    app.run(debug=True)
