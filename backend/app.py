#!/usr/bin/env python3
"""
Flask Web Application for Round Robin Tournament Statistics
Provides UI with charts for player statistics
"""
import os
import sys
import warnings
from flask import Flask

# Suppress the harmless semaphore cleanup warning from Flask's reloader
warnings.filterwarnings('ignore', message='.*resource_tracker.*', category=UserWarning)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.api.routes import register_routes
from backend.db.round_robin_client import RoundRobinClient

# Create Flask app with custom template and static folders
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'frontend', 'templates'),
    static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'frontend', 'static')
)
app.config['JSON_SORT_KEYS'] = False

# Make Google Analytics ID available to all templates
@app.context_processor
def inject_ga_id():
    """Inject Google Analytics ID into all templates"""
    ga_id = os.environ.get('GOOGLE_ANALYTICS_ID')
    return {'google_analytics_id': ga_id}

# Initialize database client
try:
    db_client = RoundRobinClient()
except Exception as e:
    print(f"Warning: Could not initialize database client: {e}")
    db_client = None

# Register API routes
register_routes(app, db_client)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    host = os.environ.get('HOST', '127.0.0.1')
    use_reloader = os.environ.get('FLASK_RELOADER', 'true').lower() == 'true'
    
    print(f"\n{'='*60}")
    print(f"Round Robin Statistics UI")
    print(f"{'='*60}")
    print(f"Server starting on http://{host}:{port}")
    print(f"Open your browser and navigate to: http://{host}:{port}")
    print(f"{'='*60}\n")
    
    # Disable reloader if it causes issues (set FLASK_RELOADER=false)
    # The reloader can cause semaphore warnings on shutdown
    app.run(debug=debug, host=host, port=port, use_reloader=use_reloader and debug)
