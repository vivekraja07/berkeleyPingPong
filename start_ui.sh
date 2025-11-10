#!/bin/bash
# Start the Round Robin Statistics UI

echo "=========================================="
echo "Starting Round Robin Statistics UI"
echo "=========================================="
echo ""

# Check if Flask is installed
if ! python3 -c "import flask" 2>/dev/null; then
    echo "Installing Flask..."
    pip3 install flask
fi

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found"
    echo "Make sure SUPABASE_URL and SUPABASE_KEY are set"
    echo ""
fi

echo "Starting Flask server..."
echo "The UI will be available at: http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python3 app.py

