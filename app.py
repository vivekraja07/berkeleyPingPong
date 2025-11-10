#!/usr/bin/env python3
"""
Flask Web Application for Round Robin Tournament Statistics
Provides UI with charts for player statistics
"""
from flask import Flask, render_template, jsonify, request
from db.round_robin_client import RoundRobinClient
from datetime import datetime
import os

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Initialize database client
try:
    db_client = RoundRobinClient()
except Exception as e:
    print(f"Warning: Could not initialize database client: {e}")
    db_client = None


@app.route('/')
def index():
    """Home page with player list"""
    return render_template('index.html')


@app.route('/api/players')
def get_players():
    """Get all players"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        players = db_client.get_all_players()
        return jsonify({'players': players})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/rating-history')
def get_player_rating_history(player_name):
    """Get player rating history for charting"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        history = db_client.get_player_rating_history(player_name)
        
        # Format for Chart.js
        chart_data = {
            'labels': [entry.get('tournament_date', '') for entry in history],
            'datasets': [{
                'label': 'Rating',
                'data': [entry.get('rating_post') for entry in history],
                'borderColor': 'rgb(75, 192, 192)',
                'backgroundColor': 'rgba(75, 192, 192, 0.2)',
                'tension': 0.1,
                'fill': True
            }]
        }
        
        return jsonify({
            'chart_data': chart_data,
            'raw_data': history
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/match-stats')
def get_player_match_stats(player_name):
    """Get player match statistics, optionally filtered by days_back"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        days_back = request.args.get('days_back', type=int)
        stats = db_client.get_player_match_stats(player_name, days_back=days_back)
        
        # Add debug info to help troubleshoot
        debug_info = {
            'highest_rating': stats.get('highest_rating'),
            'top_rated_win': stats.get('top_rated_win'),
            'date_joined': stats.get('date_joined'),
            'last_match_date': stats.get('last_match_date'),
            'highest_rating_type': type(stats.get('highest_rating')).__name__,
            'top_rated_win_type': type(stats.get('top_rated_win')).__name__,
        }
        stats['_debug'] = debug_info
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/tournament-stats')
def get_player_tournament_stats(player_name):
    """Get player statistics by tournament"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        stats = db_client.get_player_stats_by_tournament(player_name)
        return jsonify({'stats': stats})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/matches')
def get_player_matches(player_name):
    """Get recent matches for a player with pagination"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 20, type=int)
        days_back = request.args.get('days_back', type=int)
        tournament_id = request.args.get('tournament_id', type=int)
        
        # Get total count and paginated matches
        result = db_client.get_player_matches_paginated(
            player_name, 
            page=page, 
            page_size=page_size, 
            days_back=days_back,
            tournament_id=tournament_id
        )
        
        return jsonify({
            'matches': result['matches'],
            'total': result['total'],
            'page': page,
            'page_size': page_size,
            'total_pages': result['total_pages']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/tournaments')
def get_player_tournaments(player_name):
    """Get all tournaments that a player has participated in"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        tournaments = db_client.get_player_tournaments(player_name)
        return jsonify({'tournaments': tournaments})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player1_name>/vs/<player2_name>')
def get_head_to_head_matches(player1_name, player2_name):
    """Get head-to-head matches between two players"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        matches = db_client.get_head_to_head_matches(player1_name, player2_name)
        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<player_name>/opponents')
def get_player_opponents(player_name):
    """Get all opponents that a player has played against"""
    if not db_client:
        return jsonify({'error': 'Database not available'}), 500
    
    try:
        opponents = db_client.get_opponents(player_name)
        return jsonify({'opponents': opponents})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/player/<player_name>')
def player_detail(player_name):
    """Player detail page with charts"""
    return render_template('player.html', player_name=player_name)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"\n{'='*60}")
    print(f"Round Robin Statistics UI")
    print(f"{'='*60}")
    print(f"Server starting on http://localhost:{port}")
    print(f"Open your browser and navigate to: http://localhost:{port}")
    print(f"{'='*60}\n")
    app.run(debug=True, host='127.0.0.1', port=port)

