"""
API Routes for Round Robin Tournament Statistics
"""
from flask import render_template, jsonify, request
from time import time
from functools import wraps
import hashlib
import os

# Simple in-memory cache for expensive queries
# Cache structure: {cache_key: {'data': ..., 'expires_at': timestamp}}
_cache = {}
CACHE_TTL = 300  # Cache for 5 minutes (300 seconds)
# Set ENABLE_CACHE=false to disable caching (enabled by default)
ENABLE_CACHE = os.environ.get('ENABLE_CACHE', 'true').lower() == 'true'


def register_routes(app, db_client):
    """Register all API routes with the Flask app"""
    
    def generate_cache_key(path, **kwargs):
        """Generate a cache key from path and parameters"""
        # Include path and sorted query parameters for consistent keys
        key_parts = [path]
        
        # Add query parameters (sorted for consistency)
        query_params = dict(request.args)
        if query_params:
            # Convert to sorted tuple for consistent hashing
            sorted_params = tuple(sorted(query_params.items()))
            key_parts.append(str(sorted_params))
        
        # Add any additional kwargs (like path parameters)
        if kwargs:
            sorted_kwargs = tuple(sorted(kwargs.items()))
            key_parts.append(str(sorted_kwargs))
        
        # Create a hash for the key
        key_string = '|'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get_cached_data(cache_key):
        """Get data from cache if it exists and hasn't expired"""
        if cache_key in _cache:
            cached = _cache[cache_key]
            if time() < cached['expires_at']:
                return cached['data']
            else:
                # Cache expired, remove it
                del _cache[cache_key]
        return None
    
    def set_cached_data(cache_key, data, ttl=CACHE_TTL):
        """Store data in cache with expiration time"""
        _cache[cache_key] = {
            'data': data,
            'expires_at': time() + ttl
        }
    
    def cached_endpoint(ttl=CACHE_TTL):
        """Decorator to cache API endpoint responses"""
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                # If caching is disabled, just execute the function directly
                if not ENABLE_CACHE:
                    result = f(*args, **kwargs)
                    if isinstance(result, tuple) and len(result) == 2:
                        return jsonify(result[0]), result[1]
                    return jsonify(result)
                
                # Generate cache key from request path and parameters
                cache_key = generate_cache_key(request.path, **kwargs)
                
                # Check cache first
                cached_result = get_cached_data(cache_key)
                if cached_result is not None:
                    # Return cached response (could be a tuple for status codes)
                    if isinstance(cached_result, tuple) and len(cached_result) == 2:
                        return jsonify(cached_result[0]), cached_result[1]
                    return jsonify(cached_result)
                
                # Cache miss - execute the function
                try:
                    result = f(*args, **kwargs)
                    
                    # Handle different return types
                    if isinstance(result, tuple) and len(result) == 2:
                        # It's a tuple (data, status_code)
                        data, status_code = result
                        cache_data = (data, status_code)
                        # Convert to JSON response for return
                        response = jsonify(data), status_code
                    else:
                        # It's just data (dict)
                        cache_data = result
                        response = jsonify(result)
                    
                    # Cache the result
                    set_cached_data(cache_key, cache_data, ttl=ttl)
                    
                    return response
                except Exception as e:
                    # Don't cache errors
                    raise
            return decorated_function
        return decorator
    
    @app.route('/')
    def index():
        """Home page with player list"""
        return render_template('index.html')
    
    @app.route('/api/players')
    @cached_endpoint()
    def get_players():
        """Get all players with rankings and current ratings (cached)"""
        if not db_client:
            return jsonify({'error': 'Database not available'}), 500
        
        try:
            players = db_client.get_all_players_with_rankings()
            total_tournaments = db_client.get_total_tournaments()
            return {
                'players': players,
                'total_tournaments': total_tournaments
            }
        except Exception as e:
            print(f"ERROR in /api/players: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}, 500
    
    @app.route('/api/cache/clear')
    def clear_cache():
        """Clear the cache (useful for testing or after data updates)"""
        global _cache
        cache_size = len(_cache)
        _cache.clear()
        return jsonify({
            'message': 'Cache cleared',
            'cleared_entries': cache_size
        })
    
    @app.route('/api/cache/stats')
    def cache_stats():
        """Get cache statistics"""
        global _cache
        now = time()
        active_entries = 0
        expired_entries = 0
        total_size = 0
        
        for key, value in _cache.items():
            if now < value['expires_at']:
                active_entries += 1
                # Estimate size (rough calculation)
                if isinstance(value['data'], dict):
                    total_size += len(str(value['data']))
            else:
                expired_entries += 1
        
        return jsonify({
            'total_entries': len(_cache),
            'active_entries': active_entries,
            'expired_entries': expired_entries,
            'cache_ttl_seconds': CACHE_TTL,
            'estimated_size_bytes': total_size
        })
    
    @app.route('/api/rating-distribution')
    @cached_endpoint()
    def get_rating_distribution():
        """Get rating distribution for active players (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            active_days = request.args.get('active_days', 365, type=int)
            distribution = db_client.get_rating_distribution(active_days=active_days)
            return distribution
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/rating-history')
    @cached_endpoint()
    def get_player_rating_history(player_name):
        """Get player rating history for charting (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            history = db_client.get_player_rating_history(player_name)
            
            # If no history found, try to get current rating from rankings view as fallback
            if not history or len(history) == 0:
                try:
                    ranking_result = (
                        db_client.client.table('player_rankings_view')
                        .select('current_rating')
                        .eq('player_name', player_name)
                        .execute()
                    )
                    if ranking_result.data and len(ranking_result.data) > 0:
                        current_rating = ranking_result.data[0].get('current_rating')
                        if current_rating:
                            print(f"Found current rating {current_rating} for {player_name} in rankings view, but no rating history")
                            # Return a minimal history entry so frontend can display the rating
                            history = [{
                                'tournament_date': None,
                                'rating_post': current_rating,
                                'rating_pre': current_rating
                            }]
                except Exception as e:
                    print(f"Could not get rating from rankings view: {e}")
            
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
            
            return {
                'chart_data': chart_data,
                'raw_data': history
            }
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/match-stats')
    @cached_endpoint()
    def get_player_match_stats(player_name):
        """Get player match statistics, optionally filtered by days_back (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            days_back = request.args.get('days_back', type=int)
            include_top_rated_win = request.args.get('include_top_rated_win', 'false').lower() == 'true'
            stats = db_client.get_player_match_stats(player_name, days_back=days_back, 
                                                      include_top_rated_win=include_top_rated_win)
            return stats
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/top-rated-win')
    @cached_endpoint()
    def get_player_top_rated_win(player_name):
        """Get top rated win for a player (lazy-loaded after initial stats, cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            days_back = request.args.get('days_back', type=int)
            # Get match stats with top rated win enabled
            stats = db_client.get_player_match_stats(player_name, days_back=days_back, 
                                                      include_top_rated_win=True)
            return {
                'top_rated_win': stats.get('top_rated_win'),
                'top_rated_win_info': stats.get('top_rated_win_info')
            }
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/tournament-stats')
    @cached_endpoint()
    def get_player_tournament_stats(player_name):
        """Get player statistics by tournament (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            stats = db_client.get_player_stats_by_tournament(player_name)
            return {'stats': stats}
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/matches')
    @cached_endpoint()
    def get_player_matches(player_name):
        """Get recent matches for a player with pagination (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
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
            
            return {
                'matches': result['matches'],
                'total': result['total'],
                'page': page,
                'page_size': page_size,
                'total_pages': result['total_pages']
            }
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/tournaments')
    @cached_endpoint()
    def get_player_tournaments(player_name):
        """Get all tournaments that a player has participated in (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            tournaments = db_client.get_player_tournaments(player_name)
            return {'tournaments': tournaments}
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player1_name>/vs/<player2_name>')
    @cached_endpoint()
    def get_head_to_head_matches(player1_name, player2_name):
        """Get head-to-head matches between two players with pagination (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            page = request.args.get('page', 1, type=int)
            page_size = request.args.get('page_size', 20, type=int)
            
            # Get paginated head-to-head matches
            result = db_client.get_head_to_head_matches_paginated(
                player1_name, 
                player2_name,
                page=page, 
                page_size=page_size
            )
            
            return {
                'matches': result['matches'],
                'total': result['total'],
                'page': page,
                'page_size': page_size,
                'total_pages': result['total_pages'],
                'player1_wins': result.get('player1_wins', 0),
                'player2_wins': result.get('player2_wins', 0),
                'player1_name': player1_name,
                'player2_name': player2_name
            }
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/opponents')
    @cached_endpoint()
    def get_player_opponents(player_name):
        """Get all opponents that a player has played against (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            opponents = db_client.get_opponents(player_name)
            return {'opponents': opponents}
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/performance-vs-rating-ranges')
    @cached_endpoint()
    def get_performance_vs_rating_ranges(player_name):
        """Get win rate performance against different rating ranges (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            days_back = request.args.get('days_back', type=int)
            performance = db_client.get_performance_vs_rating_ranges(player_name, days_back=days_back)
            return performance
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/api/player/<player_name>/tournament-calendar')
    @cached_endpoint()
    def get_player_tournament_calendar(player_name):
        """Get all tournaments with attendance information for calendar view (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            tournaments = db_client.get_all_tournaments_with_attendance(player_name)
            return {'tournaments': tournaments}
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/player/<player_name>')
    def player_detail(player_name):
        """Player detail page with charts"""
        return render_template('player.html', player_name=player_name)
    
    @app.route('/tournaments')
    def tournaments():
        """Tournaments list page"""
        return render_template('tournaments.html')
    
    @app.route('/api/tournaments')
    @cached_endpoint()
    def get_tournaments():
        """Get all tournaments with statistics (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            tournaments = db_client.get_all_tournaments_with_stats()
            return {'tournaments': tournaments}
        except Exception as e:
            return {'error': str(e)}, 500
    
    @app.route('/tournament/<int:tournament_id>')
    def tournament_detail(tournament_id):
        """Tournament detail page"""
        return render_template('tournament.html', tournament_id=tournament_id)
    
    @app.route('/api/tournament/<int:tournament_id>')
    @cached_endpoint()
    def get_tournament_detail(tournament_id):
        """Get tournament details including groups, players, and matches (cached)"""
        if not db_client:
            return {'error': 'Database not available'}, 500
        
        try:
            tournament_details = db_client.get_tournament_details(tournament_id)
            if not tournament_details:
                return {'error': 'Tournament not found'}, 404
            return tournament_details
        except Exception as e:
            return {'error': str(e)}, 500
    

