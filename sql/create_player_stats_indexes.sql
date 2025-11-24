-- Create Indexes for Player Lookups
-- These indexes speed up queries on player names in views
-- Note: Cannot index views directly, so we index the underlying tables

-- ============================================================================
-- Indexes on players table (for joins in views)
-- ============================================================================

-- Index on player name for fast lookups in views
CREATE INDEX IF NOT EXISTS idx_players_name 
ON players(name);

-- ============================================================================
-- Indexes on matches table (used by match_results_view)
-- ============================================================================

-- Index for player1 lookups (most common)
CREATE INDEX IF NOT EXISTS idx_matches_player1_tournament 
ON matches(player1_id, tournament_id);

-- Index for player2 lookups
CREATE INDEX IF NOT EXISTS idx_matches_player2_tournament 
ON matches(player2_id, tournament_id);

-- Composite index covering both players
CREATE INDEX IF NOT EXISTS idx_matches_players_tournament 
ON matches(player1_id, player2_id, tournament_id);

-- Index on winner_id for win/loss queries
CREATE INDEX IF NOT EXISTS idx_matches_winner 
ON matches(winner_id) WHERE winner_id IS NOT NULL;

-- ============================================================================
-- Indexes on tournaments table (for date filtering in views)
-- ============================================================================

-- Index on tournament date for date filtering
CREATE INDEX IF NOT EXISTS idx_tournaments_date 
ON tournaments(date DESC);

-- ============================================================================
-- Indexes on player_rating_history table (used by player_rating_chart_view)
-- ============================================================================

-- Index for getting latest rating per player (with date ordering)
CREATE INDEX IF NOT EXISTS idx_rating_history_player_tournament 
ON player_rating_history(player_id, tournament_id DESC, created_at DESC);

-- Index for date filtering via tournament_id
CREATE INDEX IF NOT EXISTS idx_rating_history_tournament 
ON player_rating_history(tournament_id);

-- Index for rating_post lookups (filtering non-null ratings)
CREATE INDEX IF NOT EXISTS idx_rating_history_rating_post 
ON player_rating_history(player_id, rating_post DESC) 
WHERE rating_post IS NOT NULL;

-- ============================================================================
-- Indexes on player_tournament_stats table (used by player_stats_view)
-- ============================================================================

-- Index for player lookups
CREATE INDEX IF NOT EXISTS idx_player_tournament_stats_player 
ON player_tournament_stats(player_id, tournament_id);

-- Index for tournament lookups
CREATE INDEX IF NOT EXISTS idx_player_tournament_stats_tournament 
ON player_tournament_stats(tournament_id);

