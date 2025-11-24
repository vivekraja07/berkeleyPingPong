-- Create Materialized View for Player Match Statistics
-- This pre-computes match statistics for each player
-- Refreshes only when new data is imported (via refresh function)

-- ============================================================================
-- Step 1: Create the Materialized View
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS player_match_stats_view AS
WITH player_match_counts AS (
    -- Count matches for each player (as player1 and player2)
    SELECT 
        p.id AS player_id,
        p.name AS player_name,
        COUNT(*) FILTER (WHERE m.winner_id = p.id) AS total_wins,
        COUNT(*) FILTER (WHERE m.winner_id IS NULL) AS total_draws,
        COUNT(*) FILTER (WHERE m.winner_id != p.id AND m.winner_id IS NOT NULL) AS total_losses,
        COUNT(*) AS total_matches,
        COUNT(DISTINCT m.tournament_id) AS total_tournaments,
        MIN(t.date) AS first_tournament_date,
        MAX(t.date) AS last_tournament_date
    FROM players p
    LEFT JOIN matches m ON (m.player1_id = p.id OR m.player2_id = p.id)
    LEFT JOIN tournaments t ON m.tournament_id = t.id
    GROUP BY p.id, p.name
),
player_ratings AS (
    -- Get current and highest ratings for each player
    SELECT DISTINCT ON (prh.player_id)
        prh.player_id,
        prh.rating_post AS current_rating,
        t.date AS rating_date
    FROM player_rating_history prh
    JOIN tournaments t ON prh.tournament_id = t.id
    WHERE prh.rating_post IS NOT NULL
    ORDER BY prh.player_id, t.date DESC, prh.created_at DESC
),
player_highest_ratings AS (
    -- Get highest rating ever for each player
    SELECT 
        prh.player_id,
        MAX(prh.rating_post) AS highest_rating
    FROM player_rating_history prh
    WHERE prh.rating_post IS NOT NULL
    GROUP BY prh.player_id
),
player_first_tournament AS (
    -- Get first tournament date from rating history (more accurate than matches)
    SELECT 
        prh.player_id,
        MIN(t.date) AS date_joined
    FROM player_rating_history prh
    JOIN tournaments t ON prh.tournament_id = t.id
    GROUP BY prh.player_id
)
SELECT 
    pmc.player_id,
    pmc.player_name,
    pmc.total_matches,
    pmc.total_wins,
    pmc.total_losses,
    pmc.total_draws,
    CASE 
        WHEN (pmc.total_wins + pmc.total_losses) > 0 THEN
            ROUND((pmc.total_wins::NUMERIC / (pmc.total_wins + pmc.total_losses)) * 100, 2)
        ELSE 0
    END AS win_percentage,
    pmc.total_tournaments,
    pmc.first_tournament_date,
    pmc.last_tournament_date,
    COALESCE(pft.date_joined, pmc.first_tournament_date) AS date_joined,
    pr.current_rating,
    phr.highest_rating
FROM player_match_counts pmc
LEFT JOIN player_ratings pr ON pmc.player_id = pr.player_id
LEFT JOIN player_highest_ratings phr ON pmc.player_id = phr.player_id
LEFT JOIN player_first_tournament pft ON pmc.player_id = pft.player_id;

-- ============================================================================
-- Step 2: Create Indexes for Fast Lookups
-- ============================================================================

-- Unique index for fast player lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_stats_player_id 
ON player_match_stats_view(player_id);

-- Index for player name lookups
CREATE INDEX IF NOT EXISTS idx_player_stats_player_name 
ON player_match_stats_view(player_name);

-- Index for sorting by total matches
CREATE INDEX IF NOT EXISTS idx_player_stats_total_matches 
ON player_match_stats_view(total_matches DESC);

-- Index for sorting by win percentage
CREATE INDEX IF NOT EXISTS idx_player_stats_win_percentage 
ON player_match_stats_view(win_percentage DESC);

-- ============================================================================
-- Step 3: Create Function to Refresh the View
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_player_match_stats_view()
RETURNS void AS $$
BEGIN
    -- Use CONCURRENTLY to avoid blocking reads (requires unique index)
    REFRESH MATERIALIZED VIEW CONCURRENTLY player_match_stats_view;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Step 4: Initial Population
-- ============================================================================

-- Populate the view with initial data
REFRESH MATERIALIZED VIEW player_match_stats_view;

-- ============================================================================
-- Notes:
-- ============================================================================
-- 1. This view refreshes only when explicitly called via refresh_player_match_stats_view()
-- 2. Call this function after importing new tournament data
-- 3. The view uses CONCURRENTLY to avoid blocking reads during refresh
-- 4. Statistics are calculated for all players (not filtered by date)
-- 5. For date-filtered stats (days_back), you'll still need to query match_results_view
--    but can use this view for base statistics

