-- Create Materialized View for Player Rankings
-- This pre-computes player rankings, current ratings, and last match dates
-- Refreshes only when new data is imported (via trigger or manual refresh)

-- ============================================================================
-- Step 1: Create the Materialized View
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS player_rankings_view AS
WITH latest_ratings AS (
    -- Get the most recent rating for each player
    -- Use rating_post first, fallback to rating_pre if rating_post is missing (matching old method)
    SELECT DISTINCT ON (prh.player_id)
        prh.player_id,
        CASE 
            WHEN prh.rating_post IS NOT NULL AND prh.rating_post::TEXT != '' THEN
                prh.rating_post::INTEGER
            WHEN prh.rating_pre IS NOT NULL AND prh.rating_pre::TEXT != '' THEN
                prh.rating_pre::INTEGER
            ELSE NULL
        END AS current_rating,
        t.date AS rating_date
    FROM player_rating_history prh
    JOIN tournaments t ON prh.tournament_id = t.id
    WHERE (prh.rating_post IS NOT NULL AND prh.rating_post::TEXT != '') 
       OR (prh.rating_pre IS NOT NULL AND prh.rating_pre::TEXT != '')
    ORDER BY prh.player_id, t.date DESC, prh.created_at DESC
),
active_player_ids AS (
    -- Get all players who have played in the last 365 days
    -- Check both matches and rating history
    SELECT DISTINCT player_id FROM (
        -- Players from matches
        SELECT player1_id AS player_id 
        FROM matches m
        JOIN tournaments t ON m.tournament_id = t.id
        WHERE t.date >= CURRENT_DATE - INTERVAL '365 days'
        
        UNION
        
        SELECT player2_id AS player_id 
        FROM matches m
        JOIN tournaments t ON m.tournament_id = t.id
        WHERE t.date >= CURRENT_DATE - INTERVAL '365 days'
        
        UNION
        
        -- Players from rating history (catches edge cases)
        SELECT prh.player_id 
        FROM player_rating_history prh
        JOIN tournaments t ON prh.tournament_id = t.id
        WHERE t.date >= CURRENT_DATE - INTERVAL '365 days'
    ) combined
),
ranked_active_players AS (
    -- Calculate rankings for active players only
    -- Only rank players who have a rating (exclude NULL ratings from ranking)
    SELECT 
        p.id AS player_id,
        p.name AS player_name,
        lr.current_rating,
        ROW_NUMBER() OVER (
            ORDER BY lr.current_rating DESC
        ) AS ranking
    FROM players p
    INNER JOIN active_player_ids ap ON p.id = ap.player_id
    INNER JOIN latest_ratings lr ON p.id = lr.player_id
    -- Only include active players who have a rating
    WHERE lr.current_rating IS NOT NULL
),
last_match_dates AS (
    -- Get the most recent match date for each player
    SELECT 
        p.id AS player_id,
        MAX(t.date) AS last_match_date
    FROM players p
    LEFT JOIN matches m ON (m.player1_id = p.id OR m.player2_id = p.id)
    LEFT JOIN tournaments t ON m.tournament_id = t.id
    GROUP BY p.id
),
last_rating_dates AS (
    -- Get the most recent rating date for each player (fallback if no matches)
    SELECT 
        p.id AS player_id,
        MAX(t.date) AS last_rating_date
    FROM players p
    LEFT JOIN player_rating_history prh ON p.id = prh.player_id
    LEFT JOIN tournaments t ON prh.tournament_id = t.id
    GROUP BY p.id
)
SELECT 
    p.id AS player_id,
    p.name AS player_name,
    lr.current_rating,
    rap.ranking,
    CASE 
        WHEN lmd.last_match_date IS NOT NULL AND lrd.last_rating_date IS NOT NULL THEN
            GREATEST(lmd.last_match_date, lrd.last_rating_date)
        WHEN lmd.last_match_date IS NOT NULL THEN lmd.last_match_date
        WHEN lrd.last_rating_date IS NOT NULL THEN lrd.last_rating_date
        ELSE NULL
    END AS last_match_date,
    (p.id IN (SELECT player_id FROM active_player_ids)) AS is_active
FROM players p
LEFT JOIN latest_ratings lr ON p.id = lr.player_id
LEFT JOIN ranked_active_players rap ON p.id = rap.player_id
LEFT JOIN last_match_dates lmd ON p.id = lmd.player_id
LEFT JOIN last_rating_dates lrd ON p.id = lrd.player_id
ORDER BY rap.ranking NULLS LAST, p.name;

-- ============================================================================
-- Step 2: Create Indexes for Fast Lookups
-- ============================================================================

-- Index for fast lookups by player name
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_rankings_player_id 
ON player_rankings_view(player_id);

-- Index for sorting by rating
CREATE INDEX IF NOT EXISTS idx_player_rankings_rating 
ON player_rankings_view(current_rating DESC NULLS LAST);

-- Index for sorting by ranking
CREATE INDEX IF NOT EXISTS idx_player_rankings_ranking 
ON player_rankings_view(ranking NULLS LAST);

-- Index for filtering active players
CREATE INDEX IF NOT EXISTS idx_player_rankings_active 
ON player_rankings_view(is_active) WHERE is_active = true;

-- ============================================================================
-- Step 3: Create Function to Refresh the View
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_player_rankings_view()
RETURNS void AS $$
BEGIN
    -- Use CONCURRENTLY to avoid blocking reads (requires unique index)
    REFRESH MATERIALIZED VIEW CONCURRENTLY player_rankings_view;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Step 4: Initial Population
-- ============================================================================

-- Populate the view with initial data
REFRESH MATERIALIZED VIEW player_rankings_view;

-- ============================================================================
-- Notes:
-- ============================================================================
-- 1. This view refreshes only when explicitly called via refresh_player_rankings_view()
-- 2. Call this function after importing new tournament data
-- 3. The view uses CONCURRENTLY to avoid blocking reads during refresh
-- 4. Rankings are calculated only for active players (played in last 365 days)
-- 5. All players are included, but only active ones have rankings

