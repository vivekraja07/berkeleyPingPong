-- Migration script to fix rating calculation bug in player_rankings_view
-- Issue: When multiple rating history entries exist for the same tournament date,
-- the view was not prioritizing entries with rating_post over entries with only rating_pre
-- This caused incorrect ratings to be displayed on the main players page

-- Step 1: Drop the old view (CASCADE to drop dependent indexes)
DROP MATERIALIZED VIEW IF EXISTS player_rankings_view CASCADE;

-- Step 2: Recreate the view with the fixed ordering logic
CREATE MATERIALIZED VIEW player_rankings_view AS
WITH latest_ratings AS (
    -- Get the most recent rating for each player
    -- Use rating_post first, fallback to rating_pre if rating_post is missing (matching old method)
    -- Prioritize entries with rating_post over entries with only rating_pre
    -- When multiple entries exist for the same date, prefer the one with rating_post and highest value
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
    ORDER BY prh.player_id, 
             t.date DESC, 
             -- Prioritize entries with rating_post over entries with only rating_pre
             CASE WHEN prh.rating_post IS NOT NULL AND prh.rating_post::TEXT != '' THEN 0 ELSE 1 END,
             -- When both have rating_post, prefer the highest rating_post value
             COALESCE(prh.rating_post, 0) DESC,
             prh.created_at DESC
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

-- Step 3: Recreate indexes for fast lookups
CREATE UNIQUE INDEX idx_player_rankings_player_id 
ON player_rankings_view(player_id);

CREATE INDEX idx_player_rankings_rating 
ON player_rankings_view(current_rating DESC NULLS LAST);

CREATE INDEX idx_player_rankings_ranking 
ON player_rankings_view(ranking NULLS LAST);

CREATE INDEX idx_player_rankings_active 
ON player_rankings_view(is_active) WHERE is_active = true;

-- Step 4: Verify the fix
-- Check Ryan Wang's rating (should now show 1660 instead of 1630)
SELECT player_name, current_rating, ranking 
FROM player_rankings_view 
WHERE player_name = 'Ryan Wang';

