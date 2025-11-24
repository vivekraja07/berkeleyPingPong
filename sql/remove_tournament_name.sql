-- Remove tournament name column from database
-- Run this in your Supabase SQL Editor

-- ============================================================================
-- Step 1: Drop views that reference tournament_name
-- ============================================================================

DROP VIEW IF EXISTS player_stats_view CASCADE;
DROP VIEW IF EXISTS match_results_view CASCADE;
DROP VIEW IF EXISTS player_rating_chart_view CASCADE;

-- ============================================================================
-- Step 2: Drop unique constraint on (name, date) and index on name
-- ============================================================================

-- Drop the unique constraint (if it exists)
ALTER TABLE tournaments DROP CONSTRAINT IF EXISTS tournaments_name_date_key;
ALTER TABLE tournaments DROP CONSTRAINT IF EXISTS tournaments_name_date_unique;

-- Drop the index on name
DROP INDEX IF EXISTS idx_tournaments_name;

-- ============================================================================
-- Step 3: Drop the name column
-- ============================================================================

ALTER TABLE tournaments DROP COLUMN IF EXISTS name;

-- ============================================================================
-- Step 4: Recreate views without tournament_name
-- ============================================================================

-- View: Player statistics with tournament details
CREATE OR REPLACE VIEW player_stats_view AS
SELECT 
    p.id AS player_id,
    p.name AS player_name,
    t.id AS tournament_id,
    t.date AS tournament_date,
    rr.id AS group_id,
    rr.group_number,
    rr.group_name,
    pts.rating_pre,
    pts.rating_post,
    pts.rating_change,
    pts.matches_won,
    pts.games_won,
    pts.bonus_points,
    pts.change_w_bonus,
    pts.created_at
FROM player_tournament_stats pts
JOIN players p ON pts.player_id = p.id
JOIN tournaments t ON pts.tournament_id = t.id
JOIN round_robin_groups rr ON pts.group_id = rr.id
ORDER BY t.date DESC, rr.group_number, pts.player_number;

-- View: Match results with player names
CREATE OR REPLACE VIEW match_results_view AS
SELECT 
    m.id AS match_id,
    t.id AS tournament_id,
    t.date AS tournament_date,
    rr.id AS group_id,
    rr.group_number,
    rr.group_name,
    p1.id AS player1_id,
    p1.name AS player1_name,
    p2.id AS player2_id,
    p2.name AS player2_name,
    m.player1_score,
    m.player2_score,
    w.id AS winner_id,
    w.name AS winner_name,
    m.created_at
FROM matches m
JOIN tournaments t ON m.tournament_id = t.id
LEFT JOIN round_robin_groups rr ON m.group_id = rr.id
JOIN players p1 ON m.player1_id = p1.id
JOIN players p2 ON m.player2_id = p2.id
LEFT JOIN players w ON m.winner_id = w.id
ORDER BY t.date DESC, rr.group_number, m.created_at;

-- View: Player rating history for charting
CREATE OR REPLACE VIEW player_rating_chart_view AS
SELECT 
    p.id AS player_id,
    p.name AS player_name,
    t.id AS tournament_id,
    t.date AS tournament_date,
    prh.rating_pre,
    prh.rating_post,
    prh.rating_change,
    prh.created_at
FROM player_rating_history prh
JOIN players p ON prh.player_id = p.id
JOIN tournaments t ON prh.tournament_id = t.id
ORDER BY t.date ASC, prh.created_at ASC;

