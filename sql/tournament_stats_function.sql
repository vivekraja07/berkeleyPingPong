-- SQL Function to get tournament statistics efficiently
-- Run this in your Supabase SQL Editor
-- Updated to include source_url, parsing_status, and parse_error
-- Optimized to use subqueries instead of joins to avoid Cartesian products

-- Drop the existing function first (required when changing return type)
DROP FUNCTION IF EXISTS get_tournament_stats();

-- Create the function as SQL (not PL/pgSQL) to avoid variable name conflicts
CREATE FUNCTION get_tournament_stats()
RETURNS TABLE (
    tournament_id BIGINT,
    tournament_date DATE,
    num_players BIGINT,
    num_matches BIGINT,
    source_url TEXT,
    parsing_status TEXT,
    parse_error TEXT
) AS $$
    SELECT 
        t.id,
        t.date,
        COALESCE((
            SELECT COUNT(DISTINCT pts.player_id)
            FROM player_tournament_stats pts
            WHERE pts.tournament_id = t.id
        ), 0)::BIGINT,
        COALESCE((
            SELECT COUNT(*)
            FROM matches m
            WHERE m.tournament_id = t.id
        ), 0)::BIGINT,
        t.source_url,
        COALESCE(t.parsing_status, 'success'),
        t.parse_error
    FROM tournaments t
    ORDER BY t.date DESC;
$$ LANGUAGE sql;

