-- Create Missing Round Robin Tables
-- Run this in Supabase SQL Editor if you're missing round_robin_groups or player_tournament_stats

-- ============================================================================
-- ROUND_ROBIN_GROUPS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS round_robin_groups (
    id BIGSERIAL PRIMARY KEY,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    group_number INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tournament_id, group_number)
);

CREATE INDEX IF NOT EXISTS idx_rr_groups_tournament ON round_robin_groups(tournament_id);
CREATE INDEX IF NOT EXISTS idx_rr_groups_number ON round_robin_groups(group_number);

-- ============================================================================
-- PLAYER_TOURNAMENT_STATS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS player_tournament_stats (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES round_robin_groups(id) ON DELETE CASCADE,
    player_number INTEGER NOT NULL,
    rating_pre INTEGER,
    rating_post INTEGER,
    rating_change INTEGER,
    matches_won INTEGER DEFAULT 0,
    games_won INTEGER DEFAULT 0,
    bonus_points INTEGER DEFAULT 0,
    change_w_bonus INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(player_id, tournament_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_tournament_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_tournament ON player_tournament_stats(tournament_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_group ON player_tournament_stats(group_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_player_tournament ON player_tournament_stats(player_id, tournament_id);

-- ============================================================================
-- VIEWS (Optional but recommended)
-- ============================================================================

-- View: Player statistics with tournament details
CREATE OR REPLACE VIEW player_stats_view AS
SELECT 
    p.id AS player_id,
    p.name AS player_name,
    t.id AS tournament_id,
    t.name AS tournament_name,
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
    t.name AS tournament_name,
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
    t.date AS tournament_date,
    prh.rating_pre,
    prh.rating_post,
    prh.rating_change,
    t.name AS tournament_name,
    rr.group_number
FROM player_rating_history prh
JOIN players p ON prh.player_id = p.id
JOIN tournaments t ON prh.tournament_id = t.id
LEFT JOIN round_robin_groups rr ON prh.group_id = rr.id
ORDER BY p.name, t.date ASC;


