-- Round Robin Tournament Database Schema
-- Run this SQL in your Supabase SQL Editor to create all necessary tables

-- ============================================================================
-- 1. PLAYERS TABLE
-- ============================================================================
-- Stores unique player information
CREATE TABLE IF NOT EXISTS players (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_players_name ON players(name);

-- ============================================================================
-- 2. TOURNAMENTS TABLE
-- ============================================================================
-- Stores tournament information
CREATE TABLE IF NOT EXISTS tournaments (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(name, date)
);

CREATE INDEX IF NOT EXISTS idx_tournaments_date ON tournaments(date);
CREATE INDEX IF NOT EXISTS idx_tournaments_name ON tournaments(name);

-- ============================================================================
-- 3. ROUND_ROBIN_GROUPS TABLE
-- ============================================================================
-- Stores round robin group information
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
-- 4. PLAYER_RATING_HISTORY TABLE
-- ============================================================================
-- Tracks player rating changes over time for charting
CREATE TABLE IF NOT EXISTS player_rating_history (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    group_id BIGINT REFERENCES round_robin_groups(id) ON DELETE SET NULL,
    rating_pre INTEGER,
    rating_post INTEGER,
    rating_change INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rating_history_player ON player_rating_history(player_id);
CREATE INDEX IF NOT EXISTS idx_rating_history_tournament ON player_rating_history(tournament_id);
CREATE INDEX IF NOT EXISTS idx_rating_history_date ON player_rating_history(created_at);
CREATE INDEX IF NOT EXISTS idx_rating_history_player_date ON player_rating_history(player_id, created_at);

-- ============================================================================
-- 5. PLAYER_TOURNAMENT_STATS TABLE
-- ============================================================================
-- Stores comprehensive player statistics per tournament/group
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
-- 6. MATCHES TABLE
-- ============================================================================
-- Stores individual match results
CREATE TABLE IF NOT EXISTS matches (
    id BIGSERIAL PRIMARY KEY,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    group_id BIGINT REFERENCES round_robin_groups(id) ON DELETE SET NULL,
    player1_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    player2_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    player1_score INTEGER NOT NULL DEFAULT 0,
    player2_score INTEGER NOT NULL DEFAULT 0,
    winner_id BIGINT REFERENCES players(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CHECK (player1_id != player2_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament_id);
CREATE INDEX IF NOT EXISTS idx_matches_group ON matches(group_id);
CREATE INDEX IF NOT EXISTS idx_matches_player1 ON matches(player1_id);
CREATE INDEX IF NOT EXISTS idx_matches_player2 ON matches(player2_id);
CREATE INDEX IF NOT EXISTS idx_matches_winner ON matches(winner_id);
CREATE INDEX IF NOT EXISTS idx_matches_players ON matches(player1_id, player2_id);

-- ============================================================================
-- 7. VIEWS FOR EASY QUERYING
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

-- ============================================================================
-- 8. FUNCTIONS FOR AUTOMATIC UPDATES
-- ============================================================================

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for players table
CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Trigger for tournaments table
CREATE TRIGGER update_tournaments_updated_at BEFORE UPDATE ON tournaments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to automatically set winner_id in matches
CREATE OR REPLACE FUNCTION set_match_winner()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.player1_score > NEW.player2_score THEN
        NEW.winner_id = NEW.player1_id;
    ELSIF NEW.player2_score > NEW.player1_score THEN
        NEW.winner_id = NEW.player2_id;
    ELSE
        NEW.winner_id = NULL; -- Draw
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically set winner
CREATE TRIGGER set_winner_before_insert_match BEFORE INSERT ON matches
    FOR EACH ROW EXECUTE FUNCTION set_match_winner();

CREATE TRIGGER set_winner_before_update_match BEFORE UPDATE ON matches
    FOR EACH ROW EXECUTE FUNCTION set_match_winner();

-- ============================================================================
-- 9. HELPER FUNCTIONS FOR QUERIES
-- ============================================================================

-- Function to get player rating over time
CREATE OR REPLACE FUNCTION get_player_rating_history(player_name_param TEXT)
RETURNS TABLE (
    tournament_date DATE,
    rating_pre INTEGER,
    rating_post INTEGER,
    rating_change INTEGER,
    tournament_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.date,
        prh.rating_pre,
        prh.rating_post,
        prh.rating_change,
        t.name
    FROM player_rating_history prh
    JOIN players p ON prh.player_id = p.id
    JOIN tournaments t ON prh.tournament_id = t.id
    WHERE p.name = player_name_param
    ORDER BY t.date ASC;
END;
$$ LANGUAGE plpgsql;

-- Function to get player match statistics
CREATE OR REPLACE FUNCTION get_player_match_stats(player_name_param TEXT)
RETURNS TABLE (
    total_matches BIGINT,
    wins BIGINT,
    losses BIGINT,
    draws BIGINT,
    win_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT AS total_matches,
        COUNT(*) FILTER (WHERE m.winner_id = p.id)::BIGINT AS wins,
        COUNT(*) FILTER (WHERE m.winner_id != p.id AND m.winner_id IS NOT NULL)::BIGINT AS losses,
        COUNT(*) FILTER (WHERE m.winner_id IS NULL)::BIGINT AS draws,
        CASE 
            WHEN COUNT(*) > 0 THEN 
                ROUND((COUNT(*) FILTER (WHERE m.winner_id = p.id)::NUMERIC / COUNT(*)::NUMERIC) * 100, 2)
            ELSE 0
        END AS win_percentage
    FROM players p
    LEFT JOIN matches m ON (m.player1_id = p.id OR m.player2_id = p.id)
    WHERE p.name = player_name_param
    GROUP BY p.id;
END;
$$ LANGUAGE plpgsql;

