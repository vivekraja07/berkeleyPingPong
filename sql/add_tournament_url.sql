-- Add URL column to tournaments table
-- This stores the original source URL where the tournament data was extracted from

ALTER TABLE tournaments 
ADD COLUMN IF NOT EXISTS source_url TEXT;

CREATE INDEX IF NOT EXISTS idx_tournaments_source_url ON tournaments(source_url);

-- Add comment
COMMENT ON COLUMN tournaments.source_url IS 'Original URL where tournament data was extracted from (e.g., PDF or HTML page)';

