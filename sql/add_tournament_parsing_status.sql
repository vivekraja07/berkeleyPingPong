-- Add parsing status fields to tournaments table
-- This allows us to track tournaments that couldn't be fully parsed but still have a URL and date

ALTER TABLE tournaments 
ADD COLUMN IF NOT EXISTS parsing_status TEXT DEFAULT 'success',
ADD COLUMN IF NOT EXISTS parse_error TEXT;

-- Add comment
COMMENT ON COLUMN tournaments.parsing_status IS 'Status of parsing: success, parsing_failed, validation_failed, or db_error';
COMMENT ON COLUMN tournaments.parse_error IS 'Error message if parsing/validation failed';

-- Create index for filtering
CREATE INDEX IF NOT EXISTS idx_tournaments_parsing_status ON tournaments(parsing_status);

