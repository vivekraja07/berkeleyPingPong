-- Add name column to tournaments table if it doesn't exist
-- Run this in your Supabase SQL Editor

ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS name TEXT;

-- Optional: Add a unique constraint if you want to prevent duplicate tournament names
-- ALTER TABLE tournaments ADD CONSTRAINT tournaments_name_unique UNIQUE (name);

-- Optional: Make it required (NOT NULL) if you want
-- ALTER TABLE tournaments ALTER COLUMN name SET NOT NULL;

