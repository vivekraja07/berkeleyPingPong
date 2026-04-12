-- RLS policies for Berkeley Ping Pong when using the Supabase anon (or authenticated) API role.
--
-- Run this in the Supabase SQL Editor. Safe to re-run: drops and recreates named policies.
--
-- Important: This file intentionally does NOT include GRANT EXECUTE on functions. If those
-- grants fail (wrong signature / missing function), some clients roll back the whole script
-- and you end up with no policies — including INSERT on tournaments (import then fails with
-- 42501). Grants live in sql/rls_function_grants.sql; run that separately after this file.
--
-- Scope:
--   - Base tables: tournaments, players, round_robin_groups, player_tournament_stats,
--     player_rating_history, matches.
--   - Views (match_results_view, player_rating_chart_view, player_stats_view) use
--     security_invoker: underlying table policies apply.
--
-- Writes are allowed for anon + authenticated so server-side import (GitHub Actions) using
-- the anon key keeps working. Never expose the anon key in browser JavaScript.

-- =============================================================================
-- Explicit policies (SELECT / INSERT / UPDATE / DELETE) — avoids FOR ALL edge cases
-- =============================================================================

DO $$
DECLARE
  t text;
  tables text[] := ARRAY[
    'tournaments',
    'players',
    'round_robin_groups',
    'player_tournament_stats',
    'player_rating_history',
    'matches'
  ];
BEGIN
  FOREACH t IN ARRAY tables
  LOOP
    EXECUTE format($f$
      ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;

      DROP POLICY IF EXISTS "berkeley_ping_pong_api_all" ON public.%I;
      DROP POLICY IF EXISTS "berkeley_ping_pong_select" ON public.%I;
      DROP POLICY IF EXISTS "berkeley_ping_pong_insert" ON public.%I;
      DROP POLICY IF EXISTS "berkeley_ping_pong_update" ON public.%I;
      DROP POLICY IF EXISTS "berkeley_ping_pong_delete" ON public.%I;

      CREATE POLICY "berkeley_ping_pong_select" ON public.%I
        FOR SELECT TO anon, authenticated USING (true);

      CREATE POLICY "berkeley_ping_pong_insert" ON public.%I
        FOR INSERT TO anon, authenticated WITH CHECK (true);

      CREATE POLICY "berkeley_ping_pong_update" ON public.%I
        FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);

      CREATE POLICY "berkeley_ping_pong_delete" ON public.%I
        FOR DELETE TO anon, authenticated USING (true);
    $f$, t, t, t, t, t, t, t, t, t, t);
  END LOOP;
END $$;

-- =============================================================================
-- Optional: RLS on materialized views (only if you enabled RLS on these relations)
-- =============================================================================
--
-- ALTER MATERIALIZED VIEW public.player_rankings_view ENABLE ROW LEVEL SECURITY;
-- DROP POLICY IF EXISTS "berkeley_ping_pong_mv_select" ON public.player_rankings_view;
-- CREATE POLICY "berkeley_ping_pong_mv_select" ON public.player_rankings_view
--   FOR SELECT TO anon, authenticated USING (true);
--
-- ALTER MATERIALIZED VIEW public.player_match_stats_view ENABLE ROW LEVEL SECURITY;
-- DROP POLICY IF EXISTS "berkeley_ping_pong_mv_select" ON public.player_match_stats_view;
-- CREATE POLICY "berkeley_ping_pong_mv_select" ON public.player_match_stats_view
--   FOR SELECT TO anon, authenticated USING (true);
