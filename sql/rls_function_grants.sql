-- GRANT EXECUTE on RPCs used by the Flask backend. Run in Supabase SQL Editor *after*
-- sql/rls_public_policies.sql so a failed GRANT cannot roll back your RLS policies.
--
-- If a line errors (function name/signature differs in your DB), remove that line and re-run.

GRANT EXECUTE ON FUNCTION public.get_player_rating_history(text) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_tournament_stats() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refresh_player_rankings_view() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refresh_player_match_stats_view() TO anon, authenticated;
