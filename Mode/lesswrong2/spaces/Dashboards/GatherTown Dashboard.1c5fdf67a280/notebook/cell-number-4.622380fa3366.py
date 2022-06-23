def generate_streaks(var='num_sessions'):
  tt = df.set_index(['player_id', 'name', 'week'])[var].unstack(level='week').fillna(0)
  tt['total'] = tt.sum(axis=1)
  session_streaks = tt.sort_values('total', ascending=False) #.head(50)
  return session_streaks

session_streaks = generate_streaks(var='total_approx_duration')