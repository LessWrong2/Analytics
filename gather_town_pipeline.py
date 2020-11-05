import pandas as pd
import numpy as np
from utils import get_collection, get_mongo_db_object
from postgres_ops import get_pg_engine, truncate_or_drop_tables, bulk_upload_to_pg
from utils import timed


def get_raw_gt_data():
    gt_raw = get_collection('lwevents', query_filter={"name": "gatherTownUsersCheck"}, db=get_mongo_db_object())
    gt_raw['time'] = gt_raw['properties'].str['time']
    gt_raw['gatherTownUsers'] = gt_raw['properties'].str['gatherTownUsers']
    return gt_raw

def generate_checks_table(gt_raw):

    gt_user_col = gt_raw.set_index('createdAt')['gatherTownUsers']

    checks_aggregator = []
    for timestamp, data in gt_user_col.iteritems():
        checks_aggregator.append(pd
                      .DataFrame
                      .from_dict(data)
                      .T
                      .assign(timestamp=timestamp))

    checks = (pd
          .concat(checks_aggregator, axis=0, sort=False)
          .sort_values(['name', 'timestamp'])
          .reset_index(drop=True)
          )

    checks['timestamp'] = checks['timestamp'] - pd.Timedelta('7 hours')
    checks['elapsed_min'] = checks.groupby('name')['timestamp'].diff(1).dt.total_seconds().div(60).round(2)
    checks['first_visit'] = False
    checks.loc[checks['elapsed_min'].isnull(), 'first_visit'] = True

    checks['new_session'] = False
    checks.loc[checks['elapsed_min'] > 30, 'new_session'] = True
    checks['session_no'] = checks.groupby('name')['new_session'].cumsum().fillna(0).astype(int)

    checks['busy'] = checks['busy'].fillna(0).astype(int)

    checks['x_coord'] = checks['location'].str['x']
    checks['y_coord'] = checks['location'].str['y']
    checks['map'] = checks['location'].str['map']

    checks['lw_team'] = checks['name'].isin(['Raemon', 'Habryka', 'Ruby', 'Ben Pace', 'jimrandomh'])

    for col in ['audio', 'video', 'blocked']:
        checks[col] = checks[col].fillna(False)

    checks_output_cols = ['timestamp', 'name', 'busy', 'audio', 'video', 'blocked', 'elapsed_min', 'first_visit',
                          'new_session', 'session_no', 'lw_team']

    return checks[checks_output_cols]


def generate_presence_table(checks):
    presence = (checks
                .groupby(['timestamp'])['name']
                .agg(['nunique', lambda x: x.tolist()])
                .fillna(0)
                .sort_index()
                )

    #     presence['total_present'] = presence[[F.sum(axis=1)
    presence.columns = ['num_present', 'names']

    return presence


def generate_sessions_table(checks):

    presence = generate_presence_table(checks)

    sessions = (checks
        .merge(presence, on='timestamp')
        .sort_values('timestamp')
        .groupby(['name', 'session_no'])
        .agg({
        'audio': 'size',
        'elapsed_min': lambda x: x.iloc[1:].max(),
        'timestamp': ['min', 'max'],
        'first_visit': np.any,
        'lw_team': np.any,
        'num_present': [lambda x: x.iloc[0]<2, lambda x: x.iloc[-1]<2, lambda x: (x>1).mean().round(2)],
        'names': lambda x: list(np.unique(x.sum()))
    })
    )

    sessions.columns = ['num_checks', 'max_gap', 'start_time', 'end_time', 'first_visit', 'lw_team', 'alone_at_start',
                        'alone_at_end', 'percent_accompanied', 'concurrent_visitors']
    sessions['approx_duration'] = (sessions['end_time'] - sessions['start_time']).dt.total_seconds().div(60).round(2)
    sessions = sessions.reset_index()
    sessions.apply(lambda x: x['concurrent_visitors'].remove(x['name']), axis=1) #.sample(10)
    sessions['concurrent_visitors'] = sessions['concurrent_visitors'].astype(str)

    return sessions


def generate_users_table(sessions):

    user_stats = (sessions
                  .groupby('name')
                  .agg({
        'session_no': 'size',
        'num_checks': 'sum',
        'start_time': [lambda x: x.dt.date.nunique(), 'min'],
        'end_time': 'max',
        'approx_duration': ['sum', 'mean', 'median', 'max', 'min'],
        'lw_team': np.any
    })
                  .round(2)
                  .reset_index()
                  )

    user_stats.columns = ['name', 'num_sessions', 'num_checks', 'num_distinct_days', 'first_seen', 'last_seen',
                          'total_approx_duration', 'mean_session_length', 'median_session_length',
                          'max_session_length', 'min_session_length', 'lw_team']

    return user_stats

@timed
def run_gather_town_pipeline():

    raw_gt_data = get_raw_gt_data()
    checks = generate_checks_table(raw_gt_data)
    sessions = generate_sessions_table(checks)
    user_stats = generate_users_table(sessions)

    gt_tables = {
        'gather_town_checks': checks,
        'gather_town_sessions': sessions,
        'gather_town_users': user_stats
    }

    engine = get_pg_engine()

    with engine.begin() as conn:
        truncate_or_drop_tables(tables=gt_tables.keys(), conn=conn, drop=False)
        [bulk_upload_to_pg(df, table, conn=conn, clean_text=False) for table, df in gt_tables.items()]

    engine.dispose()
