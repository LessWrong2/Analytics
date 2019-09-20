import pandas as pd
import configparser
import sqlalchemy as sqa
from setthetable import table_creation_commands
from utils import timed

from IPython.display import display


def prepare_users(dfu):
    users_sql_cols = ['_id',
                      'username',
                      'displayName',
                      'createdAt',
                      'postCount',
                      'commentCount',
                      'karma',
                      'afKarma',
                      'legacyKarma',
                      'deleted',
                      'banned',
                      'legacy',
                      'shortformFeedId',
                      'signUpReCaptchaRating',
                      'reviewedByUserId',
                      'earliest_activity',
                      'true_earliest',
                      'most_recent_activity',
                      'days_since_active',
                      'total_posts',
                      'earliest_post',
                      'most_recent_post',
                      'num_drafts',
                      'percent_drafts',
                      'total_comments',
                      'earliest_comment',
                      'most_recent_comment',
                      'num_votes',
                      'most_recent_vote',
                      'earliest_vote',
                      'percent_downvotes',
                      'percent_bigvotes',
                      'most_recent_view',
                      'earliest_view',
                      'num_distinct_posts_viewed',
                      'num_days_present_last_30_days',
                      'num_posts_last_30_days',
                      'num_comments_last_30_days',
                      'num_votes_last_30_days',
                      'num_views_last_30_days',
                      'num_distinct_posts_viewed_last_30_days',
                      'num_posts_last_180_days',
                      'num_comments_last_180_days',
                      'num_votes_last_180_days',
                      'num_views_last_180_days',
                      'num_distinct_posts_viewed_last_180_days',
                      'bio',
                      'email']

    users = dfu.loc[:,users_sql_cols]
    users.loc[:,'afKarma'] = users['afKarma'].fillna(0).astype(int)
    users.loc[:,'num_drafts'] = users['num_drafts'].replace(False, 0).fillna(0).astype(int)
    users.loc[:,'percent_drafts'] = users['percent_drafts'].replace(False, 0).fillna(0)
    users.loc[:,'birth'] = pd.datetime.now()
    return users

def prepare_posts(dfp):
    posts_sql_cols = [
        '_id',
        'userId',
        'postedAt',
        'username',
        'displayName',
        'title',
        'af',
        'baseScore',
        'afBaseScore',
        'score',
        'draft',
        'question',
        'isEvent',
        'viewCount',
        'viewCountLogged',
        'clickCount',
        'commentCount',
        'num_comments_rederived',
        'num_distinct_viewers',
        'num_distinct_commenters',
        'wordCount',
        'num_votes',
        'smallUpvote',
        'bigUpvote',
        'smallDownvote',
        'bigDownvote',
        'percent_downvotes',
        'percent_bigvotes',
        'url',
        'slug',
        'canonicalCollectionSlug',
        'website',
        'gw',
        'frontpaged',
        'frontpageDate',
        'curatedDate',
        'status',
        'legacySpam',
        'authorIsUnreviewed',
        'most_recent_comment',
        'userAgent',
        'createdAt',
    ]

    posts = dfp[posts_sql_cols].sort_values('postedAt', ascending=False)
    posts.loc[:,'birth'] = pd.datetime.now()

    return posts

def prepare_comments(dfc):
    comments_sql_cols = [
        '_id',
        'userId',
        'username',
        'displayName',
        'postId',
        'postedAt',
        'af',
        'baseScore',
        'score',
        'answer',
        'parentAnswerId',
        'parentCommentId',
        'wordCount',
        'top_level',
        'gw',
        'num_votes',
        'percent_downvotes',
        'percent_bigvotes',
        'smallUpvote',
        'bigUpvote',
        'smallDownvote',
        'bigDownvote',
        'userAgent',
        'createdAt'
        'deleted'
    ]

    comments = dfc[comments_sql_cols].sort_values('postedAt', ascending=False)
    comments.loc[:,'gw'] = comments['gw'].fillna(False)
    comments.loc[:,'birth'] = pd.datetime.now()
    return comments


def get_pg_engine():
    config = configparser.ConfigParser()
    config.read('config.ini')

    PG_ACCOUNT = config['POSTGRESDB']['pg_account']
    PG_PASSWORD = config['POSTGRESDB']['pg_password']
    PG_HOST = config['POSTGRESDB']['pg_host']
    PG_DB_NAME = config['POSTGRESDB']['pg_db_name']

    return sqa.create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(PG_ACCOUNT, PG_PASSWORD, PG_HOST, PG_DB_NAME))


def prep_frames_for_db(dfs):
    prep_funcs = {
        'users': prepare_users,
        'posts': prepare_posts,
        'comments': prepare_comments
    }

    [prep_funcs[coll](dfs[coll])
         .to_csv('/home/ubuntu/lesswrong-analytics/analytics_data_files/export/{}.csv'.format(coll),
                                                                                       index=False) for coll in
     ['users', 'posts', 'comments']]


def truncate_or_drop_tables(tables, conn, drop=False):
    if type(tables) == str:
        tables_str = tables
    else:
        tables_str = ', '.join(tables)

    if drop:
        conn.execute('DROP TABLE IF EXISTS {}'.format(tables_str))
    else:
        conn.execute('TRUNCATE {}'.format(tables_str))


def create_tables(tables, conn):
    if type(tables) == str:
        tables = [tables]

    [conn.execute(table_creation_commands[table]) for table in tables]

def load_csvs_to_pg(date_str, conn):
    for coll in ['votes', 'views']:
        sql = "COPY {} FROM '/home/ubuntu/lesswrong-analytics/analytics_data_files/processed/{}/{}.csv' DELIMITER ',' CSV HEADER;".format(
            coll, date_str, coll)
        print(sql)
        conn.execute(sql)

    for coll in ['posts', 'comments', 'users']:
        sql = "COPY {} FROM '/home/ubuntu/lesswrong-analytics/analytics_data_files/export/{}.csv' DELIMITER ',' CSV HEADER;".format(
            coll, coll)
        print(sql)
        conn.execute(sql)

@timed
def run_pg_pandas_transfer(dfs, date_str):
    tables = ['users', 'posts', 'comments', 'votes', 'views']

    prep_frames_for_db(dfs)

    engine = get_pg_engine()

    conn = engine.connect()
    transaction = conn.begin()
    print('truncating postgres tables')
    truncate_or_drop_tables(tables, conn, drop=False)
    print('loading csv\'s into postgres db')
    load_csvs_to_pg(date_str, conn)
    transaction.commit()
    print('transaction finished')
    conn.close()


def test_db_contents():
    tables = ['users', 'posts', 'comments', 'votes', 'views']
    engine = get_pg_engine()
    conn = engine.connect()
    _ = [display(pd.read_sql("SELECT * FROM {} LIMIT 3".format(coll), conn)) for coll in tables]
    conn.close()










