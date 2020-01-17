import pandas as pd
import configparser
import sqlalchemy as sqa
from setthetable import table_creation_commands
from utils import timed, get_config_field
from io import StringIO

from IPython.display import display

BASE_PATH = get_config_field('PATHS','base')

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
        'deleted',
        'createdAt'
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
        'comments': prepare_comments,
        'votes': lambda x: x,
        'views': lambda x: x
    }

    return {coll: prep_funcs[coll](dfs[coll]) for coll in ['users', 'posts', 'comments', 'votes', 'views']}


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

def load_csvs_to_pg(dfs, conn): #date_str

    for coll in ['users', 'posts', 'comments', 'votes', 'views']:
        bulk_upload_to_pg(dfs[coll], table_name=coll, conn=conn)

    # for coll in ['votes', 'views']:
    #     sql = "COPY {} FROM '{}processed/{}/{}.csv' DELIMITER ',' CSV HEADER;".format(coll, BASE_PATH, date_str, coll)
    #     print(sql)
    #     conn.execute(sql)
    #
    # for coll in ['posts', 'comments', 'users']:
    #     sql = "COPY {} FROM '{}export/{}.csv' DELIMITER ',' CSV HEADER;".format(coll, BASE_PATH, coll)
    #     print(sql)
    #     conn.execute(sql)

@timed
def bulk_upload_to_pg(df, table_name, conn):

    sep = '\t'

    buffer = StringIO()
    buffer.write(df.to_csv(index=None, header=None, sep=sep))  # Write the Pandas DataFrame as a csv to the buffer
    buffer.seek(0)  # Be sure to reset the position to the start of the stream

    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as c:
        c.copy_from(buffer, table_name, columns=df.columns, sep=sep)
        dbapi_conn.commit()


@timed
def run_pg_pandas_transfer(dfs, date_str):
    tables = ['users', 'posts', 'comments', 'votes', 'views']

    dfs_prepared = prep_frames_for_db(dfs)

    engine = get_pg_engine()
    conn = engine.connect()

    transaction = conn.begin()

    print('truncating postgres tables')
    truncate_or_drop_tables(tables, conn, drop=False)

    print('loading tables into postgres db')
    [bulk_upload_to_pg(dfs[coll], table_name=coll, conn=conn) for coll in tables]
    # load_csvs_to_pg(date_str, conn)

    transaction.commit()
    print('transaction finished')
    conn.close()


def test_db_contents():
    tables = ['users', 'posts', 'comments', 'votes', 'views']
    engine = get_pg_engine()
    conn = engine.connect()
    _ = [display(pd.read_sql("SELECT * FROM {} LIMIT 3".format(coll), conn)) for coll in tables]
    print({table: conn.execute("SELECT COUNT(*) FROM {}".format(table)).first()[0] for table in tables})

    conn.close()










