import pandas as pd
import numpy as np
import configparser
import sqlalchemy as sqa
import re
from table_schemas import table_creation_commands
from utils import timed, get_config_field, print_and_log
from io import StringIO
import csv

from IPython.display import display




BASE_PATH = get_config_field('PATHS','base')

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def clean_dataframe_text(df):

    def replace_strings(col, pat, repl):
        df.loc[:, col] = df.loc[:, col].str.replace(pat, repl)

    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]): #this line no longer works for detecting string type columsn and is including date columns...
            try:
                _ = [replace_strings(col, pat, repl) for pat, repl in [('\\', ''), ('\t', '  '), ('\n', '\\n'), ('\r', '\\r')]]
            except Exception:
                pass

    return df


def prepare_users(dfu):
    users_sql_cols = ['_id',
                      'username',
                      'displayName',
                      'createdAt',
                      'postCount',
                      'commentCount',
                      'karma',
                      'afKarma',
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
                      'walledGardenInvite',
                      'hideWalledGardenUI',
                      'email']

    users = dfu.loc[:,users_sql_cols]
    users.loc[:,'afKarma'] = users['afKarma'].fillna(0).astype(int)
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
        'authorIsUnreviewed',
        'most_recent_comment',
        'userAgent',
        'createdAt',
    ]

    posts = dfp[posts_sql_cols].sort_values('postedAt', ascending=False)

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

    return comments


def prepare_views(dpv):
    dpv.loc[:,'documentId'] = dpv.loc[:,'documentId'].str[0:25] #because of one stupid row
    dpv = dpv.sort_values('createdAt')
    return dpv
  
def prepare_votes(dpv):
    dpv = dpv.sort_values('createdAt')
    dpv.loc[:,'authorIds'] =  dpv.loc[:,'authorIds'].astype(str)
    return dpv


def prepare_tags(tags):
    tag_sql_cols = [
        'createdAt',
        '_id',
        'name',
        'slug',
        'deleted',
        'postCount',
        'adminOnly',
        'core',
        'suggestedAsFilter',
        'defaultOrder',
    ]

    tags.loc[:,'postCount'] = tags.loc[:,'postCount'].fillna(0).astype(int)

    return tags[tag_sql_cols]


def prepare_sequences(sequences):
    sequences_sql_cols = [
        '_id',
        'userId',
        'title',
        'createdAt',
        'draft',
        'isDeleted',
        'hidden',
        'schemaVersion',
    ]

    return sequences[sequences_sql_cols]


def get_pg_engine(db='analytics'):

    config = configparser.ConfigParser()
    config.read('config.ini')

    if db == 'analytics':
        db_config_name = "POSTGRESANALYTICSDB"
    elif db == 'prod_db':
        db_config_name = "POSTGRESPRODDB"
    elif db == 'dev_db':
        db_config_name = "POSTGRESDEVDB"

    PG_ACCOUNT = config[db_config_name]['pg_account']
    PG_PASSWORD = config[db_config_name]['pg_password']
    PG_HOST = config[db_config_name]['pg_host']
    PG_DB_NAME = config[db_config_name]['pg_db_name']

    return sqa.create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(PG_ACCOUNT, PG_PASSWORD, PG_HOST, PG_DB_NAME))


def prep_frames_for_db(dfs):
    prep_funcs = {
        'users': prepare_users,
        'posts': prepare_posts,
        'comments': prepare_comments,
        'votes': lambda x: x,
        'views': prepare_views,
        'tags': prepare_tags,
        'tagrels': lambda x: x,
        'sequences': prepare_sequences
    }

    return {coll: prep_funcs[coll](dfs[coll]) for coll in dfs.keys()}


def truncate_or_drop_tables(tables, conn=None, drop=False):

    if type(tables) == str:
        tables_str = tables
    else:
        tables_str = ', '.join(tables)

    if drop:
        command = 'DROP TABLE IF EXISTS {} CASCADE'.format(tables_str)
    else:
        command = 'TRUNCATE {}'.format(tables_str)

    if not conn:
        engine = get_pg_engine()
        with engine.begin() as conn:
            conn.execute(command)
        engine.dispose()
    else:
        with conn.begin():
            conn.execute(command)


def create_tables(tables, conn=None):

    if type(tables) == str:
        tables = [tables]

    if not conn:
        engine = get_pg_engine()
        with engine.begin() as conn:
            [conn.execute(table_creation_commands[table]) for table in tables]
        engine.dispose()
    else:
        with conn.begin():
            [conn.execute(table_creation_commands[table]) for table in tables]


@timed
def bulk_upload_to_pg(df, table_name, conn=None, clean_text=True):

    print_and_log('Upload {}'.format(table_name))
  
    df = df.copy()
    df.loc[:,'birth'] = pd.datetime.now()
    df.columns = df.columns.to_series().apply(camel_to_snake)
    if clean_text:
        df = clean_dataframe_text(df)

    sep = '\t'

    buffer = StringIO()
    buffer.write(df.to_csv(index=None, header=None, sep=sep, na_rep='', escapechar='\\', quoting=csv.QUOTE_NONE))  # Write the Pandas DataFrame as a csv to the buffer
    buffer.seek(0)  # Be sure to reset the position to the start of the stream

    def execute_copy(conn):
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as c:
            c.copy_from(buffer, table_name, columns=df.columns, sep=sep, null='')

    if not conn:
        engine = get_pg_engine()
        with engine.begin() as conn:
            execute_copy(conn)
    else:
        execute_copy(conn)

@timed
def run_pg_pandas_transfer(dfs,
                          tables = ('users', 'posts', 'comments', 'votes', 'tags', 'tagrels'),
                          drop_tables=False,
                           ):


    dfs_prepared = prep_frames_for_db(dfs)

# try:
    engine = get_pg_engine()

    with engine.begin() as conn:

        if drop_tables:
            print_and_log('dropping postgres tables')
        else:
            print_and_log('truncating postgres tables')
        truncate_or_drop_tables(tables, conn=conn, drop=drop_tables)
        if drop_tables:
            create_tables(tables, conn)

        print_and_log('loading tables into postgres db')
        [bulk_upload_to_pg(dfs_prepared[coll], table_name=coll, conn=conn) for coll in tables]

        print_and_log('transaction successful!')

# except:
#     print_and_log('transfer failed')
# finally:
    engine.dispose()


def test_db_contents():
    tables = ['users', 'posts', 'comments', 'votes', 'views']
    engine = get_pg_engine()
    with engine.begin() as conn:
        print({table: conn.execute("SELECT COUNT(*) FROM {}".format(table)).first()[0] for table in tables})
        _ = [display(pd.read_sql("SELECT * FROM {} LIMIT 3".format(coll), conn)) for coll in tables]
    engine.dispose()


def get_db_freshness():
    tables = ['users', 'posts', 'comments', 'votes', 'views', 'tags', 'tagrels', 'sequences', 'urls']
    engine = get_pg_engine()
    with engine.begin() as conn:
        tables_eariest_birth = {table: conn.execute("SELECT MIN(birth) FROM {}".format(table)).first()[0] for table in tables}
    engine.dispose()

    return tables_eariest_birth









