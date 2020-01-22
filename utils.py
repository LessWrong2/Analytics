import pandas as pd
import numpy as np
import datetime
import pytz
import logging
from functools import wraps
import configparser
from multiprocessing import cpu_count, Pool
from selectolax.parser import HTMLParser


def get_config_field(section, field):
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config[section][field]


def print_and_log(message):
    #TO-DO configure logger to go to both file and stdout, putt timestamp in there, also capture stderr somehow . . .

    logging.basicConfig(filename=get_config_field('LOGGING', 'file'), level=logging.DEBUG)

    print(message) #TO-DO: Refactor to do print within logging statement, then don't need this extra wrapper
    logging.debug(message)


def timed(func):
    """This decorator prints the start, end, and execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        def get_local_time(tz='US/Pacific'):
            return datetime.datetime.now(pytz.timezone('US/Pacific'))

        start = get_local_time()
        print_and_log('{} started at {}'.format(func.__name__, start.strftime('%Y-%m-%d %H:%M:%S')))

        result = func(*args, **kwargs)
        end = get_local_time()
        print_and_log('{} finished at {}'.format(func.__name__, end.strftime('%Y-%m-%d %H:%M:%S')))
        print_and_log("{} ran in {}\n".format(func.__name__, end - start))

        return result

    return wrapper


def mem_and_info(df):
    """Convenience function to display the memory usage and data types of dataframes during development"""

    print((df.memory_usage(deep=True) / 2 ** 20).sum().round(1))
    print()
    a = (df.memory_usage(deep=True) / 2 ** 20).round(1).to_frame('memory')  # .to_frame('memory').merge
    b = df.dtypes.to_frame('dtype')
    c = df.isnull().mean().to_frame('percent_null').round(3)
    print(a.merge(b, left_index=True, right_index=True)
          .merge(c, left_index=True, right_index=True)
          .sort_values('memory', ascending=False))
    print()  # creates space when running repeatedly


def parallelize_dataframe(df, func, n_cores=None):

    if not n_cores:
        n_cores = cpu_count()

    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def get_valid_users(dfs, required_minimum_posts_views=None):
    dfu = dfs['users']

    valid_users = dfu[(~dfu['banned'])&(~dfu['deleted'])]
    if required_minimum_posts_views:
        return valid_users[valid_users['num_distinct_posts_viewed']>=required_minimum_posts_views]
    else:
        return valid_users


def get_valid_posts(dfs, required_upvotes=None):
    dfp = dfs['posts']

    valid_posts = dfp[(~dfp['draft'])&(~dfp['legacySpam'])&(~dfp['authorIsUnreviewed'])&(dfp['status']==2)]
    if required_upvotes:
        return valid_posts[valid_posts[['smallUpvote', 'bigUpvote']].sum(axis=1) >= required_upvotes+1]
    else:
        return valid_posts


def get_valid_comments(dfs):
    dfc = dfs['comments']

    return dfc[(dfc['userId']!='pgoCXxuzpkPXADTp2') #remove GPT-2
               &(dfc['userId'].isin(get_valid_users(dfs)['_id']))&(~dfc['deleted'])]


def get_valid_votes(dfs):
    dfp = dfs['posts']
    dfc = dfs['comments']
    dfv = dfs['votes']

    # removes self-votes
    a = dfv[~dfv['cancelled']].merge(dfp[['_id', 'userId']], left_on='documentId', right_on='_id',
                                     suffixes=['', '_post'], how='left')
    b = a.merge(dfc[['_id', 'userId']], left_on='documentId', right_on='_id', suffixes=['', '_comment'], how='left')
    b['userId_document'] = b['userId_comment'].fillna(b['userId_post'])
    b['self_vote'] = b['userId'] == b['userId_document']
    b = b[b['userId'].isin(get_valid_users(dfs, required_minimum_posts_views=None)['_id'])]

    return b[~b['self_vote']]


def get_valid_views(dfs):
    dpv = dfs['views']  # pv = post-views

    valid_views = dpv[(dpv['userId'].isin(get_valid_users(dfs)['_id']))]
    return valid_views[valid_views['documentId'].isin(get_valid_posts(dfs)['_id'])]


def get_word_count(dfp_full):
    dfp_full['html'] = dfp_full['contents'].str['html']
    dfp_full['text'] = parallelize_dataframe(dfp_full['html'], htmlBody2plaintext, n_cores=cpu_count())

    dfp_full['characters'] = dfp_full['text'].str.len()
    dfp_full['word_count'] = np.round(dfp_full['characters'] / 6.5, 1)

    return dfp_full[['text', 'characters', 'word_count']]



def get_text_selectolax(html):
    if type(html) == str:
        tree = HTMLParser(html)

        if tree.body is None:
            return None

        for tag in tree.css('script'):
            tag.decompose()
        for tag in tree.css('style'):
            tag.decompose()

        text = tree.body.text(separator=' ').replace('\n', '').replace('\t', ' ').replace('\xa0', ' ')
        return text

    else:
        return np.nan


def htmlBody2plaintext(html_series):
    return html_series.apply(lambda x: get_text_selectolax(x) if type(x) == str else np.nan)
