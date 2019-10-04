import pandas as pd
import numpy as np
import datetime
import pytz
import logging
from functools import wraps
import configparser
from multiprocessing import cpu_count, Pool


def get_config_field(section, field):
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config[section][field]


def print_and_log(message):
    #TO-DO configure logger to go to both file and stdout, putt timestamp in there, also capture stderr somehow . . .

    logging.basicConfig(filename=get_config_field('LOGGING', 'file'), level=logging.DEBUG)

    print(message)
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

    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def get_valid_users(dfu):
    return dfu[(~dfu['banned'])&(~dfu['deleted'])&(dfu['num_distinct_posts_viewed']>=5)]


def get_valid_posts(dfp):
    return dfp[(dfp[['smallUpvote', 'bigUpvote']].sum(axis=1) >= 2)&~dfp['draft']&~dfp['legacySpam']]


def get_valid_comments(dfc):
    return dfc[dfc['userId']!='pgoCXxuzpkPXADTp2'] #remove GPT-2


def get_valid_non_self_votes(dfv, dfp, dfc, dfu):
    a = dfv[~dfv['cancelled']].merge(dfp[['_id', 'userId']], left_on='documentId', right_on='_id',
                                     suffixes=['', '_post'], how='left')
    b = a.merge(dfc[['_id', 'userId']], left_on='documentId', right_on='_id', suffixes=['', '_comment'], how='left')
    b['userId_document'] = b['userId_comment'].fillna(b['userId_post'])
    b['self_vote'] = b['userId'] == b['userId_document']
    b = b[b['userId'].isin(dfu[(dfu['signUpReCaptchaRating'].fillna(1)>0.3)&(~dfu['banned'])]['_id'])]

    return b[~b['self_vote']]


def get_valid_votes(dfv, dfp, dfc, dfu):
    return get_valid_non_self_votes(dfv,dfp, dfc, dfu)
