import datetime
import pytz
import logging
from functools import wraps
import configparser

def get_config_field(section, field):
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config[section][field]


def print_and_log(message):
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
