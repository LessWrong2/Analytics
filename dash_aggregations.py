import pandas as pd
import pickle

from dataclasses import dataclass
from typing import List, Tuple
from datetime import datetime, timedelta
from google_analytics_ops import get_traffic_metrics
from utils import get_valid_users, get_valid_posts, get_valid_comments, get_valid_votes, get_valid_views, print_and_log, timed, get_config_field
from karmametric import compute_karma_metric

BASE_PATH = get_config_field('PATHS','base')

# Configurables
minimum_post_views_for_users = 1
post_required_upvotes = 1

# Abstraction for specifying each plot. All needed fields for generate_timeseries
@dataclass
class PlotSpec:
    title: str
    data: pd.DataFrame
    color: str
    date_column: str
    period: str = 'W'
    moving_averages: List[int] = (1, 4)
    agg_func: str = 'size'
    agg_column: str = 'dummy'
    start_date: datetime = datetime.today().date() - timedelta(14)
    end_date: datetime = datetime.today().date()
    size: Tuple[int, int] = (800, 400)
    remove_last_periods: int = 1


# This structure contains the "business logic" of each plot and uses them to load data and generate actual plot_spec objects that can generate plots.
def generate_specs(collections):

    allVotes, _, _ = compute_karma_metric(collections) # calculate karma metric
    traffic_metrics = get_traffic_metrics()

    plot_specs = [
        PlotSpec(
            title='Karma Metric',
            data=allVotes,
            date_column='votedAt',
            agg_func='sum',
            agg_column='effect',
            color='red',
        ),
        PlotSpec(
            title='Accounts Created, {}+ posts_viewed'.format(minimum_post_views_for_users),
            data=get_valid_users(collections, required_minimum_posts_views=minimum_post_views_for_users),
            date_column='true_earliest',
            color='grey',
        ),
        PlotSpec(
            title='Num Logged-In Users',
            data=collections['views'][collections['views']['userId'].isin(get_valid_users(collections)['_id'])],
            date_column='createdAt',
            color='black',
            agg_func='nunique',
            agg_column='userId',
        ),
        PlotSpec(
            title='Num Posts with 2+ Upvotes',
            data=get_valid_posts(collections, required_upvotes=post_required_upvotes),
            date_column='postedAt',
            color='blue',
        ),
        PlotSpec(
            title='Num Unique Posters',
            data=get_valid_posts(collections, required_upvotes=post_required_upvotes),
            date_column='postedAt',
            color='darkblue',
            agg_func='nunique',
            agg_column='userId'
        ),
        PlotSpec(
            title='Num Comments',
            data=get_valid_comments(collections),
            date_column='postedAt',
            color='green'
        ),
        PlotSpec(
            title='Num Unique Commenters',
            data=get_valid_comments(collections),
            date_column='postedAt',
            color='darkgreen',
            agg_func='nunique',
            agg_column='userId'
        ),
        PlotSpec(
            title='Num Votes (excluding self-votes)',
            data=get_valid_votes(collections),
            date_column='votedAt',
            color='orange',
        ),
        PlotSpec(
            title='Num Unique Voters',
            data=get_valid_votes(collections),
            date_column='votedAt',
            color='darkorange',
            agg_func='nunique',
            agg_column='userId'
        ),
        PlotSpec(
            title='Num Logged-In Post Views',
            data=(get_valid_views(collections)
                  .assign(hour=lambda x: x['createdAt'].dt.round('H'))
                  .drop_duplicates(subset=['userId', 'documentId', 'hour'])),
            date_column='createdAt',
            color='red',
        ),
        PlotSpec(
            title='GA Users',
            data=traffic_metrics,
            date_column='date',
            agg_func='sum',
            agg_column='ga:users',
            color='teal'
        )
    ]

    return plot_specs


def resample_timeseries(title, data, date_column, agg_column, agg_func, period, moving_average=1,
                        remove_last_periods=1):
    """Assumes date column is already index"""
    return (data
                .set_index(date_column)
                .assign(dummy=1)
                .resample(period)[agg_column]
                .agg(agg_func)
                .rolling(moving_average)
                .mean()
                .to_frame(title)
                .round(1)
                .reset_index()
                .iloc[:-remove_last_periods]
                )


@timed
def generate_timeseries_dict(plot_specs, periods=['D', 'W', 'M'], moving_averages=[1, 4, 7, 28]):
    return {
        (spec.title, pr, ma): resample_timeseries(
            title=spec.title,
            data=spec.data,
            date_column=spec.date_column,
            agg_column=spec.agg_column,
            agg_func=spec.agg_func,
            period=pr,
            moving_average=ma,
        )
        for spec in plot_specs for pr in periods for ma in moving_averages
    }


@timed
def run_dash_aggregations_pipeline(collections, date_str):

    plot_specs = generate_specs(collections)
    timeseries_dict = generate_timeseries_dict(plot_specs)


    directory = BASE_PATH + '{folder}/{date}'.format(folder='processed', date=date_str)
    print_and_log('Writing timeseries_dict to disk.')
    pickle.dump(plot_specs, open(directory + '/plot_specs.p', 'wb'))
    print_and_log('Writing plot_specs to disk.')
    pickle.dump(timeseries_dict, open(directory + '/timeseries_dict.p', 'wb'))

    print_and_log('Writing timeseries_dict and plot_specs to disk completed')

