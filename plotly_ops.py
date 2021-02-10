import pandas as pd
from chart_studio.tools import set_credentials_file
import chart_studio.plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from lwdash import *
from utils import get_config_field, timed, get_valid_users, get_valid_posts, get_valid_comments, get_valid_votes, get_valid_views
from gspread_pandas import Spread
import numpy as np


def generate_annotation_object(index, x, y, text):
    """Creates a single annotations object for plotly plots"""

    def calc_offset(i):
        """takes integer index as input, alternates positive negative on each number,
        every second number changes magnitude"""
        return (-1)**(i % 2)*((i//2 % 2 == 0)*75+75)

    return go.layout.Annotation(
        x=x,
        y=y.loc[x],
        xref="x",
        yref='y',
        # axref='x',
        # ayref='y',
        text=text,
        showarrow=True,
        arrowhead=7,
        arrowwidth=1,
        ax=0,
        ay=calc_offset(index),
        font={'color': 'blue'},
        clicktoshow='onoff'
        #         textangle=345
    )


def get_events_sheet(only_top=True):
    spreadsheet_user = get_config_field('GSHEETS', 'user')
    s = Spread(user=spreadsheet_user, spread='Release & PR Events', sheet='Events', create_spread=False, create_sheet=False)
    events = s.sheet_to_df()
    events.index = pd.to_datetime(events.index)
    events = events.reset_index().reset_index().set_index('date')
    events['top'] = events['top']=='TRUE'
    if only_top:
        events = events[events['top']]

    return events


def timeseries_plot(datapoints, title='missing', color='yellow', start_date=None, end_date=None, date_col='postedAt',
                    size=(700, 400), online=False, remove_last_periods=1, annotations=False, unique_on=None,
                    period='D', moving_average_lengths=[1, 7], widths={1: 0.5, 7: 1.5, 28: 3},
                    hidden_by_default=[], ymin=0):
    if not end_date:
        end_date = pd.datetime.today()

    period_dict = {'D': 'day', 'W': 'week', 'M': 'month', 'Y': 'year'}
    period_dictly = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Y': 'yearly'}

    timeseries_dict = {ma:
                           (datapoints
                                .set_index(date_col)
                                .assign(dummy=1)
                                .resample(period)
                            [unique_on if unique_on else 'dummy']
                                .agg('nunique' if unique_on else 'size')
                                .rolling(ma)
                                .mean()
                                .round(1)
                                .to_frame(title)
                                .reset_index()
                                .iloc[:-remove_last_periods])
                       for ma in moving_average_lengths}

    data = [
        go.Scatter(x=timeseries[date_col],
                   y=timeseries[title],
                   line={'color': color, 'width': widths[ma]},
                   name='{} {} avg'.format(ma, period_dict[period]),
                   visible=True if not ma in hidden_by_default else 'legendonly'
                   )
        for ma, timeseries in timeseries_dict.items()

    ]

    layout = go.Layout(
        autosize=True, width=size[0], height=size[1],
        title=title,
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [ymin, timeseries_dict[np.min(moving_average_lengths)].set_index(date_col)[start_date:][
            title].max() * 1.05],
               'title': title},
        #         annotations=annotations
    )

    fig = go.Figure(data=data, layout=layout)

    if online:
        py.iplot(fig, filename=title)
    else:
        iplot(fig, filename=title)

    return timeseries_dict


def plot_table(df, title='missing', online=False):
    trace = go.Table(
        header=dict(values=df.columns.tolist(),
                    fill={'color': '#C2D4FF'},
                    align=['left'] * 2),
        cells=dict(values=[df[col] for col in df.columns],
                   fill={'color': '#F5F8FF'},
                   align=['left'] * 2)
    )

    data = [trace]

    if online:
        py.iplot(data, filename=title)
    else:
        iplot(data, filename=title)


@timed
def run_plotline(dfs, online=False, start_date=None, end_date=None, size=(1000, 400), pr='D', ma=[1, 7],
                  widths={1: 0.75, 7: 3}, annotations=False, hidden_by_default=[]):
    set_credentials_file(username=get_config_field('PLOTLY', 'username'),
                         api_key=get_config_field('PLOTLY', 'api_key'))
    init_notebook_mode(connected=True)

    dpv = dfs['views']  # pv = post-views
    
    minimum_post_views = 1
    valid_users = get_valid_users(dfs, required_minimum_posts_views=minimum_post_views)
    valid_posts = get_valid_posts(dfs, required_upvotes=1)
    valid_comments = get_valid_comments(dfs)
    valid_votes = get_valid_votes(dfs)
    valid_views = get_valid_views(dfs)
    valid_views['hour'] = valid_views['createdAt'].dt.round('H')
    valid_views_deduped = valid_views.drop_duplicates(subset=['userId', 'documentId', 'hour'])

    plotly_args = {'start_date': start_date, 'end_date': end_date, 'period': pr, 'moving_average_lengths': ma,
                   'widths': widths, 'size': size,
                   'online': online, 'annotations': annotations, 'hidden_by_default': hidden_by_default}

    timeseries_plot(title='Accounts Created, {}+ posts_viewed'.format(minimum_post_views), datapoints=valid_users, date_col='true_earliest',
                    color='grey', **plotly_args)
    timeseries_plot(title='Num Logged-In Users', datapoints=dpv[dpv['userId'].isin(valid_users['_id'])],
                    date_col='createdAt', color='black', unique_on='userId', **plotly_args)

    timeseries_plot(title='Num Posts with 2+ upvotes', datapoints=valid_posts, date_col='postedAt', color='blue',
                    **plotly_args)
    timeseries_plot(title='Num Unique Posters', datapoints=valid_posts, date_col='postedAt', color='darkblue',
                    unique_on='userId', **plotly_args)

    timeseries_plot(title='Num Comments', datapoints=valid_comments, date_col='postedAt', color='green', **plotly_args)
    timeseries_plot(title='Num Unique Commenters', datapoints=valid_comments, date_col='postedAt', color='darkgreen',
                    unique_on='userId', **plotly_args)

    timeseries_plot(title='Num Votes (excluding self-votes)', datapoints=valid_votes, date_col='votedAt',
                    color='orange', **plotly_args)
    timeseries_plot(title='Num Unique Voters', datapoints=valid_votes, date_col='votedAt', color='darkorange',
                    unique_on='userId', **plotly_args)

    timeseries_plot(title='Num Logged-In Post Views', datapoints=valid_views_deduped, date_col='createdAt', color='red',
                    **plotly_args)


