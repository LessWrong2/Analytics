from multiprocessing import Event
from matplotlib.pyplot import cla
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html

from dash.dependencies import Input, Output, State
from flask_caching import Cache
from dataclasses import dataclass, asdict
from typing import List, Tuple, Optional
from datetime import datetime
from core_pipeline import load_from_file
from utils import get_valid_users, get_valid_posts, get_valid_comments, get_valid_votes, get_valid_views, print_and_log, timed
from karmametric import compute_karma_metric

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

import logging
import sys
logging.basicConfig(filename='dash_app.log', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "LW Analytics Dashboard v3.0.1"
server = app.server

cache = Cache(app.server, config={'CACHE_TYPE': 'SimpleCache'})

# Configurables
minimum_post_views_for_users = 1
post_required_upvotes = 1
start_year = 2020 #default start year to display on graphs
min_year = 2009
max_year = datetime.today().year

# A big ugly. Attempted performance optimization–only use needed columns. Need to determine whether it actually makes a difference.
used_columns = {
    'users': ['true_earliest', 'banned', 'deleted', '_id'],
    'posts': ['userId', 'draft', 'legacySpam', 'authorIsUnreviewed', 'status', 'postedAt', 'smallUpvote', 'bigUpvote'],
    'comments': ['postedAt', 'userId', 'deleted'],
    'votes': ['votedAt', 'collectionName', 'userId'],
    'views': ['userId', 'documentId', 'createdAt']
}


# Abstraction for specifying each plot. All needed fields for generate_timeseries
@dataclass
class PlotSpec:
    title: str
    data: pd.DataFrame
    color: str
    date_column: str
    period: str = 'week'
    moving_averages: List[int] = (1, 4) 
    agg_func: str = 'size'
    agg_col: str = 'dummy'
    start_date: datetime = datetime(2020, 1, 1)
    end_date: datetime = datetime.today().date()
    size: Tuple[int, int] = (800, 400)
    remove_last_periods: int = 1
        

# This structure contains the "business logic" of each plot and uses them to load data and generate actual plot_spec objects that can generate plots.
@cache.memoize(timeout=3600)
def load_data_generate_specs():
    
    logging.debug('loading and generating data')
  
    collections = load_from_file(date_str='most_recent', coll_names=['users', 'posts', 'comments', 'votes', 'views'])
    allVotes, _, _ = compute_karma_metric(collections) # calculate karma metric
        
    plot_specs = [
        PlotSpec(
            title='Karma Metric',
            data=allVotes,
            date_column='votedAt',
            agg_func='sum',
            agg_col='effect',
            color='red',
        ),
        PlotSpec(
            title='Accounts Created, {}+ posts_viewed'.format(minimum_post_views_for_users),
            data=get_valid_users(collections, required_minimum_posts_views=minimum_post_views_for_users)[used_columns['users']],
            date_column='true_earliest',
            color='grey',
        ),
        PlotSpec(
            title='Num Logged-In Users',
            data=collections['views'][collections['views']['userId'].isin(get_valid_users(collections)['_id'])][used_columns['views']],
            date_column='createdAt',
            color='black',
            agg_func='nunique',
            agg_col='userId',
        ),
        PlotSpec(
            title='Num Posts with 2+ Upvotes', 
            data=get_valid_posts(collections, required_upvotes=post_required_upvotes)[used_columns['posts']],
            date_column='postedAt', 
            color='blue',
        ),
        PlotSpec(
            title='Num Unique Posters',
            data=get_valid_posts(collections, required_upvotes=post_required_upvotes)[used_columns['posts']],
            date_column='postedAt',
            color='darkblue',
            agg_func='nunique',
            agg_col='userId'
        ),
        PlotSpec(
            title='Num Comments',
            data=get_valid_comments(collections)[used_columns['comments']],
            date_column='postedAt',
            color='green'
        ),
        PlotSpec(
            title='Num Unique Commenters',
            data=get_valid_comments(collections)[used_columns['comments']],
            date_column='postedAt',
            color='darkgreen',
            agg_func='nunique',
            agg_col='userId'
        ),
        PlotSpec(
            title='Num Votes (excluding self-votes)',
            data=get_valid_votes(collections)[used_columns['votes']],
            date_column='votedAt',
            color='orange',
        ),
        PlotSpec(
            title='Num Unique Voters',
            data=get_valid_votes(collections)[used_columns['votes']],
            date_column='votedAt', 
            color='darkorange',
            agg_func='nunique',
            agg_col='userId'
        ),
        PlotSpec(
            title='Num Logged-In Post Views', 
            data=(get_valid_views(collections)
                   .assign(hour = lambda x: x['createdAt'].dt.round('H'))
                   .drop_duplicates(subset=['userId', 'documentId', 'hour'])
                 )[used_columns['views']],
            date_column='createdAt', 
            color='red',
        )
    ]
    
    return plot_specs

def format_title(title):
    return title.lower().replace(' ','-').replace("+","plus").replace(',','')

@timed
def generate_timeseries_plot(
    data, 
    title,
    color,
    date_column,
    start_date,
    end_date, 
    period='D', 
    moving_averages=[1, 7], 
    widths={1: 0.5, 4: 1.7, 7: 1.5, 28: 3},
    agg_func='size',
    agg_col='dummy', 
    size=(700, 500), 
    remove_last_periods=1, 
    hidden_by_default=[], 
    ymin=0):
    """Takes in collection dataframes plus plot specification to generate a plotly/dash graphout (layout/data pair)"""
    logging.debug('generating graph for %s', title)
    
    period_dict = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}

    timeseries_dict = { #generate one timeseries for each moving average value TODO: could possibly generate a daily series for each that would be faster for subsequent resamples, except that's hard with the unique-ons
        ma: (data
            .set_index(date_column)
            .assign(dummy=1)
            .resample(period_dict[period.lower()])[agg_col]
            .agg(agg_func)
            .rolling(ma)
            .mean()
            .to_frame(title)
            .round(1)
            .reset_index()
            .iloc[:-remove_last_periods])
        for ma in moving_averages
    }

    data = [
        go.Scatter(
            x=timeseries[date_column],
            y=timeseries[title],
            line={'color': color, 'width': widths[ma]},
            name='{}-{} avg'.format(ma, period.lower()),
            visible=True if not ma in hidden_by_default else 'legendonly'
            )
        for ma, timeseries in timeseries_dict.items()
    ]

    layout = go.Layout(
        autosize=True, width=size[0], height=size[1],
        title=title,
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [ymin, timeseries_dict[ 
            np.min(moving_averages)]
            .set_index(date_column)[start_date:][title]
            .max() * 1.05],
        'title': title},
        template="seaborn",
        font={
            'family': "'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif",
            'size': 12
            },
        title_font={
            'family': "'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif",
            'size': 18
            }
    )

    return {'layout': layout, 'data': data}


# Dash App Layout
app.layout = html.Div([
    html.Div(className='layout', children=[
        html.H1(app.title, className="main-title"),
        html.Div(className='controls', children=[
            html.Div("Aggregation Period", className="control-labels"),
            dcc.RadioItems(
                id='period-radio-buttons',
                options=[{'label': i, 'value': i} for i in ['Day', 'Week', 'Month']],
                value='Week',
                labelStyle={'display': 'inline-block', 'margin-right': '15px'}
            ),
            html.Div("Moving Average Filters", className="control-labels"),
            dcc.Checklist(
                className='moving-average-checkboxes',
                id='moving-averages-checkboxes',
                options=[
                    {'label': '1', 'value': 1},
                    {'label': '4', 'value': 4},
                    {'label': '7', 'value': 7},
                    {'label': '28', 'value': 28},
                ],
                value=[1, 4],
                labelStyle={'display': 'inline-block', 'margin-right': '15px'}
            ),
            html.Div("Select Date Range", className="control-labels"),
            dcc.RangeSlider(
                id='year-range-slider',
                min=min_year,
                max=max_year,
                step=1,
                marks={y: str(y) for y in range(min_year,max_year,1)},
                value=[start_year, datetime.today().year]
            ),
            html.Button('Update Graphs', id='update-button', className='update-button'),
        ],
        ),
        html.Div(className='graphs', children=[
            dcc.Graph(
                    id=format_title(spec.title),
                    figure=generate_timeseries_plot(**asdict(spec)),
                    className='graph',
                    style={'background-color': 'f0f0f0'} 
                ) for spec in load_data_generate_specs()],
        ),
        dcc.Interval(id='interval-component', interval=3600*1000, n_intervals=0)
    ],
    )   
])


#Redraw all graphs upon changing inputs
@app.callback(
    *[Output(format_title(spec.title), 'figure') for spec in load_data_generate_specs()],
    [
        Input('update-button', 'n_clicks'),
        Input('interval-component', 'n_intervals')
    ],
    state=[
        State('period-radio-buttons', 'value'),
        State('moving-averages-checkboxes', 'value'),
        State('year-range-slider', 'value'),
    ], prevent_initial_callback=True)
def update_graphs(n_clicks, n_intervals, period, moving_averages, years):
    logging.debug('graphs updating!')
    logging.debug('n_clicks: '.format(n_clicks))
    logging.debug('n_intervals: '.format(n_intervals))
    graphs = [generate_timeseries_plot(**{
        **asdict(spec), 
        **{ #second dict overwrites original spec dict
            'period':period, 
            'moving_averages':moving_averages, 
            'start_date':datetime(years[0],1,1), 
            'end_date': min(datetime.today(), datetime(years[1],12,31))
        }
    }) for spec in load_data_generate_specs()]
    
    return graphs

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)