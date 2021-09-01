import pickle
import numpy as np
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_aggregations as da
import core_pipeline as cp

from dash.dependencies import Input, Output, State
from flask_caching import Cache
from datetime import datetime, timedelta, date
from utils import timed
from dash_aggregations import generate_specs, BASE_PATH

import logging
import sys
logging.basicConfig(filename='dash_app.log', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

app = dash.Dash(__name__)
app.title = "LW Analytics Dashboard v3.0.1"
server = app.server

cache = Cache(app.server, config={'CACHE_TYPE': 'SimpleCache'})


# Configurables
start_year = 2020 #default start year to display on graphs
min_year = 2009
max_year = datetime.today().year


def format_title(title):
    return title.lower().replace(' ','-').replace("+","plus").replace(',','')

def load_timeseries_dict():
    date_str = cp.get_list_of_dates()[0][-8:]
    directory = da.BASE_PATH + '{folder}/{date}'.format(folder='processed', date=date_str)
    timeseries_dict_file = open(directory + '/timeseries_dict.p', 'rb')
    timeseries_dict = pickle.load(timeseries_dict_file)
    timeseries_dict_file.close()

    return timeseries_dict


@cache.memoize(timeout=36000)
def generate_specs():
    date_str = cp.get_list_of_dates()[0][-8:]
    directory = da.BASE_PATH + '{folder}/{date}'.format(folder='processed', date=date_str)
    plot_specs_file = open(directory + '/plot_specs.p', 'rb')
    plot_specs = pickle.load(plot_specs_file)
    plot_specs_file.close()

    return plot_specs



@timed
def generate_timeseries_plot(
    timeseries_dict, 
    title,
    color,
    date_column,
    start_date,
    end_date, 
    period='day', 
    moving_averages=[1, 7], 
    widths={1: 0.5, 4: 1.7, 7: 1.5, 28: 3},
    size=(700, 500), 
    hidden_by_default=[], 
    ymin=0):
    """Takes in dict containing all traces precomputed plus plot specifications in order to generate figure data"""
    logging.debug('generating graph for %s', title)
    
    period_dict = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}

    traces_dict  = {ma: timeseries_dict[(title, period_dict[period.lower()], ma)] for ma in moving_averages}


    data = [
        go.Scatter(
            x=timeseries[date_column],
            y=timeseries[title],
            line={'color': color, 'width': widths[ma]},
            name='{}-{} avg'.format(ma, period.lower()),
            visible=True if not ma in hidden_by_default else 'legendonly'
            )
        for ma, timeseries in traces_dict.items()
    ]

    layout = go.Layout(
        autosize=True, width=size[0], height=size[1],
        title=title,
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [ymin, traces_dict[ 
            np.min(moving_averages)]
            .set_index(date_column)[start_date:][title]
            .max() * 1.05],
        'title': title},
        template="seaborn",
        font={
            'family': "'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif",
            'size': 14
            },
        title_font={
            'family': "'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif",
            'size': 24
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
                value='Day',
                labelStyle={'display': 'inline-block', 'margin-right': '15px'},
                inputStyle={'width': '20px', 'height': '20px', 'margin-right': '6px'},
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
                value=[1, 7],
                inputStyle={'width': '20px', 'height': '20px', 'margin-right': '6px'},
                labelStyle={'display': 'inline-block', 'margin-right': '15px'}
            ),
            html.Div("Select Date Range", className="control-labels"),
            dcc.DatePickerRange(
                className='date-range-picker',
                id='date-picker-range',
                display_format="YYYY-MM-DD",
                min_date_allowed=date(2009, 1, 1),
                max_date_allowed=date.today(),
                initial_visible_month=date(2021, 1, 1),
                start_date=date.today() - timedelta(28),
                end_date=date.today()
            ),
            html.Div(className='update-button-container', children=[
                html.Button('Update Graphs', id='update-button', className='update-button'),
                ]
            )
        ],
        ),
        html.Div(className='graphs', children=[
            dcc.Graph(
                    id=format_title(spec.title),
                    figure=generate_timeseries_plot(
                        timeseries_dict=load_timeseries_dict(),
                        title=spec.title,
                        color=spec.color,
                        period='Day',
                        moving_averages=[1,7],
                        date_column=spec.date_column,
                        start_date=spec.start_date,
                        end_date=spec.end_date,
                    ),
                    className='graph',
                    style={'background-color': 'f0f0f0'} 
                ) for spec in generate_specs()],
                 ),
        dcc.Interval(id='interval-component', interval=3600*1000, n_intervals=0)
    ],
    )   
])


#Redraw all graphs upon changing inputs
@app.callback(
    *[Output(format_title(spec.title), 'figure') for spec in generate_specs()],
    [
        Input('update-button', 'n_clicks'),
        Input('interval-component', 'n_intervals')
    ],
    state=[
        State('period-radio-buttons', 'value'),
        State('moving-averages-checkboxes', 'value'),
        State('date-picker-range', 'start_date'),
        State('date-picker-range', 'end_date'),
    ], prevent_initial_callback=False)
def update_graphs(n_clicks, n_intervals, period, moving_averages, start_date, end_date):
    logging.debug('graphs updating!')
    logging.debug('n_clicks: '.format(n_clicks))
    logging.debug('n_intervals: '.format(n_intervals))
    graphs = [
        generate_timeseries_plot(
            timeseries_dict=load_timeseries_dict(),
            title=spec.title,
            color=spec.color,
            date_column=spec.date_column,
            period=period,
            moving_averages=moving_averages,
            start_date=start_date,
            end_date=end_date
        ) for spec in generate_specs()]
    
    return graphs

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)