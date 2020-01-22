import pandas as pd
from chart_studio.tools import set_credentials_file
import chart_studio.plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from lwdash import *
from utils import get_config_field, timed, get_valid_users, get_valid_posts, get_valid_comments, get_valid_votes


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


def plotly_ts_ma(raw_data=None, resampled_data=None, title='missing', color='yellow', start_date=None,
                 end_date=pd.datetime.today(), date_col='postedAt', size=(700, 400), online=False,
                 exclude_last_period=True, annotations=False, pr='D', ma=7, ymin=0):

    pr_dict = {'D':'day', 'W':'week', 'M':'month', 'Y':'year'}
    pr_dictly = {'D':'daily', 'W':'weekly', 'M':'monthly', 'Y':'yearly'}

    if resampled_data is None:
        resampled_data = raw_data.set_index(date_col).resample(pr).size().to_frame(title).reset_index()

    dd_ma = resampled_data.set_index(date_col)[title].rolling(ma).mean().round(1).reset_index()

    if exclude_last_period:
        resampled_data = resampled_data.iloc[:]
        dd_ma = dd_ma.iloc[:]

    data = [
        go.Scatter(x=resampled_data[date_col], y=resampled_data[title], line={'color': color, 'width': 0.75}, name='{}-value'.format(pr_dictly[pr])),
        go.Scatter(x=dd_ma[date_col], y=dd_ma[title], line={'color': color, 'width': 3}, name='{} {} avg'.format(ma, pr_dict[pr]))
    ]

    if annotations:
        events = get_events_sheet(only_top=False)
        annotations = [generate_annotation_object(index=index, x=date, y=resampled_data.set_index(date_col)[title], text=event)
                       for date, index, event in events[resampled_data[date_col].min():][['index', 'event']].resample(pr)
                           .agg({'event': lambda x: ';<br>'.join(x.tolist()) if len(x) > 0 else np.nan})
                           .dropna().reset_index().reset_index().set_index('date').itertuples()]
    else:
        annotations = None

    layout = go.Layout(
        autosize = True, width=size[0], height=size[1],
        title= title,
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [ymin, resampled_data.set_index(date_col)[start_date:][title].max() * 1.05],
               'title': title},
        annotations=annotations
    )

    fig = go.Figure(data=data, layout=layout)

    if online:
        py.iplot(fig, filename=title)
    else:
        iplot(fig, filename=title)


def plotly_uniques(raw_data, date_col, title, start_date, end_date, color, size, online=False, annotations=False, pr='D', ma=7):
    dd = raw_data.set_index(date_col)['2009':].resample(pr)['userId'].nunique().to_frame(title).reset_index()
    plotly_ts_ma(resampled_data=dd, title=title, color=color, start_date=start_date, end_date=end_date, date_col=date_col, size=size,
                 online=online, annotations=annotations, pr=pr, ma=ma)


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
def run_plotline(dfs, online=False, start_date=None, end_date=None, size=(1000, 400), pr='D', ma=7, annotations=False):

    set_credentials_file(username=get_config_field('PLOTLY', 'username'),
                                      api_key=get_config_field('PLOTLY', 'api_key'))
    init_notebook_mode(connected=True)

    dpv = dfs['views']  # pv = post-views

    valid_users = get_valid_users(dfs, required_minimum_posts_views=5)
    valid_posts = get_valid_posts(dfs, required_upvotes=1)
    valid_comments = get_valid_comments(dfs)
    valid_votes = get_valid_votes(dfs)

    plotly_args = {'start_date': start_date, 'end_date': end_date, 'pr': pr, 'ma': ma, 'size': size,
                   'online': online, 'annotations': annotations}

    plotly_ts_ma(title='Accounts Created, 5+ posts_viewed', raw_data=valid_users, date_col='true_earliest', color='grey', **plotly_args)
    plotly_uniques(title='Num Logged-In Users', raw_data=dpv[dpv['userId'].isin(valid_users['_id'])], date_col='createdAt', color='black', **plotly_args)

    plotly_ts_ma(title='Num Posts with 2+ upvotes', raw_data=valid_posts, date_col='postedAt', color='blue', **plotly_args)
    plotly_uniques(title='Num Unique Posters', raw_data=valid_posts, date_col='createdAt', color='darkblue', **plotly_args)

    plotly_ts_ma(title='Num Comments', raw_data=valid_comments, date_col='postedAt', color='green', **plotly_args)
    plotly_uniques(title='Num Unique Commenters', raw_data=valid_comments, date_col='postedAt', color='darkgreen', **plotly_args)

    plotly_ts_ma(title='Num Votes (excluding self-votes)', raw_data=valid_votes, date_col='votedAt', color='orange', **plotly_args)
    plotly_uniques(title='Num Unique Voters', raw_data=valid_votes, date_col='votedAt', color='darkorange', **plotly_args)

    plotly_ts_ma(title='Num Logged-In Post Views', raw_data=dpv, date_col='createdAt', color='red', **plotly_args)

    # plot_table(downvote_monitoring(dfv, dfp, dfc, dfu, 2, ), title='Downvote Monitoring', online=online)

