import pandas as pd
import numpy as np
from chart_studio.tools import set_credentials_file
import chart_studio.plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import init_notebook_mode, iplot
from gspread_pandas import Spread
from utils import timed, get_config_field, get_lw_team


def filtered_and_enriched_votes(dfs):
    users = dfs['users']
    posts = dfs['posts']
    comments = dfs['comments']
    votes_raw = dfs['votes']

    excluded_posts = posts[(posts['status'] != 2) | posts['authorIsUnreviewed'] | posts['draft']]['_id']
    lw_team = get_lw_team(users)

    votes_processed = votes_raw[(~votes_raw['userId'].isin(lw_team)) & (~votes_raw['documentId'].isin(excluded_posts))
                & (votes_raw['collectionName'].isin(['Posts', 'Comments'])
                & ~votes_raw['cancelled'])
                & (~votes_raw['documentId'].isin(comments[comments['deleted']]['_id']))
    ].copy()  # votes_processed: filtered votes column
    votes_processed['downvote'] = votes_processed['power'] < 0  # create boolean column for later convenience

    votes_processed.loc[:, 'power_d4'] = votes_processed['power'].copy()  # create a copy of the power (karma) column
    votes_processed.loc[votes_processed['power'] < 0, 'power_d4'] = votes_processed.loc[votes_processed[
                                                           'power'] < 0, 'power'] * 4  # multiply all rows with negative power by 4

    votes_processed = votes_processed.sort_values('votedAt')
    votes_processed['voteId'] = (
            votes_processed['documentId'] + votes_processed['userId'] + votes_processed['voteType'].astype(str) +
            votes_processed['votedAt'].astype(int).astype('str')
    ).apply(lambda x: hex(hash(x))).astype('str')
    votes_processed = votes_processed.set_index(votes_processed['voteId'])

    return votes_processed


def run_incremental_vote_algorithm(votes):
    def fancy_power(x, power):
        return np.sign(x) * np.abs(x) ** power

    baseScoresD4 = {}
    docScores = {}
    voteEffects = {}

    for vote in votes.itertuples(index=False, name='Vote'):
        oldScore = fancy_power(baseScoresD4.get(vote.documentId, 0), 1.2)
        newScore = fancy_power(baseScoresD4.get(vote.documentId, 0) + vote.power_d4, 1.2)
        voteEffects[vote.voteId] = newScore - oldScore

        baseScoresD4[vote.documentId] = baseScoresD4.get(vote.documentId, 0) + vote.power_d4
        docScores[vote.documentId] = newScore

    return baseScoresD4, docScores, voteEffects


def compute_karma_metric(dfs):
    allVotes = filtered_and_enriched_votes(dfs)
    baseScoresD4, docScores, voteEffects = run_incremental_vote_algorithm(allVotes)
    allVotes = allVotes.merge(pd.Series(voteEffects).to_frame('effect'), left_index=True, right_index=True)

    return allVotes, baseScoresD4, docScores


def create_trend_frame(initial_value, freq):
    def rate_scaling(freq, value):
        days_in_period = {'D': 1, 'W': 7, 'M': 365 / 12, 'Y': 365}
        return value ** (days_in_period[freq] / 7)

    def growth_series(trend_range, growth_rate, initial_value):
        return [initial_value * growth_rate ** i for i in range(len(trend_range))]

    trend_range = pd.date_range('2019-06-30', '2020-06-30', freq=freq)
    trends = pd.DataFrame(
        data={
            'date': trend_range,
            '5%': growth_series(trend_range, rate_scaling(freq, 1.05), initial_value),
            '7%': growth_series(trend_range, rate_scaling(freq, 1.07), initial_value),
            '10%': growth_series(trend_range, rate_scaling(freq, 1.10), initial_value)
        }
    ).round(1)

    return trends


def plot_karma_metric(allVotes, start_date, end_date, online=False, pr='D', ma=7):
    votes_ts = allVotes.set_index('votedAt').resample(pr)['effect'].sum()
    votes_ts = votes_ts.reset_index().iloc[:-1]
    votes_ts_ma = votes_ts.set_index('votedAt')['effect'].rolling(ma).mean().round(1).reset_index()

    days_in_period = {'D': 1, 'W': 7, 'M': 365 / 12, 'Y': 365}

    # trends = create_trend_frame(days_in_period[pr] * 550, pr)

    # plotly section
    date_col = 'votedAt'
    title = 'effect'
    color = 'red'
    size = (1200, 500)

    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}
    pr_dict2 = {'D': 'day', 'W': 'week', 'M': 'month', 'Q': 'quarter', 'Y': 'year'}

    data = [
        go.Scatter(x=votes_ts[date_col], y=votes_ts['effect'].round(1), line={'color': color, 'width': 0.5},
                   name='{}-value'.format(pr_dict[pr]),
                   hoverinfo='x+y+name'),
        go.Scatter(x=votes_ts_ma[date_col], y=votes_ts_ma['effect'].round(1), line={'color': color, 'width': 4},
                   name='average of last {} {}s'.format(ma, pr_dict2[pr]),
                   hoverinfo='x+y+name') #,
        # go.Scatter(x=trends['date'], y=trends['5%'], line={'color': 'grey', 'width': 1, 'dash': 'dash'}, mode='lines',
        #            name='5% growth', hoverinfo='skip'),
        # go.Scatter(x=trends['date'], y=trends['7%'], line={'color': 'black', 'width': 2, 'dash': 'dash'}, mode='lines',
        #            name='7% growth', hoverinfo='x+y'),
        # go.Scatter(x=trends['date'], y=trends['10%'], line={'color': 'grey', 'width': 1, 'dash': 'dash'}, mode='lines',
        #            name='10% growth', hoverinfo='skip')
    ]

    layout = go.Layout(
        autosize=True, width=size[0], height=size[1],
        title='Net Karma, 4x Downvote, {}, 1.2 item exponent'.format(pr_dict[pr].capitalize()),
        xaxis={'range': [start_date, end_date], 'title': None},
        yaxis={'range': [0, votes_ts.set_index(date_col)[start_date:]['effect'].max() * 1.1],
               'title': 'net karma'}
    )

    fig = go.Figure(data=data, layout=layout)

    set_credentials_file(username=get_config_field('PLOTLY', 'username'),
                                      api_key=get_config_field('PLOTLY', 'api_key'))
    init_notebook_mode(connected=True)

    filename = 'Net Karma Metric - {}'.format(pr_dict[pr].capitalize())
    if online:
        py.iplot(fig, filename=filename)
    else:
        iplot(fig, filename=filename)

    return votes_ts


def agg_votes_to_period(dfvv, start_date, pr='D'):
    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}

    post_cols = ['_id', 'postedAt', 'username', 'title', 'baseScore', 'num_votes', 'percent_downvotes',
                 'num_distinct_viewers']
    comment_cols = ['_id', 'postId', 'postedAt', 'username', 'baseScore', 'num_votes', 'percent_downvotes']

    d = dfvv.set_index('votedAt').sort_index()[start_date:].groupby(['collectionName', 'documentId']).resample(pr).agg(
        {'power_d4': 'sum', 'effect': 'sum', 'legacy': 'size', 'downvote': 'mean'}
    ).round(1).reset_index()
    d = d.rename(
        columns={'legacy': 'num_votes_{}'.format(pr_dict[pr]), 'downvote': 'percent_downvotes_{}'.format(pr_dict[pr])})
    d = d[d['power_d4'] != 0]  # introduced by resampling function
    return d


# add total effects
def add_total_effect_cumulative_and_ranks(dd, pr='D'):
    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}

    dd['effect'] = dd['effect'].round(1)
    dd['abs_effect'] = dd['effect'].abs()
    total_effects = dd.groupby('votedAt')[['effect', 'abs_effect']].sum().round(1)
    total_effects.columns = ['net_effect_for_{}'.format(pr_dict[pr]), 'abs_effect_for_{}'.format(pr_dict[pr])]
    total_effects.head()
    dd = dd.merge(total_effects, left_on='votedAt', right_index=True)
    dd = dd.sort_values(['votedAt', 'effect'], ascending=[True, False]).set_index(['votedAt', 'title'])
    dd['rank'] = dd.groupby(level='votedAt')['effect'].rank(method='first', ascending=False)
    dd['cum_effect'] = dd.groupby(level='votedAt')['effect'].cumsum().round(1)
    dd['effect_over_abs'] = (dd['effect'] / dd['abs_effect_for_{}'.format(pr_dict[pr])]).round(3)
    dd['cum_over_abs'] = (dd['cum_effect'] / dd['abs_effect_for_{}'.format(pr_dict[pr])]).round(3)
    dd['inverse_rank'] = dd.groupby(level='votedAt')['effect'].rank(method='first', ascending=True)
    return dd.reset_index()


def create_url_hyperlink(item):
    if item[['postId', 'title']].notnull().all():

        if 'collectionName' not in item or item['collectionName'] == 'Posts':
            return '=HYPERLINK("www.lesswrong.com/posts/' + item['postId'] + '", "' + item['title'].replace('"',
                                                                                                            '""') + '")'

        else:
            if item['_id_comment']:
                return '=HYPERLINK("www.lesswrong.com/posts/' + item['postId'] + '#' + item['_id_comment'] + '", "' + \
                       item['title'].replace('"', '""') + '")'
            else:
                return ''
    else:
        return ''


def add_url_column(dd):
    dd['title_plain'] = dd['title']
    dd['title'] = dd.apply(create_url_hyperlink, axis=1)
    return dd


def item_agg_select_columns(dd, pr):
    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}
    cols = ['votedAt', 'collectionName', 'title', 'username_post', 'baseScore_post', 'username_comment',
            'baseScore_comment',
            'effect', 'effect_over_abs', 'cum_effect', 'cum_over_abs',
            'net_effect_for_{}'.format(pr_dict[pr]), 'abs_effect_for_{}'.format(pr_dict[pr]), 'rank', 'inverse_rank',
            'num_votes_{}'.format(pr_dict[pr]), 'percent_downvotes_{}'.format(pr_dict[pr]),
            'postedAt_post', 'num_distinct_viewers', 'num_votes_post', 'percent_downvotes_post',
            'postedAt_comment', 'num_votes_comment', 'percent_downvotes_comment', 'title_plain'
            ]

    return dd[cols].set_index(['votedAt', 'collectionName', 'title'])


def post_agg_select_columns(dd, pr):
    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}
    cols = ['votedAt', 'title', 'username', 'baseScore', 'num_comments_voted_on_{}'.format(pr_dict[pr]),
            'num_votes_thread_{}'.format(pr_dict[pr]), 'num_downvotes_{}'.format(pr_dict[pr]),
            'effect', 'effect_over_abs', 'cum_effect', 'cum_over_abs',
            'net_effect_for_{}'.format(pr_dict[pr]), 'abs_effect_for_{}'.format(pr_dict[pr]), 'rank', 'inverse_rank',
            'postedAt', 'num_distinct_viewers', 'num_comments_rederived', 'num_votes', 'percent_downvotes',
            'title_plain'
            ]

    return dd[cols].set_index(['votedAt', 'title'])


def agg_votes_to_items(dfvv, dfp, dfc, start_date, pr='D'):
    post_cols = ['_id', 'postedAt', 'username', 'title', 'baseScore', 'num_votes', 'percent_downvotes',
                 'num_distinct_viewers']
    comment_cols = ['_id', 'postId', 'postedAt', 'username', 'baseScore', 'num_votes', 'percent_downvotes']

    d = agg_votes_to_period(dfvv, start_date, pr)

    # add in post and comment details
    dd = d.merge(dfc[comment_cols], left_on='documentId', right_on='_id', how='left', suffixes=['', '_comment'])
    dd['postId'] = dd['postId'].fillna(dd['documentId'])
    dd = dd.merge(dfp[post_cols], left_on='postId', right_on='_id', how='left', suffixes=['_comment', '_post'])

    # add total effects and ranks
    dd = add_total_effect_cumulative_and_ranks(dd, pr)

    # add url and polish
    dd = add_url_column(dd)
    dd = item_agg_select_columns(dd, pr)

    return dd


def agg_votes_to_posts(dfvv, dfp, dfc, start_date, pr='D'):
    pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}
    d = agg_votes_to_period(dfvv, start_date, pr)

    # add in comments
    post_cols = ['_id', 'postedAt', 'username', 'title', 'baseScore', 'num_votes', 'percent_downvotes',
                 'num_distinct_viewers']
    comment_cols = ['_id', 'postId', 'postedAt', 'username', 'baseScore', 'num_votes', 'percent_downvotes']
    dd = d.merge(dfc[comment_cols], left_on='documentId', right_on='_id', how='left', suffixes=['', '_comment'])

    # aggregate to post level
    dd['postId'] = dd['postId'].fillna(dd['documentId'])
    dd['num_downvotes_{}'.format(pr_dict[pr])] = (
            dd['num_votes_{}'.format(pr_dict[pr])] * dd['percent_downvotes_{}'.format(pr_dict[pr])]).round().astype(
        int)
    dd = dd.groupby(['votedAt', 'postId']).agg({'power_d4': 'sum', 'effect': 'sum', 'username': 'size',
                                                'num_votes_{}'.format(pr_dict[pr]): 'sum',
                                                'num_downvotes_{}'.format(pr_dict[pr]): 'sum'
                                                })
    dd['num_comments_voted_on_{}'.format(pr_dict[pr])] = dd['username'] - 1
    dd = dd.rename(columns={'username': 'num_items',
                            'num_votes_{}'.format(pr_dict[pr]): 'num_votes_thread_{}'.format(pr_dict[pr])})
    dd = dd.reset_index()

    # add in post details
    dd = dd.merge(dfp[post_cols + ['num_comments_rederived']], left_on='postId', right_on='_id', how='left',
                  suffixes=['_comment', '_post'])

    # add total effects and ranks
    dd = add_total_effect_cumulative_and_ranks(dd, pr)

    # add url and clean up columns
    dd = add_url_column(dd)
    dd = post_agg_select_columns(dd, pr)

    return dd

@timed
def run_metric_pipeline(dfs, end_date_str, online=False, sheets=False, plots=False):
    dfp = dfs['posts']
    dfc = dfs['comments']

    allVotes, baseScoresD4, docScores = compute_karma_metric(dfs)

    end_date = pd.to_datetime(end_date_str).strftime('%Y-%m-%d')
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(180, unit='d')).strftime('%Y-%m-%d')
    start_date_sheets = (pd.to_datetime(end_date) - pd.Timedelta(30, unit='d')).strftime('%Y-%m-%d')


    if plots:
        _ = plot_karma_metric(allVotes, online=online, start_date=start_date, end_date=end_date, pr='D', ma=7)
        _ = plot_karma_metric(allVotes, online=online, start_date=start_date, end_date=end_date, pr='W', ma=4)

    if sheets:
        spreadsheet_name = get_config_field('GSHEETS', 'spreadsheet_name')
        spreadsheet_user = get_config_field('GSHEETS', 'user')
        s = Spread(spread=spreadsheet_name, sheet=None, create_spread=True, create_sheet=True, user=spreadsheet_user)

        pr_dict = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'Y': 'yearly'}

        for pr in ['D', 'W']:
            votes2posts = agg_votes_to_posts(allVotes, dfp, dfc, pr=pr, start_date=start_date_sheets)
            data = votes2posts.reset_index().sort_values(['votedAt', 'rank'], ascending=[False, True]).copy()
            data['birth'] = pd.datetime.now()
            data.columns = [col.replace('_', ' ').title() for col in data.columns]
            s.df_to_sheet(data, replace=True, sheet='KM: Posts/{}'.format(pr_dict[pr]), index=False)

            votes2items = agg_votes_to_items(allVotes, dfp, dfc, pr=pr, start_date=start_date_sheets)
            data = votes2items.reset_index().sort_values(['votedAt', 'rank'], ascending=[False, True]).copy()
            data['birth'] = pd.datetime.now()
            data.columns = [col.replace('_', ' ').title() for col in data.columns]
            s.df_to_sheet(data, replace=True, sheet='KM: Items/{}'.format(pr_dict[pr]), index=False)