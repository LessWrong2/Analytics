import pandas as pd
from gspread_pandas import Spread
from utils import timed, get_config_field


def upload_to_gsheets(df, spreadsheet_name, sheet_name, create_spread=False, create_sheet=False, grant_access=None,
                      index=False, format_columns=False):

    df = df.copy()

    if format_columns:
        df.columns = df.columns.to_series().str.replace('_', ' ').str.title()

    spreadsheet = Spread(spread=spreadsheet_name, sheet=sheet_name, create_spread=create_spread,
                         create_sheet=create_sheet, user=get_config_field('GSHEETS', 'user'))
    spreadsheet.df_to_sheet(df, index=index)

    if grant_access == 'primary':
        permissions_list = ['{email}|writer'.format(email=get_config_field('GSHEETS', 'primary_email'))]
    elif grant_access == 'team':
        emails = get_config_field('GSHEETS', 'team_emails').split(',')
        permissions_list = ['{email}|writer'.format(email=email) for email in emails]
    elif grant_access == 'public':
        permissions_list = ['anyone|reader']
    else:
        permissions_list = None

    if permissions_list:
        spreadsheet.add_permissions(permissions_list)

    print(spreadsheet.url)
    return spreadsheet.url


def create_and_update_user_sheet(dfu, spreadsheet, limit=None):
    # data = dfu[~dfu['banned']].sort_values('karma', ascending=False) //old
    data = dfu[(dfu['karma'] > 0) | (dfu['num_distinct_posts_viewed'] > 0) | (dfu['signUpReCaptchaRating'] >= 0.7)]
    data.loc[data['days_since_active'] <= 0, 'days_since_active'] = 0
    data['username'] = '=HYPERLINK("www.lesswrong.com/users/'.lower() + data['username'] + '", "' + data[
        'username'] + '")'

    data['birth'] = pd.datetime.now()
    #     data['most_recent_activity'] = data['most_recent_activity'].dt.date
    data = data.sort_values('most_recent_activity', ascending=False).head(49990)

    user_cols = [
        'birth',
        '_id',
        'username',
        'karma',
        'days_since_active',
        'most_recent_activity',
        'num_days_present_last_30_days',
        'num_distinct_posts_viewed_last_30_days',
        'num_votes_last_30_days',
        'num_comments_last_30_days',
        'num_posts_last_30_days',
        'num_views_last_180_days',
        'num_votes_last_180_days',
        'num_comments_last_180_days',
        'num_posts_last_180_days',
    ]

    # recent_count_cols = [ # test removal since trying fix upstream
    #     'num_distinct_posts_viewed_last_30_days',
    #     'num_votes_last_30_days',
    #     'num_comments_last_30_days',
    #     'num_posts_last_30_days',
    #     'num_views_last_180_days',
    #     'num_votes_last_180_days',
    #     'num_comments_last_180_days',
    #     'num_posts_last_180_days']

    if limit:
        data = data[user_cols].head(limit)
    else:
        data = data[user_cols]

    # data.loc[:, recent_count_cols] = data[recent_count_cols].fillna(0)
    data.columns = [col.replace('_', ' ').title() for col in data.columns]
    spreadsheet.df_to_sheet(data, replace=True, sheet='Users', index=False)
    return data


def create_and_update_posts_sheet(dfp, spreadsheet, num_rows=None):
    data = dfp[(~dfp['draft']) & (dfp[['smallUpvote', 'bigUpvote']].sum(axis=1) >= 2)].sort_values('postedAt',
                                                                                                   ascending=False)
    data['title'] = '=HYPERLINK("www.lesswrong.com/posts/' + data['_id'] + '", "' + data['title'].str.replace('"','""') + '")'
    data['birth'] = pd.datetime.now()
    data['postedAt'] = data['postedAt'].dt.date

    post_cols = [
        'birth',
        '_id',
        'postedAt',
        'username',
        'title',
        'baseScore',
        'frontpaged',
        'question',
        'num_comments_rederived',
        'num_distinct_viewers',
        'num_votes',
        'percent_downvotes',
        'viewCountLogged',
        'af',
        'curatedDate',
        'wordCount'
    ]

    int_cols = ['num_comments_rederived', 'viewCountLogged', 'num_votes', 'num_distinct_viewers']

    if num_rows:
        data = data[post_cols].head(num_rows)
    else:
        data = data[post_cols]

    data.loc[:, int_cols] = data[int_cols].fillna(0)
    data.columns = [col.replace('_', ' ').title() for col in data.columns]
    spreadsheet.df_to_sheet(data, replace=True, sheet='Posts (2+ upvotes)', index=False)
    return data

def create_and_update_votes_sheet(dfv, spreadsheet, num_days=180):
    data = dfv[dfv['votedAt'] >= dfv['votedAt'].max() - pd.Timedelta(num_days, unit='d')].sort_values('votedAt', ascending=False)
    data['birth'] = pd.datetime.now()
    spreadsheet.df_to_sheet(data, replace=True, sheet='Votes (last 180 days)', index=False)
    return data

@timed
def create_and_update_all_sheets(dfs, spreadsheet_name):
    dfu = dfs['users']
    dfp = dfs['posts']
    # dfv = dfs['votes']

    s = Spread(spread=spreadsheet_name, sheet=None, create_spread=True, create_sheet=True, user=get_config_field('GSHEETS', 'user'))
    _ = create_and_update_user_sheet(dfu, s)
    _ = create_and_update_posts_sheet(dfp, s)
    # _ = create_and_update_votes_sheet(dfv, s) // we never use this

    return s


