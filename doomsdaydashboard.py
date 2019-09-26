import pandas as pd
import etlw as et
from utils import get_config_field
import plotly
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

plotly.tools.set_credentials_file(username=get_config_field('PLOTLY', 'username'),
                                      api_key=get_config_field('PLOTLY', 'api_key'))
init_notebook_mode(connected=True)

from losttheplotly import plot_table


def update_petrov():
    dfs = et.load_from_file(date_str='most_recent', coll_names=['users'])
    dfu = dfs['users']

    have_codes = pd.read_csv('/Users/rbloom/Downloads/petrov_day_list.csv')
    have_codes.head()

    projection = ['_id', 'username', 'displayName', 'karma', 'petrovPressedButtonDate', 'petrovCodesEntered', 'petrovCodesEnteredDate']
    users_raw = et.get_collection('users', query_filter={'petrovPressedButtonDate': {'$exists':True}},
                              projection=projection, db=et.get_mongo_db_object())

    users = users_raw.merge(have_codes[['Username']], left_on='username', right_on='Username', indicator=True, how='left')
    users = users.merge(dfu[['_id', 'num_days_present_last_30_days', 'createdAt', 'true_earliest']].fillna(0), left_on='_id', right_on='_id', how='left')
    users['has_codes'] = (users['_merge']=='both')

    users.loc[:,['petrovPressedButtonDate', 'petrovCodesEnteredDate']] = users.loc[:,['petrovPressedButtonDate', 'petrovCodesEnteredDate']] - pd.Timedelta('7 hours')
    users['karma'] = users['karma'].fillna(0)
    users = users.sort_values('petrovPressedButtonDate', ascending=False)

    users = users[['displayName', 'karma', 'petrovPressedButtonDate', 'petrovCodesEntered', 'petrovCodesEnteredDate', 'has_codes', 'num_days_present_last_30_days', 'true_earliest']]

    users_pressed = users.dropna(subset=['petrovPressedButtonDate'])
    print('num users pressed button: {}'.format(users_pressed.shape[0]))
    print(users_pressed.head(25))

    users_pressed_and_entered = users.dropna(subset=['petrovPressedButtonDate','petrovCodesEntered'])
    print('num users pressed button and entered codes: {}'.format(users_pressed_and_entered.shape[0]))
    print(users_pressed_and_entered.head(5))

    users_pressed_and_entered_has_codes = users_pressed_and_entered[users_pressed_and_entered['has_codes']]
    print('num users pressed button and entered codes: {}'.format(users_pressed_and_entered_has_codes.shape[0]))
    print(users_pressed_and_entered_has_codes)

    plot_table(users_pressed, title='Users Who Pressed Button', online=True)
    plot_table(users_pressed_and_entered, title='Users Who Pressed Button and Entered Codes', online=True)
    plot_table(users_pressed_and_entered_has_codes, title='Users Who Pressed Button and Entered Some Codes Who Have True Codes', online=True)

if __name__ == '__main__':
    update_petrov()