import pandas as pd
from utils import get_config_field, timed, print_and_log
from cellularautomaton import upload_to_gsheets

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from flipthetable import get_pg_engine, create_tables, truncate_or_drop_tables, bulk_upload_to_pg


def initialize_analytics_reporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(get_config_field('GA','key_file_location'),
                                                                   [get_config_field('GA', 'scopes')])
    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics


def get_report_internal(analytics, view_id, dimensions, metrics,
                        start_date, end_date,
                        page_token=None, page_size=None,
                        ):
    body = {'reportRequests': [
        {
            'viewId': view_id,
            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
            'metrics': [{'expression': m} for m in metrics],
            'dimensions': [{'name': d} for d in dimensions],
            'pageSize': page_size,
            'pageToken': page_token
        }
    ]
    }
    #     print(body) #woo, side-effect

    return analytics.reports().batchGet(body=body).execute()


def convert_to_dataframe(response):
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = [i.get('name', {}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
        next_page_token = report.get('nextPageToken')
        finalRows = []

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            metrics = row.get('metrics', [])[0].get('values', {})
            rowObject = {}

            for header, dimension in zip(dimensionHeaders, dimensions):
                rowObject[header] = dimension

                for metricHeader, metric in zip(metricHeaders, metrics):
                    rowObject[metricHeader] = metric

            finalRows.append(rowObject)

    df = pd.DataFrame(finalRows)
    for metric in metricHeaders:
        df[metric] = pd.to_numeric(df[metric])
    return (df, next_page_token)


def get_report_recursive(analytics, view_id, dimensions, metrics,
                         start_date, end_date, page_token=None, page_size=None, df_acc=None):
    df, next_page_token = convert_to_dataframe(get_report_internal(analytics, view_id, dimensions, metrics,
                                                                   start_date, end_date, page_token, page_size))
    #     print(next_page_token)
    if next_page_token:
        return get_report_recursive(analytics, view_id, dimensions, metrics,
                                    start_date, end_date, page_token=next_page_token, page_size=page_size,
                                    df_acc=pd.concat([df_acc, df]))
    else:
        return pd.concat([df_acc, df])

@timed
def get_report(dims, metrics, start_date=None, end_date=None, days=None):

    view_id = get_config_field('GA', 'view_id')
    analytics = initialize_analytics_reporting()

    if start_date and end_date and days:
        raise Exception('Argument error: Cannot specify all of start_date, end_date, and days')
    elif not (start_date or end_date or days):
        raise Exception('Argument error: Must specify two of {start_date, end_date', 'day')
    elif end_date and not start_date:
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days-1, unit='d')).strftime('%Y-%m-%d')
    elif not end_date and start_date:
        end_date = (pd.to_datetime(start_date) + pd.Timedelta(days-1, unit='d')).strftime('%Y-%m-%d')
    elif not (end_date or start_date):
        end_date = pd.datetime.today().strftime('%Y-%m-%d')
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days-1, unit='d')).strftime('%Y-%m-%d')

    df = get_report_recursive(analytics, view_id, dims, metrics, start_date, end_date, page_size=None)
    if 'ga:date' in df.columns:
        df['date'] = pd.to_datetime(df['ga:date'])
        df = df.drop(['ga:date'], axis=1)

    return df

def agg(df, source_col, agg_col, pattern, replacement, case=False):
    if agg_col not in df.columns:
        df[agg_col] = df[source_col]
    df.loc[df[source_col].str.contains(pattern, case=case), agg_col] = replacement


def get_traffic_metrics(start_date=None, end_date=None, days=365):

    dims = ['ga:date']
    metrics = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:pageviewsPerSession']
    df = get_report(dims, metrics, start_date, end_date, days)  # next_page_token)

    return df[['date', 'ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:pageviewsPerSession']]


def get_source_metrics(start_date=None, end_date=None, days=30):

    dims = ['ga:date', 'ga:source']
    metrics = ['ga:users', 'ga:sessions']
    df = get_report(dims, metrics, start_date, end_date, days)  # next_page_token)

    agg_source = lambda pattern, replacement: agg(df, 'ga:source', 'source_agg', pattern, replacement)
    agg_source('facebook', 'facebook')
    agg_source('wikipedia', 'wikipedia')
    agg_source('twitter|t\\.co', 'twitter')

    return df[['date', 'source_agg', 'ga:source', 'ga:users', 'ga:sessions']]


def get_referrer_metrics(start_date=None, end_date=None, days=30):

    dims = ['ga:date', 'ga:fullReferrer']
    metrics = ['ga:users', 'ga:sessions']
    df = get_report(dims, metrics, start_date, end_date, days)  # next_page_token)

    agg_referrer = lambda pattern, replacement: agg(df, 'ga:fullReferrer', 'referrer_agg', pattern, replacement)
    agg_referrer('facebook', 'facebook')
    agg_referrer('wikipedia', 'wikipedia')
    agg_referrer('twitter|t\\.co', 'twitter')
    agg_referrer('yudkowsky\\.net', 'yudkowsky.net/*')

    return df[['date', 'referrer_agg', 'ga:fullReferrer', 'ga:users', 'ga:sessions']]


def get_device_metrics(start_date=None, end_date=None, days=180):

    dims = ['ga:date', 'ga:deviceCategory']
    metrics = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews']
    df = get_report(dims, metrics, start_date, end_date, days)  # next_page_token)

    return df[['date', 'ga:deviceCategory','ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews']]

def get_page_metrics(start_date=None, end_date=None, days=7):

        dims = ['ga:date', 'ga:pagePath']
        metrics = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage', 'ga:avgPageLoadTime']
        df = get_report(dims, metrics, start_date, end_date, days)  # next_page_token)

        agg_page = lambda pattern, replacement: agg(df, 'ga:pagePath', 'page_agg', pattern, replacement)
        agg_page(r'/s/.+', '/sequence/*') # is relying on next line, TO-DO, fix regex
        agg_page(r'/posts/|/s/.+/p/.+|\/lw/', '/posts/*')
        agg_page('/rationality/', '/rationality/*')
        agg_page('/codex/', '/codex/*')
        agg_page('/hpmor/', '/hpmor/*')
        agg_page('/about', '/about')
        agg_page(r'/users/', '/users/*')
        agg_page(r'/search', '/search')
        agg_page(r'/verify-email/', '/verify-email/*')
        agg_page(r'/editPost', '/editPost')
        agg_page(r'/allPosts', '/allPosts')
        agg_page(r'/inbox/', '/inbox/')
        agg_page(r'/events/', '/events/*')
        agg_page(r'/community', '/community')
        agg_page(r'/groups/', '/groups/*')
        agg_page(r'/tag/', '/tag/*')
        agg_page(r'/coronavirus-link-database', '/coronavirus-link-database')

        return df[['date', 'page_agg', 'ga:pagePath',
                   'ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews',
                   'ga:avgTimeOnPage', 'ga:avgPageLoadTime']]


def get_all_metrics():
    ga_metrics = {}
    ga_metrics['traffic'] = get_traffic_metrics(start_date='2017-01-01',
                                                end_date=pd.datetime.today().strftime('%Y-%m-%d'), days=None)
    ga_metrics['source'] = get_source_metrics(days=180)
    ga_metrics['devices'] = get_device_metrics(days=180)
    ga_metrics['referrer'] = get_referrer_metrics(days=30)
    ga_metrics['pages'] = get_page_metrics(days=14)

    return ga_metrics


def run_ga_pipeline():

    ##Get Data
    ga_metrics = get_all_metrics()

    ## GSheets Upload
    def ga_gsheets_upload(df, name):
        upload_to_gsheets(df, 'LW Automatically Updating Spreadsheets', 'GA: {}'.format(name))

    ga_metrics_gsheets = ga_metrics.copy()
    ga_metrics_gsheets['pages']['ga:pagePath'] = '=HYPERLINK("www.lesswrong.com' + \
                                                 ga_metrics_gsheets['pages']['ga:pagePath'] + '", "' + \
                                                ga_metrics_gsheets['pages']['ga:pagePath'] + '")'

    [ga_gsheets_upload(df, name) for name, df in ga_metrics.items()]


    ## Postgres Upload
    ga_metrics_pg = ga_metrics.copy()

    for df in ga_metrics_pg.values():
        df.columns = [col.replace(':', '_') for col in df.columns]

    try:
        engine = get_pg_engine()

        with engine.begin() as conn:

            print_and_log('truncating postgres tables')
            truncate_or_drop_tables(['ga_' + name for name in ga_metrics.keys()], conn=conn, drop=False)

            print_and_log('loading tables into postgres db')
            [bulk_upload_to_pg(df, table_name='ga_' + name, conn=conn) for name, df in ga_metrics.items()]

            print_and_log('transaction successful!')

    except:
        print_and_log('transfer failed')
    finally:
        engine.dispose()


