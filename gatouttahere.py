import pandas as pd
from utils import get_config_field

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


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


def get_standard_ga_metrics(start_date, end_date):

    view_id = get_config_field('GA', 'view_id')
    analytics = initialize_analytics_reporting()

    DIMENSIONS = ['ga:date', 'ga:dayOfWeekName']
    METRICS = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:pageviewsPerSession']

    df = get_report_recursive(analytics, view_id, DIMENSIONS, METRICS, start_date, end_date, page_size=None)  # next_page_token)
    df['date'] = pd.to_datetime(df['ga:date'])

    return df

def get_report(dims, metrics, start_date, end_date):

    view_id = get_config_field('GA', 'view_id')
    analytics = initialize_analytics_reporting()

    df = get_report_recursive(analytics, view_id, dims, metrics, start_date, end_date, page_size=None)
    if 'ga:date' in df.columns:
        df['date'] = pd.to_datetime(df['ga:date'])

    return df
