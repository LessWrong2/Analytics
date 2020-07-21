import pandas as pd
import etlw as et
import google_analytics_ops as ga
import url_grey as url
from cellularautomaton import upload_to_gsheets
from utils import timed


def get_top_pages_last_n(n_days=90):
    dims = ['ga:pagePath']
    metrics = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage',
               'ga:avgPageLoadTime']
    df = ga.get_report(dims, metrics, days=n_days, page_size=10000)  # next_page_token)

    return df


def get_top_pages_last_n_with_urls(n_days=90, pageviews_minimum=50):
    ga_pages = get_top_pages_last_n(90)

    top_pages = ga_pages[ga_pages['ga:pageviews'] >= pageviews_minimum].sort_values('ga:pageviews', ascending=False)
    urls_resolved = url.resolve_urls(top_pages, dfs, url_col='ga:pagePath')
    top_pages_urls = top_pages.merge(urls_resolved, left_on='ga:pagePath', right_on='url', )
    top_pages_urls['birth'] = pd.datetime.now()

    return top_pages_urls


def format_for_upload(unformatted):
    ## Format for Upload
    display_cols = ['documentId', 'author', 'baseScore', 'title', 'total_num_tags', 'tags', 'num_core_tags', 'num_non-core_tags', 'pageviews_rank']  # ga:users', 'ga:sessions', 'ga:uniquePageviews', 'ga:pageviews']

    formatted = unformatted[display_cols].copy()
    formatted['title'] =  formatted['title'] = '=HYPERLINK("www.lesswrong.com/posts/' + formatted['documentId'] + \
                                               '", "' + formatted['title'].str.replace('"' ,'""') + '")'
    formatted.columns = [col.replace('_', ' ') for col in formatted.columns]
    formatted = formatted.drop(['documentId'], axis=1)
    formatted['tags'] = formatted['tags'].astype(str).str.replace('[', '').str.replace(']', '').str.replace("'", '')
    formatted.columns = ['Author', 'Karma', 'Title', 'Num Tags', 'Tags', '#Core Tags', '#Non-Core Tags', 'Pageviews Rank'] # '#Users', '#Sessions', '#uniquePageviews', '#Pageviews']
    formatted['Last Updated'] = pd.datetime.now()

    return formatted

@timed
def run_top_posts_tags_job():

    ## Get Data
    today_str =(pd.datetime.today() +pd.Timedelta('7 hours')).strftime('%Y%m%d')

    collections = et.load_from_file(date_str='most_recent', coll_names=['users', 'posts', 'sequences', 'tags', 'tagrels'])
    tag_collections = et.get_collections_cleaned(coll_names=['tags', 'tagrels'])

    try:
        top_pages_urls = et.load_from_file('most_recent', ['top_viewed_posts_last_90'])['top_viewed_posts_last_90']
    except: #file does not exist
        top_pages_urls = None

    if top_pages_urls is None or pd.datetime.now() - top_pages_urls['birth'].max() > pd.Timedelta(24, unit='h'):
        print('refreshing pages')
        top_pages_urls = get_top_pages_last_n_with_urls(n_days=90)
        et.write_collection('top_viewed_posts_last_90', top_pages_urls, today_str)


    ## Process Data
    tagrels = tag_collections['tagrels'].merge(tag_collections['tags'], left_on='tagId', right_on='_id', suffixes=['_tr', '_t'])
    tagrels = tagrels[(~tagrels[['deleted_tr', 'deleted_t', 'inactive', 'adminOnly']].any(axis=1) ) & (tagrels['score' ]>0)]
    tagrels = tagrels[['tagId', 'postId', 'score', 'name', 'core']]

    # dataframe with counts of core tags, noncore tags, total tags for each post
    post_tag_counts = (tagrels
        .groupby('postId')['name']
        .agg(['size', lambda x: x.tolist()])
        .rename ({'size': 'total_num_tags', '<lambda_0>' :'tags'}, axis=1)
        .merge(tagrels
           .groupby(['postId', 'core'])
           .size()
           .unstack(1)
           .fillna(0)
           .rename({False: 'num_non-core_tags', True: 'num_core_tags'}, axis=1),
        left_index=True, right_index=True)
    )

    ## Aggregate different urls for same post + join in tag counts
    df = (top_pages_urls[top_pages_urls['type' ]=='post']
          .groupby(['documentId', 'title', 'author'])[['ga:users', 'ga:sessions', 'ga:uniquePageviews', 'ga:pageviews']]
          .sum()
          .reset_index()
          .merge(collections['posts'][['_id', 'baseScore']], left_on='documentId', right_on='_id')
          .merge(post_tag_counts, left_on='documentId', right_on='postId', how='left')
          )

    df.loc[:, ['total_num_tags', 'num_non-core_tags', 'num_core_tags']] = df.loc[:, ['total_num_tags', 'num_non-core_tags', 'num_core_tags']].fillna(0)
    df['tags'] = df['tags'].fillna('')
    df = df.sort_values('ga:pageviews', ascending=False)
    df['pageviews_rank'] = df['ga:pageviews'].rank(ascending=False, method='first')


    ## Upload
    formatted_df = format_for_upload(df)

    upload_to_gsheets(formatted_df, spreadsheet_name='LessWrong: Posts & Tags', sheet_name='Sorted by View Rank')
    upload_to_gsheets(formatted_df.sort_values('Karma', ascending=False), spreadsheet_name='LessWrong: Posts & Tags',
                      sheet_name='Sorted by Karma')


if __name__ == '__main__':
    run_top_posts_tags_job()