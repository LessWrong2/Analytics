import pandas as pd
import numpy as np
import etlw as et
import google_analytics_ops as ga
import url_grey as url
from cellularautomaton import upload_to_gsheets
from utils import timed


def format_tags_posts_list(posts):
    return ' '.join(['({number}) {title};  '.format(number=post.Index + 1, title=post.title)
                     for post in posts.reset_index().itertuples()])


def create_tag_posts_list(tagrels, posts):
    return (tagrels[tagrels['score'] > 0]
            .merge(posts[['_id', 'title']], left_on='postId', right_on='_id')
            .groupby('tagId')
            .apply(format_tags_posts_list)
            .to_frame('posts')
            )


def time_delta_format(seconds):
    seconds = np.round(seconds)
    if seconds < 0:
        return 'ERROR: negative value'
    if seconds <= 44:
        return 'a few seconds ago'
    elif seconds <= 89:
        return '1m'
    elif seconds <= 44 * 60:
        return '{:0.0f}m'.format(seconds / 60)
    elif seconds < 21.5 * 3600:
        return '{:0.0f}h'.format(seconds / 3600)
    elif seconds < 25.5 * 86400:
        return '{:0.0f}d'.format(seconds / 86400)
    elif seconds < 320 * 86400:
        return '{:0.0f}mo'.format(seconds / (86400 * 30.5))
    elif seconds < np.inf:
        return '{:0.0f}y'.format(seconds / (86400 * 365.25))
    elif np.isnan(seconds):
        return '-'
    else:
        return 'ERROR: not a number'


def generate_tags_sheet(collections, tag_collections):
    """Generates dataframe for uploads as tags sheet of public tag dashboard"""

    gradeDescriptions = {
        -1: 'Missing',
        0: 'Uncategorized',
        1: 'Flagged',
        2: 'Stub',
        3: 'C-Class',
        4: 'B-Class',
        5: 'A-Class'
    }

    tags = tag_collections['tags']
    tags = tags[~tags[['adminOnly', 'deleted']].any(axis=1)]  # no adminOnly or delete tags
    tagrels = tag_collections['tagrels']

    tag_posts_list = create_tag_posts_list(tagrels, collections['posts'])
    last_post_added = tagrels[tagrels['score'] > 0].groupby('tagId')['createdAt'].max().to_frame('last_post_added')

    tags['description_text'] = (et.htmlBody2plaintext(tags['description']
                                                      .str['html']
                                                      .fillna(''), ignore_links=True)
                                .str.lstrip()
                                .str.replace('\n', ' ')
                                )
    tags['last_edited'] = tags['description'].str['editedAt']
    tags['postCount'] = tags['postCount'].fillna(0)
    tags.loc[:, 'grade'] = tags['wikiGrade'].fillna(-1).apply(lambda x: gradeDescriptions[x])

    # Replace userId of creator with displayname, add list of posts, add when last post was added
    tags = (tags
            .merge(collections['users'][['_id', 'displayName']], left_on='userId', right_on='_id',
                   suffixes=['', '_user'], how='left')
            .merge(tag_posts_list, left_on='_id', right_index=True, how='left', suffixes=['', '_list'])
            .merge(last_post_added, left_on='_id', right_index=True)
            .assign(last_changed=lambda x: x[['last_edited', 'last_post_added']].max(axis=1))
            .assign(createdAt=lambda x: x['createdAt'])
            .assign(last_edited=lambda x: x['last_edited'])
            .assign(last_post_added=lambda x: x['last_post_added'])
            .assign(data_updated=pd.datetime.now())

            )

    tags['last_changed_date'] = tags['last_changed']
    tags.loc[:, ['createdAt', 'last_edited', 'last_post_added', 'last_changed']] = (pd.datetime.now() - tags.loc[:,
                                                                                                        ['createdAt',
                                                                                                         'last_edited',
                                                                                                         'last_post_added',
                                                                                                         'last_changed']]).applymap(
        lambda x: time_delta_format(x.total_seconds()))

    # add hyperlink for gsheets
    tags['name'] = '=HYPERLINK("www.lesswrong.com/tag/'.lower() + tags['slug'] + '?lw_source=tags_sheet'\
                   + '", "' + tags['name'] + '")'

    tags_formatted = tags[['displayName', 'name', 'last_changed', 'grade', 'postCount', 'description_text',
                           'posts', 'last_edited', 'last_post_added', 'createdAt', 'last_changed_date', 'data_updated']]

    tags_formatted.columns = ['Created By', 'Tag Name', 'Tag Last Changed', 'Grade', 'Post Count',
                              'Description', 'Posts', 'Edited', 'Last Added', 'Created', 'Last Changed Date',
                              'Data Updated']

    return tags_formatted


def get_top_pages_last_n(n_days=90):
    dims = ['ga:pagePath']
    metrics = ['ga:users', 'ga:sessions', 'ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage',
               'ga:avgPageLoadTime']
    df = ga.get_report(dims, metrics, days=n_days, page_size=10000)  # next_page_token)

    return df


def get_top_pages_last_n_with_urls(collections, n_days=90, pageviews_minimum=50):
    ga_pages = get_top_pages_last_n(n_days)

    top_pages = ga_pages[ga_pages['ga:pageviews'] >= pageviews_minimum].sort_values('ga:pageviews', ascending=False)
    urls_resolved = url.resolve_urls(top_pages, collections, url_col='ga:pagePath')
    top_pages_urls = top_pages.merge(urls_resolved, left_on='ga:pagePath', right_on='url', )
    top_pages_urls['birth'] = pd.datetime.now()

    return top_pages_urls


def format_for_upload(unformatted):
    ## Format for Upload
    display_cols = ['documentId', 'author', 'baseScore', 'title', 'total_num_tags', 'tags', 'num_core_tags', 'num_non-core_tags', 'pageviews_rank']  # ga:users', 'ga:sessions', 'ga:uniquePageviews', 'ga:pageviews']

    formatted = unformatted[display_cols].copy()
    formatted['title'] =  formatted['title'] = '=HYPERLINK("www.lesswrong.com/posts/' + formatted['documentId'] \
                                               + '?lw_source=posts_sheet' + '", "' + formatted['title'].str.replace('"' ,'""') + '")'
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
        top_pages_urls = get_top_pages_last_n_with_urls(n_days=90, collections=collections)
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


    ## Get the Tag Sheets
    tags_sheet = generate_tags_sheet(collections, tag_collections)


    ## Upload
    formatted_df = format_for_upload(df)

    upload_to_gsheets(formatted_df, spreadsheet_name='LessWrong: Posts & Tags', sheet_name='Posts (sorted by View Rank)')
    upload_to_gsheets(formatted_df.sort_values('Karma', ascending=False), spreadsheet_name='LessWrong: Posts & Tags',
                      sheet_name='Posts (sorted by Karma)')
    upload_to_gsheets(tags_sheet.sort_values('Last Changed Date', ascending=False),
                      spreadsheet_name='LessWrong: Posts & Tags', sheet_name='Tags (sorted by Last Changed)')
    upload_to_gsheets(tags_sheet.sort_values(['Grade', 'Last Changed Date'], ascending=[False, False]),
                      spreadsheet_name='LessWrong: Posts & Tags', sheet_name='Tags (sorted by Grade, Last Changed)')

if __name__ == '__main__':
    run_top_posts_tags_job()