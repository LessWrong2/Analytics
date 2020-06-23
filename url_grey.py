import pandas as pd
import numpy as np
from collections import namedtuple
from hashlib import md5
import re
from flipthetable import get_pg_engine, bulk_upload_to_pg, truncate_or_drop_tables
from utils import parallelize_dataframe, get_collection, get_mongo_db_object, timed
import etlw as et

urlRecord = namedtuple('urlRecord', ['url', 'documentType', 'title', 'documentId', 'author'])
urlRecord.__new__.__defaults__ = (None,) * 5
urlRecord()

def get_urls(start_date='2020-01-01'):
    query = """SELECT DISTINCT url FROM
                (SELECT DISTINCT path AS url FROM lessraw_small WHERE timestamp > '{0}'
                UNION
                SELECT DISTINCT url_to AS url FROM lessraw_small WHERE timestamp > '{0}') sub""".format(start_date)

    engine = get_pg_engine()
    with engine.begin() as conn:
        urls = pd.read_sql(query, conn)
    engine.dispose()

    return urls

def resolve_url_uncurried(url, dfs):
    dfp = dfs['posts']
    sequences = dfs['sequences']

    # try:
    homepage_pattern = r'(^/$)'
    if re.search(homepage_pattern, url):
        return urlRecord(
            url=url,
            documentType='homepage'
        )

    # standard post url: "/posts/<id>" or "/s/<sequence_id>/p/<posts_id>"
    post_patterns = r'(?<=/s/\w{17}/p/)(\w{17})|(?<=/posts/)\w{17}'
    matches = re.search(post_patterns, url)
    if matches:
        postId = matches.group(0)
        matching_posts = dfp[dfp['_id'] == postId]
        if not matching_posts.empty:
            post = dfp[dfp['_id'] == postId].iloc[0]
            return urlRecord(
                url=url,
                documentType='post',
                title=post['title'],
                documentId=postId,
                author=post['displayName']
            )

    # "/rationalty/<slug>" or "/lw/<short_id>/slug" (resolve on slug)
    post_custom_pattern = r'((?<=/rationality/)|(?<=/codex/))([\w-]+)|((?<=/lw/\w{2}/)|(?<=/lw/\w{3}/))\w+'
    matches = re.search(post_custom_pattern, url)
    if matches:
        post_slug = matches.group(0).replace('_', '-')
        post = dfp[dfp['slug'] == post_slug]
        if not post.empty:
            post = post.iloc[0]
            return urlRecord(
                url=url,
                documentType='post',
                title=post['title'],
                documentId=post['_id'],
                author=post['displayName']
            )
        else:
            return urlRecord(
                url=url,
                documentType='post',
            )

    # "/s/<sequenceId>"
    sequence_patterns = r'(?<=/s/)\w{17}$'
    matches = re.search(sequence_patterns, url)
    if matches:
        sequenceId = matches.group(0)
        sequence = sequences[sequences['_id'] == sequenceId]
        if not sequence.empty:
            sequence = sequence.iloc[0]
            return urlRecord(
                url=url,
                documentType='sequence',
                title=sequence['title'],
                documentId=sequenceId
            )
        else:
            return urlRecord(
                url=url,
                documentType='sequence',
                documentId=sequenceId
            )

    # Old Wiki
    old_wiki_patterns = r'wiki\.lesswrong'
    matches = re.search(old_wiki_patterns, url)
    if matches:
        return urlRecord(
            url=url,
            documentType='old_lw_wiki',
        )

    return urlRecord(url)
# except:
#     print(url)


def resolve_urls(df, dfs):

    unique_urls = df.dropna().drop_duplicates()
    urls_resolved = unique_urls['url'].astype(str).apply(lambda x: pd.Series(data=resolve_url_uncurried(x, dfs)))

    urls_resolved.columns = ['url', 'type', 'title', 'documentId', 'author']
    urls_resolved = urls_resolved.fillna(np.nan)
    urls_resolved['onsite'] = urls_resolved['url'].str.match(r'(^\/)') & ~urls_resolved['url'].str.match('http')

    return urls_resolved


@timed
def get_resolved_urls(sample=None, start_date=None):

    dfs = et.load_from_file(date_str='most_recent', coll_names=['posts', 'sequences'])
    urls = get_urls(start_date)

    if sample:
        urls = urls.sample(sample)

    # def resolve_urls_curried(x):
    #     return resolve_urls(x, dfs)
    # resolve_urls_curried = lambda x: resolve_urls(x, dfs)

    # urls_resolved = parallelize_dataframe(urls, resolve_urls_curried, 2)
    urls_resolved = resolve_urls(urls, dfs)
    urls_resolved['url_hash'] = urls_resolved['url'].apply(lambda x: md5(x.encode()).hexdigest())

    return urls_resolved


def run_url_table_update(override_start=None):

    engine = get_pg_engine()
    with engine.begin() as conn:
        # Download current PG url table
        query = """SELECT * FROM urls"""
        urls_existing = pd.read_sql(query, conn)

        # Get birth of url table
        if override_start:
            start_date = override_start
        else:
            start_date = urls_existing['birth'].max()

        # Download & Resolve new URLs since ~birth
        urls_resolved_new = get_resolved_urls(start_date=(pd.to_datetime(start_date) - pd.Timedelta('1 days'))
                                              .strftime('%Y-%m-%d'))
        urls_resolved_new['birth'] = pd.datetime.now()

        # Append new urls to existing table, drop duplicates
        urls_updated = pd.concat([urls_existing, urls_resolved_new]).drop_duplicates(subset=['url_hash'])

        # Replace existing PG table
        truncate_or_drop_tables('urls', conn)
        bulk_upload_to_pg(urls_updated, table_name='urls', conn=conn)

    engine.dispose()
















## tests

# resolve_url('http://wiki.lesswrong.com/wiki/Mysterious_Answers_to_Mysterious_Questions')
# resolve_url('/lw/y8/interlude_with_the_confessor_48/')
# resolve_url('/s/5g5TkQTe9rmPS5vvM/p/CPm5LTwHrvBJCa9h5#cite.0.Buehler.2002')
# resolve_url('/lw/3w3/how_to_beat_procrastination/')
# resolve_url('/rationality/preface')
# resolve_url('/codex/eight-short-studies-on-excuses')

# resolve_url('/')
# resolve_url('https://www.lesswrong.com/s/fqh9TLuoquxpducDb/p/Masoq4NdmmGSiq2xw')
# resolve_url('https://www.lesswrong.com/posts/895quRDaK6gR2rM82/diseased-thinking-dissolving-questions-about-d...')
# resolve_url('https://www.lesswrong.com/s/9bvAELWc8y2gYjRav')
# resolve_url('/s/5g5TkQTe9rmPS5vvM')
# resolve_url('www.leesdsafasdfdsaf')




