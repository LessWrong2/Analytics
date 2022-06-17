import pandas as pd
from utils import get_valid_views


def calculate_core_content_metric_trend(core_content_views, start_date=None, end_date=None, posts_read_exponent=1, karma_exponent=1):
    if not start_date:
        start_date = core_content_views.index.min().date()
    if not end_date:
        end_date = core_content_views.index.max().date()

    dates = pd.date_range(start_date, end_date)

    core_content_metric = {}
    users_views_accumulator = {}
    users_views_score = {}

    for i, _ in enumerate(dates[:-1]):
        start_date = dates[i]
        end_date = dates[i + 1]

        for view in core_content_views.sort_index()[start_date:end_date].itertuples(index=False, name='View'):
            userId = view.userId
            current_weighted_reads = users_views_accumulator.get(userId, 0) + view.weighting

            users_views_accumulator[userId] = current_weighted_reads
            users_views_score[userId] = current_weighted_reads ** posts_read_exponent

        core_content_metric[start_date] = pd.Series(users_views_score).sum()

    return pd.Series(core_content_metric)


def compute_core_content_metric(collections, start_date=None, end_date=None, posts_read_exponent=1.5, karma_exponent=1,
                                included_collections=('rationality', 'codex', 'hpmor')):

    weightings = (pd.DataFrame
        .from_dict({
            'rationality': 1,
            'codex': 1,
            'hpmor': 1,
        }, orient='index')
        .rename(columns={0: 'weighting'})
    )

    ## Get core content posts with weightings
    posts = collections['posts']
    posts_with_weightings = (
        posts[['_id', 'title', 'canonicalCollectionSlug']]
        .loc[posts['canonicalCollectionSlug'].isin(included_collections)]
        .merge(weightings, left_on='canonicalCollectionSlug', right_index=True)
    )


    core_content_views = (get_valid_views(collections)
             .assign(date=lambda x: x['createdAt'].dt.date)
             .sort_values('createdAt')
             .drop_duplicates()
             .drop_duplicates(subset=['date', 'documentId', 'userId'])
             .merge(posts_with_weightings, left_on='documentId', right_on='_id')
             .set_index('createdAt')
    )


    cumulative_metric = calculate_core_content_metric_trend(core_content_views, start_date, end_date, posts_read_exponent, karma_exponent)
    return cumulative_metric.diff()


