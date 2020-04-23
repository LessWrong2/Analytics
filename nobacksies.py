from utils import htmlBody2plaintext
import etlw as et



def format_tags_posts_list(posts):
    return ' '.join(['({number}) {title};  '.format(number=post.Index + 1, title=post.title)
                     for post in posts.reset_index().itertuples()])


def create_tag_posts_list(tag_rels, posts):
    return (tag_rels
            .merge(posts[['_id', 'title']], left_on='postId', right_on='_id')
            .groupby('tagId')
            .apply(format_tags_posts_list)
            .to_frame('posts')
            )


def create_tags_table(votes, tags, tag_rels, posts, mongo_db_object):
    """Main creation of tags table happens here"""

    earliest_vote_on_tag = (votes
                            .merge(tag_rels, left_on='documentId', right_on='_id')
                            .groupby('tagId')['votedAt'].min()
                            .to_frame('earliest_vote_on_tag')
                            )

    latest_post_added = (votes
                         .merge(tag_rels[tag_rels['voteCount'] == 1], left_on='documentId', right_on='_id')
                         .groupby(['tagId'])['votedAt'].max()
                         .to_frame('latest_post_added')
                         )

    tag_posts_list = create_tag_posts_list(tag_rels, posts)

    tags_enriched = (tags
                     .merge(earliest_vote_on_tag, left_on='_id', right_index=True, how='left')
                     .merge(latest_post_added, left_on='_id', right_index=True, how='left')
                     .merge(tag_posts_list, left_on='_id', right_index=True, how='left')
                     )

    tags_enriched['description'] = htmlBody2plaintext(tags_enriched['description'].str['html'])
    tags_enriched['description_last_edited'] = tags_enriched['description'].str['editedAt']

    tag_index_body = list(mongo_db_object['posts'].find({'_id': 'DHJBEsi4XJDw2fRFq'}))[0]['contents']['html']
    tags_enriched['in_tag_index'] = tags_enriched['slug'].apply(lambda x: ('lesswrong.com/tag/' + x) in tag_index_body)

    return tags_enriched


def format_tags_table_for_upload(tags_table):
    tags_table['name'] = '=HYPERLINK("www.lesswrong.com/tag/'.lower() + tags_table['slug'] + '", "' + tags_table[
        'name'] + '")'
    tags_table['postCount'] = tags_table['postCount'].fillna(0)

    cols = ['earliest_vote_on_tag',
            'latest_post_added',
            'name',
            'postCount',
            'in_tag_index',
            'description',
            'posts',
            'description_last_edited',
            'deleted',
            ]

    output = tags_table[cols].copy()
    output = output.sort_values(['deleted', 'in_tag_index', 'latest_post_added'],
                                ascending=[True, True, False])
    output = output.rename({
        'name': 'Tag Name',
        'postCount': 'Post Count',
    }, axis=1
    )

    return output





def run_tag_pipeline(dfs):
    mongo_db = et.get_mongo_db_object()

    tags = et.get_collection('tags', mongo_db)
    tag_rels = et.get_collection('tagrels', mongo_db)

    votes = dfs['votes']
    tag_votes = votes[votes['collectionName']=='TagRels']
    posts = dfs['posts']


    tags_enriched = create_tags_table(tag_votes, tags, tag_rels, posts, mongo_db)
    tags_table_formatted = format_tags_table_for_upload(tags_enriched)

    return tags_table_formatted




