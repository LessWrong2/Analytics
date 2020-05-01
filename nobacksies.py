import pandas as pd
import etlw as et
from utils import timed, htmlBody2plaintext, get_collection, get_mongo_db_object
from cellularautomaton import upload_to_gsheets



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


def get_lw_team(dfu):
    return (dfu[dfu['username']
            .isin(['Benito', 'habryka4', 'Raemon', 'jimrandomh', 'Ruby'])]
            .set_index('_id')['displayName']
            .to_frame('team_member_name')
            )


def enrich_tag_votes(votes, users):
    lw_team = get_lw_team(users)

    latest_vote = (votes
                   .groupby('documentId')['votedAt']
                   .max()
                   .to_frame('latest_vote_on_tag')
                   )

    votes_enriched = (votes
                      .merge(latest_vote, on='documentId')
                      .merge(lw_team, left_on='userId', right_index=True, how='left')
                      )

    votes_enriched['is_latest_vote'] = votes_enriched['latest_vote_on_tag'] == votes_enriched['votedAt']
    votes_enriched['voter_identity'] = votes_enriched['team_member_name'].fillna(
        'user: ' + votes_enriched['userId'].str.slice(6, 9))

    return votes_enriched


def get_team_vote_stats(votes_enriched):
    votes_enriched['voteType'] = votes_enriched['voteType'].astype(str)
    team_downvotes = (votes_enriched[votes_enriched['power'] < 1]
                      .groupby('documentId')
                      .size()
                      .to_frame('num_team_downvotes')
                      )

    team_votes = (votes_enriched[votes_enriched['team_member_name'].notnull()]
                  .groupby(['documentId', 'team_member_name'])['voteType']
                  .first()
                  .unstack('team_member_name')
                  .merge(team_downvotes, left_index=True, right_index=True, how='left')
                  )

    team_votes['num_team_downvotes'] = team_votes['num_team_downvotes'].fillna(0)

    return team_votes[['Ben Pace', 'habryka', 'jimrandomh', 'Raemon', 'Ruby', 'num_team_downvotes']]


def get_tags_on_post(tag_rels, tags):
    posts_with_tags = (tag_rels
                       .merge(tags[['_id', 'name']], left_on='tagId', right_on='_id')
                       .groupby('postId')['name']
                       .apply(lambda x: ';  '.join(x))
                       .to_frame('tags_on_post')
                       )

    return posts_with_tags


def enrich_tag_rels(tag_rels, tags, posts):
    def get_post_tag_ranks(tag_rels, posts):
        tag_positions = tag_rels.merge(posts[['_id', 'baseScore']], left_on='postId', right_on='_id',
                                       suffixes=['', '_post'], how='inner')
        tag_positions['tag_score_rank'] = tag_positions.groupby('tagId')['score'].rank(ascending=False, method='min')
        tag_positions['tie_break_rank'] = tag_positions.groupby(['tagId', 'tag_score_rank'])['baseScore_post'].rank(
            ascending=False, method='min')
        tag_positions['relevance_rank'] = tag_positions['tag_score_rank'] + tag_positions['tie_break_rank'] - 1

        return tag_positions[['_id', 'tagId', 'postId', 'relevance_rank']]

    tag_positions = get_post_tag_ranks(tag_rels, posts)
    tags_on_posts = get_tags_on_post(tag_rels, tags)

    tag_rels_enriched = (tag_rels
                         .drop(['baseScore'], axis=1)
                         .merge(posts[['_id', 'displayName', 'baseScore', 'title', 'postedAt']], left_on='postId',
                                right_on='_id', suffixes=['', '_post'], how='left')
                         .rename(
        {'_id': 'documentId', 'displayName': 'post_author', 'title': 'post_title', 'baseScore': 'post_karma',
         'postedAt': 'posted_at_post', 'score': 'relevance_score'}, axis=1)
                         .merge(tags[['_id', 'slug', 'name', 'core', 'postCount']], left_on='tagId', right_on='_id',
                                how='left', suffixes=['', '_tag'])
                         .rename(
        {'name': 'tag_name', 'core': 'is_core_tag', 'postCount': 'post_count', 'deleted': 'tag_deleted'}, axis=1)
                         .merge(tag_positions[['_id', 'relevance_rank']].set_index('_id'), left_on='documentId',
                                right_index=True, how='left')
                         .merge(tags_on_posts, left_on='postId', right_index=True, how='left')
                         )

    tag_rels_enriched['is_core_tag'] = tag_rels_enriched['is_core_tag'].fillna(False)

    return tag_rels_enriched


def get_mega_tag_votes_table(tags, tag_rels, tag_votes, posts, users):
    tag_votes_enriched = enrich_tag_votes(tag_votes, users)
    tag_rels_enriched = enrich_tag_rels(tag_rels, tags, posts)
    team_vote_stats = get_team_vote_stats(tag_votes_enriched)

    mega_tab_votes_table = (tag_rels_enriched
                            .merge(tag_votes_enriched, on='documentId', how='right', indicator=True)
                            .merge(team_vote_stats, on='documentId', how='left')
                            )

    return mega_tab_votes_table


def format_mega_votes_table(table):
    table = table.copy()
    table['tag_name'] = '=HYPERLINK("www.lesswrong.com/tag/'.lower() + table['slug'] + '", "' + table['tag_name'] + '")'
    table['post_title'] = '=HYPERLINK("www.lesswrong.com/posts/' + table['postId'] + '", "' + table[
        'post_title'].str.replace('"', '""') + '")'

    cols = [
        'post_author',
        'post_karma',
        'posted_at_post',
        'post_title',
        'is_core_tag',
        'tag_name',
        'votedAt',
        'latest_vote_on_tag',
        'is_latest_vote',
        'voter_identity',
        'voteType',
        'relevance_rank',
        'relevance_score',
        'voteCount',
        'Ben Pace',
        'habryka',
        'jimrandomh',
        'Raemon',
        'Ruby',
        'post_count',
        'tags_on_post',
        'tag_deleted',
        'num_team_downvotes'
    ]

    return table[cols].sort_values(['votedAt'], ascending=[False])

@timed
def run_tag_pipeline(dfs, upload=True):
    votes = dfs['votes']
    posts = dfs['posts']
    users = dfs['users']

    mongo_db = et.get_mongo_db_object()

    tags = et.get_collection('tags', mongo_db)
    tag_rels = et.get_collection('tagrels', mongo_db)

    tag_votes = votes[votes['collectionName']=='TagRels']

    tags_table = create_tags_table(tag_votes, tags.copy(), tag_rels, posts, mongo_db)
    tags_table_formatted = format_tags_table_for_upload(tags_table)
    tags_table_formatted['birth'] = pd.datetime.now()
    _ = upload_to_gsheets(tags_table_formatted,
                          'Tag Activity Dashboard', 'Tags',
                          format_columns=True)

    mega_tag_votes_table = get_mega_tag_votes_table(tags, tag_rels, tag_votes, posts, users)
    mega_tag_votes_table_formatted = format_mega_votes_table(mega_tag_votes_table)
    mega_tag_votes_table_formatted['birth'] = pd.datetime.now()
    _ = upload_to_gsheets(mega_tag_votes_table_formatted, 'Tag Activity Dashboard', 'All Activity', format_columns=True)

    mega_tag_votes_table_formatted['date'] = mega_tag_votes_table_formatted['votedAt'].dt.date
    _ = upload_to_gsheets(mega_tag_votes_table_formatted
                          .sort_values(['date', 'is_core_tag', 'tag_name', 'post_title', 'votedAt'],
                                       ascending=[False, False, True, True, False])
                          , 'Tag Activity Dashboard', 'All Activity (Daily)', create_sheet=True, format_columns=True)







