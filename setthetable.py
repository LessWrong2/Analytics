

table_creation_commands = {
    'users': """CREATE TABLE users
    (
        _id                                     varchar(64) PRIMARY KEY,
        username                                text,
        display_name                            text,
        created_at                              timestamp,
        post_count                              smallint,
        comment_count                           smallint,
        karma                                   integer,
        af_karma                                integer,
        legacy_karma                            integer,
        deleted                                 boolean,
        banned                                  boolean,
        legacy                                  boolean,
        shortform_feed_id                       varchar(256),
        sign_up_recaptcha_rating                real,
        reviewed_by_user_id                     varchar(64),
        earliest_activity                       timestamp,
        true_earliest                           timestamp,
        most_recent_activity                    timestamp,
        days_since_active                       real,
        total_posts                             numeric,
        earliest_post                           timestamp,
        most_recent_post                        timestamp,
        num_drafts                              smallint,
        percent_drafts                          real,
        total_comments                          numeric,
        earliest_comment                        timestamp,
        most_recent_comment                     timestamp,
        num_votes                             numeric,
        most_recent_vote                        timestamp,
        earliest_vote                           timestamp,
        percent_downvotes                       real,
        percent_bigvotes                        real,
        most_recent_view                        timestamp,
        earliest_view                           timestamp,
        num_distinct_posts_viewed               numeric,
        num_days_present_last_30_days           numeric,
        num_posts_last_30_days                  numeric,
        num_comments_last_30_days               numeric,
        num_votes_last_30_days                  numeric,
        num_views_last_30_days                  numeric,
        num_distinct_posts_viewed_last_30_days  numeric,
        num_posts_last_180_days                 numeric,
        num_comments_last_180_days              numeric,
        num_votes_last_180_days                 numeric,
        num_views_last_180_days                 numeric,
        num_distinct_posts_viewed_last_180_days numeric,
        bio                                     text,
        email                                   text
    ); """,
    'posts': """CREATE TABLE posts
    (
        _id                             varchar(64) PRIMARY KEY,
        user_id                         varchar(64) NOT NULL,
        posted_at                       timestamp   NOT NULL,
        username                        text,
        displayName                     text,
        title                           text,
        af                              BOOLEAN     NOT NULL,
        base_score                      numeric     NOT NULL,
        af_base_score                   numeric,
        score                           numeric,
        draft                           BOOLEAN,
        question                        BOOLEAN,
        is_event                        BOOLEAN,
        view_count                      numeric,
        view_count_logged               numeric,
        click_count                     numeric,
        comment_count                   numeric,
        num_comments_rederived          numeric,
        num_distinct_viewers            numeric,
        num_distinct_commenters         numeric,
        word_count                      numeric,
        num_votes                       numeric,
        small_upvote                    numeric,
        big_upvote                      numeric,
        small_downvote                  numeric,
        big_downvote                    numeric,
        percent_downvotes               numeric,
        percent_bigvotes               numeric,
        url                             text,
        slug                            text,
        canonical_collection_slug       text,
        website                         text,
        gw                              BOOLEAN     NOT NULL,
        frontpaged                      BOOLEAN     NOT NULL,
        frontpage_date                  timestamp,
        curated_date                    timestamp,
        moderation_guidelines_html_body text,
        status                          numeric,
        legacy_spam                     BOOLEAN     NOT NULL,
        author_is_unreviewed            BOOLEAN     NOT NULL,
        most_recent_comment             timestamp,
        user_agent                      text,
        createdAt                       timestamp   NOT NULL,
        birth                           timestamp
    );""",
    'comments': """CREATE TABLE comments
    (
        _id               varchar(64) PRIMARY KEY,
        user_id           varchar(64) NOT NULL,
        username          varchar(64),
        displayName       text,
        post_id           varchar(64),
        posted_at         timestamp   NOT NULL,
        af                BOOLEAN NOT NULL,
        base_score        numeric,
        score             numeric,
        answer            BOOLEAN,
        parent_answer_id  varchar(64),
        parent_comment_id varchar(64),
        word_count        numeric,
        top_level         BOOLEAN,
        gw                BOOLEAN,
        num_votes         numeric,
        percent_downvotes numeric,
        percent_bigvotes  numeric,
        small_upvote      numeric,
        big_upvote        numeric,
        small_downvote    numeric,
        big_downvote      numeric,
        user_agent        text,
        created_at        timestamp,
        birth             timestamp
    );""",
    'votes': """CREATE TABLE votes
    (
        document_id     varchar(64) NOT NULL,
        collection_name varchar(10) NOT NULL,
        user_id         varchar(64) NOT NULL,
        vote_type       varchar(16) NOT NULL,
        power           smallint    NOT NULL,
        voted_at        timestamp   NOT NULL,
        cancelled       BOOLEAN     NOT NULL,
        is_unvote       BOOLEAN     NOT NULL,
        af_power        smallint    NOT NULL,
        legacy          BOOLEAN     NOT NULL
    );""",
    'views': """CREATE TABLE views
    (
        user_id     varchar(64) NOT NULL,
        document_id varchar(64),
        created_at  timestamp   NOT NULL,
        date        date
    );"""
}
