

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
        deleted                                 boolean,
        banned                                  boolean,
        legacy                                  boolean,
        shortform_feed_id                       varchar(256),
        sign_up_re_captcha_rating               real,
        reviewed_by_user_id                     varchar(64),
        earliest_activity                       timestamp,
        true_earliest                           timestamp,
        most_recent_activity                    timestamp,
        days_since_active                       real,
        total_posts                             numeric,
        earliest_post                           timestamp,
        most_recent_post                        timestamp,
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
        walled_garden_invite                    boolean,
        hide_walled_garden_ui                   boolean,
        email                                   text,
        birth                                   timestamp
    ); """,
    'posts': """CREATE TABLE posts
    (
        _id                             varchar(64) PRIMARY KEY,
        user_id                         varchar(64) NOT NULL,
        posted_at                       timestamp   NOT NULL,
        username                        text,
        display_name                    text,
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
        status                          numeric,
        legacy_spam                     BOOLEAN     NOT NULL,
        author_is_unreviewed            BOOLEAN     NOT NULL,
        most_recent_comment             timestamp,
        user_agent                      text,
        created_at                       timestamp   NOT NULL,
        birth                           timestamp
    );""",
    'comments': """CREATE TABLE comments
    (
        _id               varchar(64) PRIMARY KEY,
        user_id           varchar(64) NOT NULL,
        username          varchar(64),
        display_name       text,
        post_id           varchar(64),
        posted_at         timestamp   NOT NULL,
        af                BOOLEAN NOT NULL,
        base_score        numeric,
        score             numeric,
        answer            BOOLEAN,
        parent_answer_id  varchar(64),
        parent_comment_id varchar(64),
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
        deleted           BOOLEAN,
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
        legacy          BOOLEAN     NOT NULL,
        birth           timestamp
    );""",
    'views': """CREATE TABLE views
    (
        user_id     varchar(64) NOT NULL,
        document_id varchar(64),
        created_at  timestamp   NOT NULL,
        birth       timestamp
    );""",
    'user_agents': """CREATE TABLE user_agents
(
    ua_string           text NOT NULL,
    ua_pretty           text,
    browser_family      varchar(128),
    browser_version     varchar(128),
    os_family           varchar(64),
    os_version          varchar(64),
    device_family       varchar(256),
    device_brand         varchar(64),
    device_model        varchar(256),
    is_mobile           boolean,
    is_tablet           boolean,
    is_mobile_or_tablet boolean,
    is_desktop          boolean,
    is_bot              boolean,
    birth               timestamp
);""",
    'tags': """CREATE TABLE tags
(
    created_at         timestamp,
    _id               varchar(64) PRIMARY KEY,
    name              text,
    slug              text,
    deleted           boolean,
    post_count         smallint,
    admin_only         boolean,
    core              boolean,
    suggested_as_filter boolean,
    default_order      numeric,
    promoted          boolean,
    birth               timestamp
);""",
    'tagrels': """CREATE TABLE tagrels
(
    created_at         timestamp,
    _id               varchar(64) PRIMARY KEY,
    tag_id               varchar(64), 
    post_id               varchar(64),
    user_id               varchar(64),
    base_score      smallint,
    score           smallint,
    inactive        boolean,
    vote_count      smallint,
    af_base_score   smallint,
    deleted         boolean,
    name            text,
    title           text,
    user_id_post    varchar(64),
    author          text,
    base_score_post    smallint,       
    birth               timestamp
);""",
    'sequences': """CREATE TABLE sequences
(
    _id               varchar(64) PRIMARY KEY,
    user_id           varchar(64),
    title             text,
    created_at         timestamp,
    draft             boolean,
    is_deleted         boolean,
    hidden             boolean,
    schema_version     smallint,
    plaintext_description text,
    birth       timestamp
);""",
    'urls': """CREATE TABLE urls
(
    url              text,
    type        varchar(32),
    title            text,
    author           text,
    document_id  varchar(64),
    url_hash    varchar(32) PRIMARY KEY,
    birth       timestamp
);""",
    'ga_traffic': """CREATE TABLE ga_traffic
(
    date                    timestamp,
    ga_users                int,
    ga_sessions             int,
    ga_pageviews            int,
    ga_unique_pageviews     int,
    ga_pageviews_per_session numeric,
    birth                   timestamp
);""",
    'ga_source': """CREATE TABLE ga_source
(
    date                    timestamp,
    source_agg              text,
    ga_source               text,
    ga_users                int,
    ga_sessions             int,
    birth                   timestamp
);""",
    'ga_referrer': """CREATE TABLE ga_referrer
(
    date                    timestamp,
    referrer_agg            text,
    ga_full_referrer        text,
    ga_users                int,
    ga_sessions             int,
    birth                   timestamp
);""",
    'ga_devices': """CREATE TABLE ga_devices
(
    date                timestamp,
    ga_device_category  varchar(32),
    ga_users            int,
    ga_sessions         int,
    ga_pageviews        int,
    ga_unique_pageviews  int,
    birth               timestamp
);""",
    'ga_pages': """CREATE TABLE ga_pages
(
    date                    timestamp,
    page_agg                text,
    ga_page_path            text,
    ga_users                int,
    ga_sessions             int,
    ga_pageviews            int,
    ga_unique_pageviews      int,
    ga_avg_time_on_page     numeric,
    ga_avg_page_load_time   numeric,
    birth                   timestamp
);""",
                   'gather_town_checks': """CREATE TABLE gather_town_checks
(
  timestamp         timestamp,
  player_id         text,
  name              varchar(64),
  busy              int,
  audio             boolean,
  video             boolean,
  blocked           text,
  elapsed_min       numeric,
  first_visit       boolean,
  new_session       boolean,
  session_no        int,
  lw_team           boolean,
  birth             timestamp
);""",
    'gather_town_sessions': """CREATE TABLE gather_town_sessions
(
  player_id         text,
  name              varchar(64),
  session_no        int,
  num_checks        int,
  max_gap           numeric,
  start_time        timestamp,
  end_time          timestamp,
  first_visit       boolean,
  lw_team           boolean,
  approx_duration   numeric,
  alone_at_start    boolean,
  alone_at_end      boolean,
  percent_accompanied numeric,
  concurrent_visitors   text,
  birth             timestamp
);""",
    'gather_town_users': """CREATE TABLE gather_town_users
(
  player_id             text,
  name                  varchar(64),
  num_sessions          int,
  num_checks            int,
  num_distinct_days     int,
  first_seen            timestamp,
  last_seen             timestamp,
  total_approx_duration numeric,
  mean_session_length   numeric,
  median_session_length numeric,
  max_session_length   numeric,
  min_session_length   numeric,
  lw_team               boolean,
  birth                 timestamp
);"""
}
