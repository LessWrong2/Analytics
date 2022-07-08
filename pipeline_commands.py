
pipeline_commands = {
    'update_lessraw_small': """INSERT INTO lessraw_small
    SELECT DISTINCT ON (uuid) *
    FROM (SELECT *, md5(environment || timestamp::text || event_type || event::text) AS uuid
      FROM (SELECT environment, timestamp, event_type, (extract_raw_json(event)).*, event FROM raw) latest
      WHERE timestamp >= (SELECT MAX(timestamp) - INTERVAL '1 hours' FROM lessraw_small)
        AND environment = 'lesswrong.com'
        AND lw_team_member IS FALSE
        AND event_type NOT IN ('ssr', 'timerEvent', 'pageVisibilityChange', 'inViewEvent', 'hoverEventTriggered',
                               'idlenessDetection', 'postItemMounted', 'postListMounted')) latest
        WHERE NOT EXISTS(SELECT 1 FROM lessraw_small existing WHERE existing.uuid = latest.uuid);""",

    'update_lessraw_medium': """INSERT INTO lessraw_medium
        SELECT DISTINCT ON (uuid) *
        FROM (SELECT *, md5(environment || timestamp::text || event_type || event::text) AS uuid
              FROM (SELECT environment, timestamp, event_type, (extract_raw_json(event)).*, event FROM raw) latest
              WHERE timestamp >= (SELECT MAX(timestamp) - INTERVAL '1 hours' FROM lessraw_medium)
                AND environment = 'lesswrong.com'
                AND lw_team_member IS FALSE
                AND event_type NOT IN ('ssr', 'timerEvent', 'pageVisibilityChange', 'inViewEvent',
                                 'idlenessDetection')) latest
        WHERE NOT EXISTS(SELECT 1 FROM lessraw_medium existing WHERE existing.uuid = latest.uuid);""",

    'create_ssrs_cleaned_table': """CREATE TABLE ssrs_cleaned AS
        SELECT environment,
              event_type,
              timestamp,
              event ->> 'url'                                                                                     AS url,
              event ->> 'ip'                                                                                      AS ip,
              event ->> 'userId'                                                                                  AS user_id,
              event ->> 'clientId'                                                                                AS client_id,
              event ->> 'userAgent'                                                                               AS user_agent,
              event ->> 'tabId'                                                                                   AS tab_id,
              event ->> 'cached'                                                                                  AS cached,
              event ->> 'timings'                                                                                 AS timings,
              event ->> 'abTestGroups'                                                                            AS ab_test_group,
              event
        FROM raw
        WHERE event ->> 'userAgent' !~* 'Amazon-Route53|HealthCheck|Health_check|bot|spider|crawler|yeti|mastodon' AND
            event_type = 'ssr'
          AND timestamp > current_date - INTERVAL '30 days';
          
        create index ssrs_cleaned__timestamp
            on ssrs_cleaned (timestamp);

        create index ssrs_cleaned__tab_id
            on ssrs_cleaned (tab_id);

        create index ssrs_cleaned__user_agent_hash
            on ssrs_cleaned (md5(user_agent));""",

    'refresh_ssrs_cleaned_table': """REFRESH TABLE ssrs_cleaned""",
    'drop_ssrs_cleaned_table': """DROP TABLE ssrs_cleaned""",

    'create_core_events_cleaned_table': """CREATE TABLE core_events_cleaned AS
        SELECT lrs.*, ab_test_group FROM
        lessraw_small lrs
        JOIN ssrs_cleaned USING (tab_id)
        WHERE lrs.timestamp > current_date - INTERVAL '30 days'
        AND lrs.event_type IN ('pageLoadFinished', 'navigate', 'linkClicked');
        
        create index core_events_cleaned__timestamp
            on core_events_cleaned (timestamp);

        create index core_events_cleaned__user_id
            on core_events_cleaned (user_id);

        create index core_events_cleaned__client_id
            on core_events_cleaned (client_id);

        create index core_events_cleaned__url_hash
            on core_events_cleaned (md5(url_to));

        create index core_events_cleaned__event_type
            on core_events_cleaned (event_type);""",

    'refresh_core_events_cleaned_table': """REFRESH TABLE core_events_cleaned""",
    'drop_core_events_cleaned_table': """DROP TABLE core_events_cleaned""",

    'create_user_day_post_views_table': """CREATE TABLE user_day_post_views AS
        WITH user_day_post_views AS (
            SELECT DATE_TRUNC('day', timestamp) AS date,
                  COALESCE(user_id, client_id) AS user_client_id,
                  user_id IS NOT NULL          AS logged_in,
                  md5(url_to)                  AS url_hash,
                  COUNT(*)                     AS num_views,
                  array_agg(DISTINCT tab_id)   AS tab_ids
            FROM core_events_cleaned
            WHERE event_type IN ('navigate', 'pageLoadFinished')
              AND timestamp > current_date - INTERVAL '30 days'
            GROUP BY 1, 2, 3, 4
        ),
            aggregated_to_post AS (
                SELECT date,
                        user_client_id,
                        logged_in,
                        tab_ids,
                        document_id,
                        title,
                        array_agg(url_hash) AS url_hashes,
                        SUM(num_views)      AS num_views
                FROM user_day_post_views
                          JOIN urls USING (url_hash)
                WHERE urls.type = 'post'
                GROUP BY 1, 2, 3, 4, 5, 6
            ),
            with_uuid AS (
                SELECT *,
                        md5(date::text || COALESCE(user_client_id, 'missing') || COALESCE(document_id, 'missing')) AS uuid
                FROM aggregated_to_post
            )
        SELECT DISTINCT ON (uuid) *
        FROM with_uuid;

        create index user_day_post_views__index_date
            on user_day_post_views (date);

        create index user_day_post_views__index_user_client_id
            on user_day_post_views (user_client_id);

        create index user_day_post_views__index_document_id
            on user_day_post_views (document_id);

        create unique index user_day_post_views__index_uuid
            on user_day_post_views (uuid);

        create index user_day_post_views__index_logged_in
            on user_day_post_views (logged_in);""",

    'refresh_user_day_post_views_table': """REFRESH TABLE user_day_post_views""",
    'drop_user_day_post_views_table': """DROP TABLE user_day_post_views""",
}