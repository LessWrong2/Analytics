SELECT *
FROM raw
WHERE timestamp > '2019-11-28 10:30'
  AND environment = 'development'

CREATE TABLE lessraw AS
    (
        SELECT environment,
               timestamp,
               event ->> 'clientId' AS client_id,
               event ->> 'userId'   AS user_id,
               event ->> 'tabId'    AS tab_id,
               event ->> 'url'      AS url,
               event ->> 'referrer' AS referrer
        FROM raw
        WHERE event_type = 'pageLoadFinished'
          AND environment = 'lesswrong.com'
          AND timestamp BETWEEN '2019-11-26' AND '2019-11-27'
    );

SELECT *
FROM raw
WHERE timestamp > '2019-11-28 10:30'
  AND environment = 'development'

WITH filtered_raw AS
    (SELECT * FROM raw WHERE environment='development' AND timestamp > '2019-11-28 11:30')
SELECT * FROM
(
SELECT environment,
       timestamp,
       event ->> 'clientId' AS client_id,
       event ->> 'userId'   AS user_id,
       event ->> 'tabId'    AS tab_id,
       event ->> 'listContext' AS list_context,
       jsonb_array_elements_text(event -> 'postIds') AS post_id
FROM filtered_raw
  WHERE event_type = 'postListMounted'
UNION
SELECT environment,
       timestamp,
       event ->> 'clientId' AS client_id,
       event ->> 'userId'   AS user_id,
       event ->> 'tabId'    AS tab_id,
       event ->> 'listContext' AS list_context,
       event ->> 'postId' AS post_id
FROM filtered_raw
WHERE event_type = 'postItemMounted'
    ) post_items_mounted
LEFT JOIN
    (SELECT _id , title, username AS author, base_score FROM posts) posts
ON posts._id = post_items_mounted.post_id

ORDER BY user_id DESC, tab_id, list_context, timestamp ASC