-- #############################################################################
-- migrate:up
-- #############################################################################

-- Before this we needed to run:
-- forumanalytics=> update raw set event = (event#>>'{}')::jsonb;
-- which casts the older text strings to actual JSONB

DROP INDEX IF EXISTS raw_event_type_post_id;

CREATE or replace FUNCTION get_post_id_from_path(path TEXT)
RETURNS TEXT AS $$
DECLARE
  post_id TEXT;
BEGIN
  -- /posts/4fPxQjq6GFZgurSsf/room-for-other-things-how-to-adjust-if-ea-seems-overwhelming
  -- /s/asdf/p/qwer
  post_id := (SELECT (regexp_matches(path, '^(https?://[^/]*?)?/posts/([a-zA-Z0-9]+)/'))[2]);
  if (post_id IS NOT NULL) THEN
    RETURN post_id;
  END IF;
  post_id := (SELECT (regexp_matches(path, '^(https?://[^/]*?)?/s/[a-zA-Z0-9]+/p/([a-zA-Z0-9]+)'))[2]);
  RETURN post_id;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE INDEX IF NOT EXISTS raw_event_type_post_id_timer_event ON raw (get_post_id_from_path(event->>'path')) WHERE event_type = 'timerEvent';
CREATE INDEX IF NOT EXISTS raw_event_type_post_id_page_load_finished ON raw (get_post_id_from_path(event->>'url')) WHERE event_type = 'pageLoadFinished';
CREATE INDEX IF NOT EXISTS raw_event_type_post_id_navigate ON raw (get_post_id_from_path(event->>'to')) WHERE event_type = 'navigate';

-- Our old view restricts to timer events on posts, we're now just wanting to
-- do all timer events
DROP VIEW post_timer_event;

CREATE VIEW event_timer_event AS (
  SELECT
    environment,
    -- event_type is always 'timerEvent'
    timestamp,
    event->>'path' AS path,
    event->>'tabId' AS tab_id,
    event->>'userId' AS user_id,
    (event->>'seconds')::INTEGER AS seconds,
    event->>'clientId' AS client_id,
    (event->>'increment')::INTEGER AS increment,
    get_post_id_from_path(event->>'path') AS post_id
  FROM raw
  WHERE event_type = 'timerEvent'
);

-- TODO: these paths aren't necessarily correct and need to be updated for
-- whatever a pageLoadFinished event normally has
CREATE VIEW event_page_load_finished AS (
  SELECT
    environment,
    -- event_type is always 'pageLoadFinished'
    timestamp,
    event->>'url' AS url,
    event->>'tabId' AS tab_id,
    event->>'userId' AS user_id,
    event->>'clientId' AS client_id,
    event->>'referrer' AS referrer,
    get_post_id_from_path(event->>'url') AS post_id,
    (event->>'performance')::JSONB->>'memory' AS performance_memory,
    -- https://stackoverflow.com/questions/16808486/explanation-of-window-performance-javascript
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'navigationStart'
      AS performance_timing_navigation_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'redirectStart'
      AS performance_timing_redirect_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'redirectEnd'
      AS performance_timing_redirect_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'fetchStart'
      AS performance_timing_fetch_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domainLookupStart'
      AS performance_timing_domain_lookup_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domainLookupEnd'
      AS performance_timing_domain_lookup_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'connectStart'
      AS performance_timing_connect_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'secureConnectionStart'
      AS performance_timing_secure_connection_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'connectEnd'
      AS performance_timing_connect_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'requestStart'
      AS performance_timing_request_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'responseStart'
      AS performance_timing_response_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'unloadEventStart'
      AS performance_timing_unload_event_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'unloadEventEnd'
      AS performance_timing_unload_event_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'responseEnd'
      AS performance_timing_response_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domLoading'
      AS performance_timing_dom_loading,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domInteractive'
      AS performance_timing_dom_interactive,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domContentLoadedEventStart'
      AS performance_timing_dom_content_loaded_event_start,
    ((event->>'perormance')::JSONB->>'timing')::JSONB->>'domContentLoadedEventEnd'
      AS performance_timing_dom_content_loaded_event_end,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'domComplete'
      AS performance_timing_dom_complete,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'loadEventStart'
      AS performance_timing_load_event_start,
    ((event->>'performance')::JSONB->>'timing')::JSONB->>'loadEventEnd'
      AS performance_timing_load_event_end,
    (event->>'performance')::JSONB->>'timeOrigin' AS performance_time_origin,
    (event->>'browserProps')::JSONB->>'userAgent' AS browser_props_user_agent,
    -- The following come from 'bowser'
    ((event->>'browserProps')::JSONB->>'mobile')::BOOLEAN AS browser_props_mobile,
    ((event->>'browserProps')::JSONB->>'tablet')::BOOLEAN AS browser_props_tablet,
    ((event->>'browserProps')::JSONB->>'chrome')::BOOLEAN AS browser_props_chrome,
    ((event->>'browserProps')::JSONB->>'firefox')::BOOLEAN AS browser_props_firefox,
    ((event->>'browserProps')::JSONB->>'safari')::BOOLEAN AS browser_props_safari,
    (event->>'browserProps')::JSONB->>'osname' AS browser_props_osname,
    ((event->>'browserProps')::JSONB->>'blocks')::JSONB->>'blocksGA' AS browser_props_blocks_ga,
    ((event->>'browserProps')::JSONB->>'blocks')::JSONB->>'blocksGTM' AS browser_props_blocks_gtm
  FROM raw
  WHERE event_type = 'pageLoadFinished'
);

CREATE VIEW event_navigate AS (
  SELECT
    environment,
    -- event_type is always 'navigate'
    timestamp,
    event->>'tabId' AS tab_id,
    event->>'userId' AS user_id,
    event->>'clientId' AS client_id,
    event->>'from' AS from,
    event->>'to' AS to,
    get_post_id_from_path(event->>'to') AS to_post_id,
    get_post_id_from_path(event->>'from') AS from_post_id
  FROM raw
  WHERE event_type = 'navigate'
);

CREATE VIEW page_view AS (
  SELECT
    environment,
    event_type,
    timestamp,
    tab_id,
    user_id,
    client_id,
    post_id
  FROM (
    SELECT
      environment,
      'page_load_finished' AS event_type,
      timestamp,
      tab_id,
      user_id,
      client_id,
      post_id
    FROM event_page_load_finished
    UNION ALL
    SELECT
      environment,
      'navigate' AS event_type,
      timestamp,
      tab_id,
      user_id,
      client_id,
      to_post_id AS post_id
    FROM event_navigate
  ) a
);

-- #############################################################################
-- migrate:down
-- #############################################################################

-- DROP INDEX raw_event_type_post_id_timer_event;
-- DROP INDEX raw_event_type_post_id_page_load_finished;
-- DROP INDEX raw_event_type_post_id_navigate;


CREATE or replace FUNCTION get_post_id_from_path(path TEXT)
RETURNS TEXT AS $$
DECLARE
  post_id TEXT;
BEGIN
  -- /posts/4fPxQjq6GFZgurSsf/room-for-other-things-how-to-adjust-if-ea-seems-overwhelming
  -- /s/asdf/p/qwer
  post_id := (SELECT (regexp_matches(path, '^/posts/([a-zA-Z0-9]+)/'))[1]);
  if (post_id IS NOT NULL) THEN
    RETURN post_id;
  END IF;
  post_id := (SELECT (regexp_matches(path, '^/s/[a-zA-Z0-9]+/p/([a-zA-Z0-9]+)'))[1]);
  RETURN post_id;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- CREATE INDEX raw_event_type_post_id ON raw (event_type, get_post_id_from_path(event->>'path'));

DROP VIEW page_view;

DROP VIEW event_timer_event;
DROP VIEW event_page_load_finished;
DROP VIEW event_navigate;

CREATE VIEW post_timer_event AS (
  SELECT
    environment,
    -- event_type is always 'timerEvent'
    timestamp,
    event->>'path' AS path,
    event->>'tabId' AS tabId,
    event->>'userId' AS userId,
    (event->>'seconds')::INTEGER AS seconds,
    event->>'clientId' AS clientId,
    (event->>'increment')::INTEGER AS increment,
    get_post_id_from_path(event->>'path') AS post_id
  FROM raw
  WHERE event_type = 'timerEvent'
    AND get_post_id_from_path(event->>'path') IS NOT NULL
);
