-- migrate:up

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
    -- event_type is always 'timerEvent'
    timestamp,
    event->>'path' AS path,
    event->>'tabId' AS tab_id,
    event->>'userId' AS user_id,
    (event->>'seconds')::INTEGER AS seconds,
    event->>'clientId' AS client_id,
    (event->>'increment')::INTEGER AS increment,
    get_post_id_from_path(event->>'url') AS post_id
  FROM raw
  WHERE event_type = 'pageLoadFinished'
);

CREATE VIEW event_navigate AS (
  SELECT
    environment,
    -- event_type is always 'timerEvent'
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

--------------------------------------------------------------------------------
-- migrate:down

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
