-- raw:
--   environment
--   event_type
--   timestamp
--   event

-- timerEvents:
--   path
--   tabId
--   userId
--   seconds
--   clientId
--   increment

-- #############################################################################
-- migrate:up
-- #############################################################################

CREATE INDEX raw_timestamp ON raw (timestamp);
CREATE INDEX raw_event_type_timestamp ON raw (event_type, timestamp);

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

CREATE INDEX raw_event_type_post_id ON raw (event_type, get_post_id_from_path(event->>'path'));

-- Timer event is the main way to tell how many people have read your post and
-- for how long
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

-- #############################################################################
-- migrate:down
-- #############################################################################

DROP VIEW post_timer_event;

DROP INDEX raw_event_type_post_id;

DROP FUNCTION get_post_id_from_path(TEXT);

DROP INDEX raw_event_type_timestamp;
DROP INDEX raw_timestamp;
