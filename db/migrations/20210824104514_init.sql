-- #############################################################################
-- migrate:up
-- #############################################################################

--CREATE TABLE raw (
--  environment TEXT,
--  event_type TEXT,
--  timestamp TIMESTAMP WITHOUT TIME ZONE,
--  event JSONB
--);

-- #############################################################################
-- migrate:down
-- #############################################################################

-- DROP TABLE raw;
