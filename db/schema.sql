SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pgtap; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgtap WITH SCHEMA public;


--
-- Name: EXTENSION pgtap; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgtap IS 'Unit testing for PostgreSQL';


--
-- Name: get_post_id_from_path(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_post_id_from_path(path text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $$
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
$$;


SET default_tablespace = '';

--
-- Name: raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw (
    environment text,
    event_type text,
    "timestamp" timestamp without time zone,
    event jsonb
);


--
-- Name: post_timer_event; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.post_timer_event AS
 SELECT raw.environment,
    raw."timestamp",
    (raw.event ->> 'path'::text) AS path,
    (raw.event ->> 'tabId'::text) AS tabid,
    (raw.event ->> 'userId'::text) AS userid,
    ((raw.event ->> 'seconds'::text))::integer AS seconds,
    (raw.event ->> 'clientId'::text) AS clientid,
    ((raw.event ->> 'increment'::text))::integer AS increment,
    public.get_post_id_from_path((raw.event ->> 'path'::text)) AS post_id
   FROM public.raw
  WHERE ((raw.event_type = 'timerEvent'::text) AND (public.get_post_id_from_path((raw.event ->> 'path'::text)) IS NOT NULL));


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(255) NOT NULL
);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: raw_event_type_timestamp_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_timestamp_idx ON public.raw USING btree (event_type, "timestamp");


--
-- Name: raw_timestamp_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_timestamp_idx ON public.raw USING btree ("timestamp");


--
-- Name: raw_timestamp_idx1; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_timestamp_idx1 ON public.raw USING btree ("timestamp");


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20210824104514'),
    ('20210824110654'),
    ('20210824111234');
