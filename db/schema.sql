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
  post_id := (SELECT (regexp_matches(path, '^(https?://[^/]*?)?/posts/([a-zA-Z0-9]+)/'))[2]);
  if (post_id IS NOT NULL) THEN
    RETURN post_id;
  END IF;
  post_id := (SELECT (regexp_matches(path, '^(https?://[^/]*?)?/s/[a-zA-Z0-9]+/p/([a-zA-Z0-9]+)'))[2]);
  RETURN post_id;
END
$$;


SET default_tablespace = '';

SET default_with_oids = false;

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
-- Name: event_navigate; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.event_navigate AS
 SELECT raw.environment,
    raw."timestamp",
    (raw.event ->> 'tabId'::text) AS tab_id,
    (raw.event ->> 'userId'::text) AS user_id,
    (raw.event ->> 'clientId'::text) AS client_id,
    (raw.event ->> 'from'::text) AS "from",
    (raw.event ->> 'to'::text) AS "to",
    public.get_post_id_from_path((raw.event ->> 'to'::text)) AS to_post_id,
    public.get_post_id_from_path((raw.event ->> 'from'::text)) AS from_post_id
   FROM public.raw
  WHERE (raw.event_type = 'navigate'::text);


--
-- Name: event_page_load_finished; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.event_page_load_finished AS
 SELECT raw.environment,
    raw."timestamp",
    (raw.event ->> 'url'::text) AS url,
    (raw.event ->> 'tabId'::text) AS tab_id,
    (raw.event ->> 'userId'::text) AS user_id,
    (raw.event ->> 'clientId'::text) AS client_id,
    (raw.event ->> 'referrer'::text) AS referrer,
    public.get_post_id_from_path((raw.event ->> 'url'::text)) AS post_id,
    (((raw.event ->> 'performance'::text))::jsonb ->> 'memory'::text) AS performance_memory,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'navigationStart'::text) AS performance_timing_navigation_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'redirectStart'::text) AS performance_timing_redirect_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'redirectEnd'::text) AS performance_timing_redirect_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'fetchStart'::text) AS performance_timing_fetch_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domainLookupStart'::text) AS performance_timing_domain_lookup_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domainLookupEnd'::text) AS performance_timing_domain_lookup_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'connectStart'::text) AS performance_timing_connect_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'secureConnectionStart'::text) AS performance_timing_secure_connection_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'connectEnd'::text) AS performance_timing_connect_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'requestStart'::text) AS performance_timing_request_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'responseStart'::text) AS performance_timing_response_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'unloadEventStart'::text) AS performance_timing_unload_event_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'unloadEventEnd'::text) AS performance_timing_unload_event_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'responseEnd'::text) AS performance_timing_response_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domLoading'::text) AS performance_timing_dom_loading,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domInteractive'::text) AS performance_timing_dom_interactive,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domContentLoadedEventStart'::text) AS performance_timing_dom_content_loaded_event_start,
    (((((raw.event ->> 'perormance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domContentLoadedEventEnd'::text) AS performance_timing_dom_content_loaded_event_end,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'domComplete'::text) AS performance_timing_dom_complete,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'loadEventStart'::text) AS performance_timing_load_event_start,
    (((((raw.event ->> 'performance'::text))::jsonb ->> 'timing'::text))::jsonb ->> 'loadEventEnd'::text) AS performance_timing_load_event_end,
    (((raw.event ->> 'performance'::text))::jsonb ->> 'timeOrigin'::text) AS performance_time_origin,
    (((raw.event ->> 'browserProps'::text))::jsonb ->> 'userAgent'::text) AS browser_props_user_agent,
    ((((raw.event ->> 'browserProps'::text))::jsonb ->> 'mobile'::text))::boolean AS browser_props_mobile,
    ((((raw.event ->> 'browserProps'::text))::jsonb ->> 'tablet'::text))::boolean AS browser_props_tablet,
    ((((raw.event ->> 'browserProps'::text))::jsonb ->> 'chrome'::text))::boolean AS browser_props_chrome,
    ((((raw.event ->> 'browserProps'::text))::jsonb ->> 'firefox'::text))::boolean AS browser_props_firefox,
    ((((raw.event ->> 'browserProps'::text))::jsonb ->> 'safari'::text))::boolean AS browser_props_safari,
    (((raw.event ->> 'browserProps'::text))::jsonb ->> 'osname'::text) AS browser_props_osname,
    (((((raw.event ->> 'browserProps'::text))::jsonb ->> 'blocks'::text))::jsonb ->> 'blocksGA'::text) AS browser_props_blocks_ga,
    (((((raw.event ->> 'browserProps'::text))::jsonb ->> 'blocks'::text))::jsonb ->> 'blocksGTM'::text) AS browser_props_blocks_gtm
   FROM public.raw
  WHERE (raw.event_type = 'pageLoadFinished'::text);


--
-- Name: event_timer_event; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.event_timer_event AS
 SELECT raw.environment,
    raw."timestamp",
    (raw.event ->> 'path'::text) AS path,
    (raw.event ->> 'tabId'::text) AS tab_id,
    (raw.event ->> 'userId'::text) AS user_id,
    ((raw.event ->> 'seconds'::text))::integer AS seconds,
    (raw.event ->> 'clientId'::text) AS client_id,
    ((raw.event ->> 'increment'::text))::integer AS increment,
    public.get_post_id_from_path((raw.event ->> 'path'::text)) AS post_id
   FROM public.raw
  WHERE (raw.event_type = 'timerEvent'::text);


--
-- Name: page_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.page_view AS
 SELECT a.environment,
    a.event_type,
    a."timestamp",
    a.tab_id,
    a.user_id,
    a.client_id,
    a.post_id
   FROM ( SELECT event_page_load_finished.environment,
            'page_load_finished'::text AS event_type,
            event_page_load_finished."timestamp",
            event_page_load_finished.tab_id,
            event_page_load_finished.user_id,
            event_page_load_finished.client_id,
            event_page_load_finished.post_id
           FROM public.event_page_load_finished
        UNION ALL
         SELECT event_navigate.environment,
            'navigate'::text AS event_type,
            event_navigate."timestamp",
            event_navigate.tab_id,
            event_navigate.user_id,
            event_navigate.client_id,
            event_navigate.to_post_id AS post_id
           FROM public.event_navigate) a;


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
-- Name: raw_event_type_post_id_navigate; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_navigate ON public.raw USING btree (public.get_post_id_from_path((event ->> 'to'::text))) WHERE (event_type = 'navigate'::text);


--
-- Name: raw_event_type_post_id_page_load_finished; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_page_load_finished ON public.raw USING btree (public.get_post_id_from_path((event ->> 'url'::text))) WHERE (event_type = 'pageLoadFinished'::text);


--
-- Name: raw_event_type_post_id_timer_event; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_timer_event ON public.raw USING btree (public.get_post_id_from_path((event ->> 'path'::text))) WHERE (event_type = 'timerEvent'::text);


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
    ('20210824111234'),
    ('20210917095409');
