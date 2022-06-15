SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: ben; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ben;


--
-- Name: fb_to_mode; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA fb_to_mode;


--
-- Name: jacob; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA jacob;


--
-- Name: jim; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA jim;


--
-- Name: oli; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA oli;


--
-- Name: ray; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ray;


--
-- Name: ruby; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ruby;


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: pgtap; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgtap WITH SCHEMA public;


--
-- Name: EXTENSION pgtap; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgtap IS 'Unit testing for PostgreSQL';


--
-- Name: unpacked_event; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.unpacked_event AS (
	tab_id text,
	client_id text,
	user_id text,
	path text,
	page_context text,
	page_section_context text,
	url_to text,
	referrer_from text,
	lw_team_member boolean
);


--
-- Name: extract_raw_json(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.extract_raw_json(event jsonb) RETURNS public.unpacked_event
    LANGUAGE plpgsql
    AS $$
DECLARE
    fields unpacked_event;
BEGIN
    SELECT event ->> 'tabId'                                                                                         AS tab_id,
           event ->> 'clientId'                                                                                      AS client_id,
           event ->> 'userId'                                                                                        AS user_id,
           event ->> 'path'                                                                                          AS path,
           event ->> 'pageContext'                                                                                   AS page_context,
           event ->> 'pageSectionContext'                                                                            AS page_section_context,
           REPLACE(COALESCE(event ->> 'url', event ->> 'to'), 'https://www.lesswrong.com',
                   '')                                                                                               AS url_to,
           REPLACE(COALESCE(event ->> 'referrer', event ->> 'from'), 'https://www.lesswrong.com',
                   '')                                                                                               AS referrer_from,
           event ->> 'userId' IN ('nLbwLhBaQeG6tCNDN', 'qgdGA4ZEyW7zNdK84', 'EQNTWXLKMeWMp2FQS', 'r38pkCm7wF4M44MDQ',
                                  'XtphY3uYHwruKqDyG') AND NOT event->>'userId' IS NULL                                                               AS lw_team_member
    INTO fields;
    RETURN fields;
END
$$;


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
-- Name: _sdc_rejected; Type: TABLE; Schema: fb_to_mode; Owner: -
--

CREATE TABLE fb_to_mode._sdc_rejected (
    record text,
    reason text,
    table_name text,
    _sdc_rejected_at timestamp with time zone
);


--
-- Name: ads; Type: TABLE; Schema: fb_to_mode; Owner: -
--

CREATE TABLE fb_to_mode.ads (
    _sdc_batched_at timestamp with time zone,
    _sdc_extracted_at timestamp with time zone,
    _sdc_received_at timestamp with time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    id text NOT NULL,
    updated_time timestamp with time zone NOT NULL
);


--
-- Name: ads_insights; Type: TABLE; Schema: fb_to_mode; Owner: -
--

CREATE TABLE fb_to_mode.ads_insights (
    _sdc_batched_at timestamp with time zone,
    _sdc_extracted_at timestamp with time zone,
    _sdc_received_at timestamp with time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    ad_id text NOT NULL,
    adset_id text NOT NULL,
    campaign_id text NOT NULL,
    clicks bigint,
    conversion_rate_ranking text,
    cost_per_inline_link_click double precision,
    cost_per_inline_post_engagement double precision,
    date_start timestamp with time zone NOT NULL,
    reach bigint,
    spend double precision,
    unique_clicks bigint,
    account_name text,
    ad_name text
);


--
-- Name: ads_insights__actions; Type: TABLE; Schema: fb_to_mode; Owner: -
--

CREATE TABLE fb_to_mode.ads_insights__actions (
    "1d_click" double precision,
    "1d_view" double precision,
    "28d_click" double precision,
    "28d_view" double precision,
    "7d_click" double precision,
    "7d_view" double precision,
    _sdc_batched_at timestamp with time zone,
    _sdc_level_0_id bigint NOT NULL,
    _sdc_received_at timestamp with time zone,
    _sdc_sequence bigint,
    _sdc_source_key_ad_id text NOT NULL,
    _sdc_source_key_adset_id text NOT NULL,
    _sdc_source_key_campaign_id text NOT NULL,
    _sdc_source_key_date_start timestamp with time zone NOT NULL,
    _sdc_table_version bigint,
    action_destination text,
    action_target_id text,
    action_type text,
    value double precision
);


--
-- Name: campaigns; Type: TABLE; Schema: fb_to_mode; Owner: -
--

CREATE TABLE fb_to_mode.campaigns (
    _sdc_batched_at timestamp with time zone,
    _sdc_extracted_at timestamp with time zone,
    _sdc_received_at timestamp with time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    id text NOT NULL,
    updated_time timestamp with time zone
);


--
-- Name: test12; Type: TABLE; Schema: jim; Owner: -
--

CREATE TABLE jim.test12 (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    event jsonb,
    uuid text
);


--
-- Name: test2; Type: TABLE; Schema: oli; Owner: -
--

CREATE TABLE oli.test2 (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    event jsonb,
    uuid text
);


--
-- Name: bot_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bot_filters (
    tab_id text,
    ab_test_groups jsonb,
    user_agent text,
    has_bot_keyword boolean,
    num_ssrs bigint,
    num_page_loads_completed bigint,
    percent_completed numeric,
    filtered boolean
);


--
-- Name: comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.comments (
    _id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    username character varying(64),
    display_name text,
    post_id character varying(64),
    posted_at timestamp without time zone NOT NULL,
    af boolean NOT NULL,
    base_score numeric,
    score numeric,
    answer boolean,
    parent_answer_id character varying(64),
    parent_comment_id character varying(64),
    word_count numeric,
    top_level boolean,
    gw boolean,
    num_votes numeric,
    percent_downvotes numeric,
    percent_bigvotes numeric,
    small_upvote numeric,
    big_upvote numeric,
    small_downvote numeric,
    big_downvote numeric,
    user_agent text,
    deleted boolean,
    created_at timestamp without time zone,
    birth timestamp without time zone
);


--
-- Name: raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw (
    environment character varying(64) NOT NULL,
    event_type character varying(256) NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    event jsonb NOT NULL
);


--
-- Name: corona_link_db_events; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.corona_link_db_events AS
 SELECT r.environment,
    r.event_type,
    r."timestamp",
    (r.event ->> 'tabId'::text) AS tab_id,
    (r.event ->> 'clientId'::text) AS client_id,
    (r.event ->> 'userId'::text) AS user_id,
    (r.event ->> 'path'::text) AS path,
    (r.event ->> 'pageContext'::text) AS page_context,
    (r.event ->> 'pageSectionContext'::text) AS page_section_context,
    replace(replace(COALESCE((r.event ->> 'url'::text), (r.event ->> 'to'::text)), 'https://www.lesswrong.com'::text, ''::text), 'https://www.alignmentforum.org'::text, ''::text) AS url_to,
    replace(replace(COALESCE((r.event ->> 'referrer'::text), (r.event ->> 'from'::text)), 'https://www.lesswrong.com'::text, ''::text), 'https://www.alignmentforum.org'::text, ''::text) AS referrer_from,
    r.event
   FROM public.raw r
  WHERE (((r.environment)::text = 'lesswrong.com'::text) AND (r."timestamp" >= '2020-03-20 00:00:00'::timestamp without time zone) AND ((r.event_type)::text = ANY ((ARRAY['linkClicked'::character varying, 'navigate'::character varying, 'pageLoadFinished'::character varying])::text[])) AND (((r.event ->> 'path'::text) ~~ '/coronavirus-link-database%'::text) OR (replace(COALESCE((r.event ->> 'url'::text), (r.event ->> 'to'::text)), 'https://www.lesswrong.com'::text, ''::text) ~~ '/coronavirus-link-database%'::text)));


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
  WHERE ((raw.event_type)::text = 'navigate'::text);


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
  WHERE ((raw.event_type)::text = 'pageLoadFinished'::text);


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
  WHERE ((raw.event_type)::text = 'timerEvent'::text);


--
-- Name: ga_devices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ga_devices (
    date timestamp without time zone,
    ga_device_category character varying(32),
    ga_users integer,
    ga_sessions integer,
    ga_pageviews integer,
    ga_unique_pageviews integer,
    birth timestamp without time zone
);


--
-- Name: ga_pages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ga_pages (
    date timestamp without time zone,
    page_agg text,
    ga_page_path text,
    ga_users integer,
    ga_sessions integer,
    ga_pageviews integer,
    ga_unique_pageviews integer,
    ga_avg_time_on_page numeric,
    ga_avg_page_load_time numeric,
    birth timestamp without time zone
);


--
-- Name: ga_referrer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ga_referrer (
    date timestamp without time zone,
    referrer_agg text,
    ga_full_referrer text,
    ga_users integer,
    ga_sessions integer,
    birth timestamp without time zone
);


--
-- Name: ga_source; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ga_source (
    date timestamp without time zone,
    source_agg text,
    ga_source text,
    ga_users integer,
    ga_sessions integer,
    birth timestamp without time zone
);


--
-- Name: ga_traffic; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ga_traffic (
    date timestamp without time zone,
    ga_users integer,
    ga_sessions integer,
    ga_pageviews integer,
    ga_unique_pageviews integer,
    ga_pageviews_per_session numeric,
    birth timestamp without time zone
);


--
-- Name: gather_town_checks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gather_town_checks (
    "timestamp" timestamp without time zone,
    player_id text,
    name character varying(64),
    busy integer,
    audio boolean,
    video boolean,
    blocked text,
    elapsed_min numeric,
    first_visit boolean,
    new_session boolean,
    session_no integer,
    lw_team boolean,
    birth timestamp without time zone
);


--
-- Name: gather_town_presence; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.gather_town_presence AS
 SELECT gather_town_checks."timestamp",
    count(
        CASE
            WHEN (gather_town_checks.lw_team IS TRUE) THEN 1
            ELSE NULL::integer
        END) AS num_lw_team_present,
    count(
        CASE
            WHEN (gather_town_checks.lw_team IS NOT TRUE) THEN 1
            ELSE NULL::integer
        END) AS num_other_present,
    count(*) AS total_present
   FROM public.gather_town_checks
  GROUP BY gather_town_checks."timestamp"
  ORDER BY gather_town_checks."timestamp" DESC;


--
-- Name: gather_town_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gather_town_sessions (
    player_id text,
    name character varying(64),
    session_no integer,
    num_checks integer,
    max_gap numeric,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    first_visit boolean,
    lw_team boolean,
    approx_duration numeric,
    alone_at_start boolean,
    alone_at_end boolean,
    percent_accompanied numeric,
    concurrent_visitors text,
    birth timestamp without time zone
);


--
-- Name: gather_town_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gather_town_users (
    player_id text,
    name character varying(64),
    num_sessions integer,
    num_checks integer,
    num_distinct_days integer,
    first_seen timestamp without time zone,
    last_seen timestamp without time zone,
    total_approx_duration numeric,
    mean_session_length numeric,
    median_session_length numeric,
    max_session_length numeric,
    min_session_length numeric,
    lw_team boolean,
    birth timestamp without time zone
);


--
-- Name: lessraw_medium; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lessraw_medium (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    event jsonb,
    uuid text
);


--
-- Name: lessraw_small; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lessraw_small (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    event jsonb,
    uuid text
);


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
-- Name: performance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.performance (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    client_id text,
    user_id text,
    tab_id text,
    navigation_start text,
    response_start text,
    load_event_end text,
    request_start_to_load_end numeric,
    time_to_first_byte numeric
);


--
-- Name: performance_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.performance_view AS
 SELECT timing.environment,
    timing."timestamp",
    timing.client_id,
    timing.user_id,
    timing.tab_id,
    timing.navigation_start,
    timing.response_start,
    timing.load_event_end,
    ((timing.load_event_end)::numeric - (timing.navigation_start)::numeric) AS nav_start_to_load_end,
    ((timing.response_start)::numeric - (timing.navigation_start)::numeric) AS time_to_first_byte
   FROM ( SELECT lessraw_small.environment,
            lessraw_small.event_type,
            lessraw_small."timestamp",
            (lessraw_small.event ->> 'clientId'::text) AS client_id,
            (lessraw_small.event ->> 'userId'::text) AS user_id,
            (lessraw_small.event ->> 'tabId'::text) AS tab_id,
            (lessraw_small.event #>> '{performance,timing,navigationStart}'::text[]) AS navigation_start,
            (lessraw_small.event #>> '{performance,timing,responseStart}'::text[]) AS response_start,
            (lessraw_small.event #>> '{performance,timing,loadEventEnd}'::text[]) AS load_event_end
           FROM public.lessraw_small
          WHERE (((lessraw_small.event_type)::text = 'pageLoadFinished'::text) AND ((lessraw_small.environment)::text = ANY ((ARRAY['lesswrong.com'::character varying, 'alignmentforum.com'::character varying])::text[])))) timing;


--
-- Name: postgres_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.postgres_log (
    log_time timestamp(3) with time zone,
    user_name text,
    database_name text,
    process_id integer,
    connection_from text,
    session_id text NOT NULL,
    session_line_num bigint NOT NULL,
    command_tag text,
    session_start_time timestamp with time zone,
    virtual_transaction_id text,
    transaction_id bigint,
    error_severity text,
    sql_state_code text,
    message text,
    detail text,
    hint text,
    internal_query text,
    internal_query_pos integer,
    context text,
    query text,
    query_pos integer,
    location text,
    application_name text
);


--
-- Name: posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.posts (
    _id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    posted_at timestamp without time zone NOT NULL,
    username text,
    display_name text,
    title text,
    af boolean NOT NULL,
    base_score numeric NOT NULL,
    af_base_score numeric,
    score numeric,
    draft boolean,
    question boolean,
    is_event boolean,
    view_count numeric,
    view_count_logged numeric,
    click_count numeric,
    comment_count numeric,
    num_comments_rederived numeric,
    num_distinct_viewers numeric,
    num_distinct_commenters numeric,
    word_count numeric,
    num_votes numeric,
    small_upvote numeric,
    big_upvote numeric,
    small_downvote numeric,
    big_downvote numeric,
    percent_downvotes numeric,
    percent_bigvotes numeric,
    url text,
    slug text,
    canonical_collection_slug text,
    website text,
    gw boolean NOT NULL,
    frontpaged boolean NOT NULL,
    frontpage_date timestamp without time zone,
    curated_date timestamp without time zone,
    status numeric,
    legacy_spam boolean NOT NULL,
    author_is_unreviewed boolean NOT NULL,
    most_recent_comment timestamp without time zone,
    user_agent text,
    created_at timestamp without time zone NOT NULL,
    birth timestamp without time zone
);


--
-- Name: review_votes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.review_votes (
    _id text,
    postid text,
    qualitativescore integer,
    quadraticscore integer,
    userid text,
    createdat text,
    schemaversion integer,
    comment text
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(255) NOT NULL
);


--
-- Name: sequences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sequences (
    _id character varying(64) NOT NULL,
    user_id character varying(64),
    title text,
    created_at timestamp without time zone,
    draft boolean,
    is_deleted boolean,
    schema_version smallint,
    plaintext_description text,
    birth timestamp without time zone,
    hidden boolean
);


--
-- Name: ssr; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ssr (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    page_load_tab_id text,
    page_load_completed boolean,
    server_id text,
    cached text,
    ab_test_groups text,
    user_agent text,
    ua_hash text
);


--
-- Name: ssr_with_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ssr_with_stats (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    page_load_completed boolean,
    user_agent text,
    ab_test_groups jsonb
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    _id character varying(64) NOT NULL,
    username text,
    display_name text,
    created_at timestamp without time zone,
    post_count smallint,
    comment_count smallint,
    karma integer,
    af_karma integer,
    legacy_karma integer,
    deleted boolean,
    banned boolean,
    legacy boolean,
    shortform_feed_id character varying(256),
    sign_up_re_captcha_rating real,
    reviewed_by_user_id character varying(64),
    earliest_activity timestamp without time zone,
    true_earliest timestamp without time zone,
    most_recent_activity timestamp without time zone,
    days_since_active real,
    total_posts numeric,
    earliest_post timestamp without time zone,
    most_recent_post timestamp without time zone,
    total_comments numeric,
    earliest_comment timestamp without time zone,
    most_recent_comment timestamp without time zone,
    num_votes numeric,
    most_recent_vote timestamp without time zone,
    earliest_vote timestamp without time zone,
    percent_downvotes real,
    percent_bigvotes real,
    most_recent_view timestamp without time zone,
    earliest_view timestamp without time zone,
    num_distinct_posts_viewed numeric,
    num_days_present_last_30_days numeric,
    num_posts_last_30_days numeric,
    num_comments_last_30_days numeric,
    num_votes_last_30_days numeric,
    num_views_last_30_days numeric,
    num_distinct_posts_viewed_last_30_days numeric,
    num_posts_last_180_days numeric,
    num_comments_last_180_days numeric,
    num_votes_last_180_days numeric,
    num_views_last_180_days numeric,
    num_distinct_posts_viewed_last_180_days numeric,
    walled_garden_invite boolean,
    hide_walled_garden_ui boolean,
    bio text,
    email text,
    birth timestamp without time zone
);


--
-- Name: tag_filter_actions; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.tag_filter_actions AS
 SELECT lrs."timestamp",
    lrs.event_type,
    lrs.tab_id,
    lrs.client_id,
    lrs.user_id,
    COALESCE(lrs.user_id, lrs.client_id) AS user_identifier,
    COALESCE((lrs.event ->> 'tagName'::text), lrs.url_to) AS tag_name,
    (lrs.event ->> 'pageElementContext'::text) AS page_element_context,
    (lrs.event ->> 'buttonPressed'::text) AS button_pressed,
    lrs.path,
    lrs.url_to,
    lrs.page_context,
    lrs.page_section_context,
    lrs.event,
    lrs.uuid
   FROM public.lessraw_small lrs
  WHERE ((lrs.page_section_context = 'tagFilterSettings'::text) AND (lrs."timestamp" >= '2020-05-17 00:00:00'::timestamp without time zone) AND ((NOT (lrs.user_id IN ( SELECT users._id
           FROM public.users
          WHERE (users.display_name = ANY (ARRAY['Habryka'::text, 'Ben Pace'::text, 'Raemon'::text, 'jimrandomh'::text, 'Ruby'::text, 'jacobjacob'::text]))))) OR (lrs.user_id IS NULL)));


--
-- Name: tag_footer_clicks; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.tag_footer_clicks AS
 SELECT lrs."timestamp",
    lrs.event_type,
    lrs.tab_id,
    lrs.client_id,
    lrs.user_id,
    COALESCE(lrs.user_id, lrs.client_id) AS user_identifier,
    COALESCE((lrs.event ->> 'tagName'::text), lrs.url_to) AS tag_name,
    (lrs.event ->> 'pageElementContext'::text) AS page_element_context,
    (lrs.event ->> 'buttonPressed'::text) AS button_pressed,
    lrs.path,
    lrs.url_to,
    lrs.page_context,
    lrs.page_section_context,
    lrs.event,
    lrs.uuid
   FROM public.lessraw_small lrs
  WHERE ((lrs.page_section_context = ANY (ARRAY['tagFooter'::text, 'tagHeader'::text])) AND ((lrs.event_type)::text = 'linkClicked'::text) AND (lrs."timestamp" >= '2020-05-17 00:00:00'::timestamp without time zone) AND ((NOT (lrs.user_id IN ( SELECT users._id
           FROM public.users
          WHERE (users.display_name = ANY (ARRAY['Habryka'::text, 'Ben Pace'::text, 'Raemon'::text, 'jimrandomh'::text, 'Ruby'::text, 'jacobjacob'::text]))))) OR (lrs.user_id IS NULL)));


--
-- Name: tagrels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tagrels (
    created_at timestamp without time zone,
    _id character varying(64) NOT NULL,
    tag_id character varying(64),
    post_id character varying(64),
    user_id character varying(64),
    base_score smallint,
    score smallint,
    inactive boolean,
    vote_count smallint,
    af_base_score smallint,
    deleted boolean,
    name text,
    title text,
    user_id_post character varying(64),
    author text,
    base_score_post smallint,
    birth timestamp without time zone
);


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    created_at timestamp without time zone,
    _id character varying(64) NOT NULL,
    name text,
    slug text,
    deleted boolean,
    post_count smallint,
    admin_only boolean,
    core boolean,
    suggested_as_filter boolean,
    default_order numeric,
    promoted boolean,
    birth timestamp without time zone
);


--
-- Name: urls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.urls (
    url text,
    type character varying(32),
    title text,
    author text,
    document_id character varying(64),
    url_hash character varying(32) NOT NULL,
    birth timestamp without time zone
);


--
-- Name: user_agents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_agents (
    ua_hash character varying(32),
    ua_string text,
    ua_pretty text,
    browser_family character varying(128),
    browser_version character varying(32),
    os_family character varying(32),
    os_version character varying(32),
    device_family text,
    device_brand character varying(128),
    device_model character varying(128),
    is_mobile boolean,
    is_tablet boolean,
    is_mobile_or_tablet boolean,
    is_desktop boolean,
    is_bot boolean,
    birth timestamp without time zone
);


--
-- Name: views; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.views (
    user_id character varying(64) NOT NULL,
    document_id character varying(64),
    created_at timestamp without time zone NOT NULL,
    birth timestamp without time zone
);


--
-- Name: votes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.votes (
    document_id character varying(64) NOT NULL,
    collection_name character varying(10) NOT NULL,
    user_id character varying(64) NOT NULL,
    vote_type character varying(16) NOT NULL,
    power smallint NOT NULL,
    voted_at timestamp without time zone NOT NULL,
    cancelled boolean NOT NULL,
    is_unvote boolean NOT NULL,
    af_power smallint NOT NULL,
    legacy boolean NOT NULL,
    birth timestamp without time zone
);


--
-- Name: comments; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.comments (
    _id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    username character varying(64),
    display_name text,
    post_id character varying(64),
    posted_at timestamp without time zone NOT NULL,
    af boolean NOT NULL,
    base_score numeric,
    score numeric,
    answer boolean,
    parent_answer_id character varying(64),
    parent_comment_id character varying(64),
    word_count numeric,
    top_level boolean,
    gw boolean,
    num_votes numeric,
    percent_downvotes numeric,
    percent_bigvotes numeric,
    small_upvote numeric,
    big_upvote numeric,
    small_downvote numeric,
    big_downvote numeric,
    user_agent text,
    deleted boolean,
    created_at timestamp without time zone,
    birth timestamp without time zone
);


--
-- Name: lessraw_with_inviews; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.lessraw_with_inviews (
    environment character varying(64),
    "timestamp" timestamp without time zone,
    event_type character varying(256),
    tab_id text,
    client_id text,
    user_id text,
    path text,
    page_context text,
    page_section_context text,
    url_to text,
    referrer_from text,
    lw_team_member boolean,
    event jsonb,
    uuid text
);


--
-- Name: myposts; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.myposts (
    _id character varying(64),
    user_id character varying(64),
    posted_at timestamp without time zone,
    username text,
    display_name text,
    title text,
    af boolean,
    base_score numeric,
    af_base_score numeric,
    score numeric,
    draft boolean,
    question boolean,
    is_event boolean,
    view_count numeric,
    view_count_logged numeric,
    click_count numeric,
    comment_count numeric,
    num_comments_rederived numeric,
    num_distinct_viewers numeric,
    num_distinct_commenters numeric,
    word_count numeric,
    num_votes numeric,
    small_upvote numeric,
    big_upvote numeric,
    small_downvote numeric,
    big_downvote numeric,
    percent_downvotes numeric,
    percent_bigvotes numeric,
    url text,
    slug text,
    canonical_collection_slug text,
    website text,
    gw boolean,
    frontpaged boolean,
    frontpage_date timestamp without time zone,
    curated_date timestamp without time zone,
    status numeric,
    legacy_spam boolean,
    author_is_unreviewed boolean,
    most_recent_comment timestamp without time zone,
    user_agent text,
    created_at timestamp without time zone,
    birth timestamp without time zone
);


--
-- Name: posts; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.posts (
    _id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    posted_at timestamp without time zone NOT NULL,
    username text,
    display_name text,
    title text,
    af boolean NOT NULL,
    base_score numeric NOT NULL,
    af_base_score numeric,
    score numeric,
    draft boolean,
    question boolean,
    is_event boolean,
    view_count numeric,
    view_count_logged numeric,
    click_count numeric,
    comment_count numeric,
    num_comments_rederived numeric,
    num_distinct_viewers numeric,
    num_distinct_commenters numeric,
    word_count numeric,
    num_votes numeric,
    small_upvote numeric,
    big_upvote numeric,
    small_downvote numeric,
    big_downvote numeric,
    percent_downvotes numeric,
    percent_bigvotes numeric,
    url text,
    slug text,
    canonical_collection_slug text,
    website text,
    gw boolean NOT NULL,
    frontpaged boolean NOT NULL,
    frontpage_date timestamp without time zone,
    curated_date timestamp without time zone,
    status numeric,
    legacy_spam boolean NOT NULL,
    author_is_unreviewed boolean NOT NULL,
    most_recent_comment timestamp without time zone,
    user_agent text,
    created_at timestamp without time zone NOT NULL,
    birth timestamp without time zone,
    other_fields jsonb
);


--
-- Name: tags; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.tags (
    created_at timestamp without time zone,
    _id character varying(64) NOT NULL,
    name text,
    slug text,
    deleted boolean,
    post_count smallint,
    admin_only boolean,
    core boolean,
    suggested_as_filter boolean,
    default_order numeric,
    promoted boolean,
    birth timestamp without time zone
);


--
-- Name: temp_ga_pages; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.temp_ga_pages (
    ga_users integer,
    ga_sessions integer,
    ga_pageviews integer,
    ga_unique_pageviews integer,
    ga_avg_time_on_page numeric,
    ga_avg_page_load_time numeric,
    ga_page_path text,
    date text,
    url text,
    type text,
    title text,
    documentid text,
    author text,
    onsite text
);


--
-- Name: views; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.views (
    user_id character varying(64) NOT NULL,
    document_id character varying(64),
    created_at timestamp without time zone NOT NULL,
    birth timestamp without time zone
);


--
-- Name: votes; Type: TABLE; Schema: ruby; Owner: -
--

CREATE TABLE ruby.votes (
    document_id character varying(64) NOT NULL,
    collection_name character varying(10) NOT NULL,
    user_id character varying(64) NOT NULL,
    vote_type character varying(16) NOT NULL,
    power smallint NOT NULL,
    voted_at timestamp without time zone NOT NULL,
    cancelled boolean NOT NULL,
    is_unvote boolean NOT NULL,
    af_power smallint NOT NULL,
    legacy boolean NOT NULL,
    birth timestamp without time zone
);


--
-- Name: ads_insights__actions_pkey; Type: CONSTRAINT; Schema: fb_to_mode; Owner: -
--

ALTER TABLE ONLY fb_to_mode.ads_insights__actions
    ADD CONSTRAINT ads_insights__actions_pkey PRIMARY KEY (_sdc_level_0_id, _sdc_source_key_ad_id, _sdc_source_key_adset_id, _sdc_source_key_campaign_id, _sdc_source_key_date_start);


--
-- Name: ads_insights_pkey; Type: CONSTRAINT; Schema: fb_to_mode; Owner: -
--

ALTER TABLE ONLY fb_to_mode.ads_insights
    ADD CONSTRAINT ads_insights_pkey PRIMARY KEY (ad_id, adset_id, campaign_id, date_start);


--
-- Name: ads_pkey; Type: CONSTRAINT; Schema: fb_to_mode; Owner: -
--

ALTER TABLE ONLY fb_to_mode.ads
    ADD CONSTRAINT ads_pkey PRIMARY KEY (id, updated_time);


--
-- Name: campaigns_pkey; Type: CONSTRAINT; Schema: fb_to_mode; Owner: -
--

ALTER TABLE ONLY fb_to_mode.campaigns
    ADD CONSTRAINT campaigns_pkey PRIMARY KEY (id);


--
-- Name: comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.comments
    ADD CONSTRAINT comments_pkey PRIMARY KEY (_id);


--
-- Name: postgres_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.postgres_log
    ADD CONSTRAINT postgres_log_pkey PRIMARY KEY (session_id, session_line_num);


--
-- Name: posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_pkey PRIMARY KEY (_id);


--
-- Name: schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sequences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sequences
    ADD CONSTRAINT sequences_pkey PRIMARY KEY (_id);


--
-- Name: tagrels_pkey1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tagrels
    ADD CONSTRAINT tagrels_pkey1 PRIMARY KEY (_id);


--
-- Name: tags_pkey1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey1 PRIMARY KEY (_id);


--
-- Name: urls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT urls_pkey PRIMARY KEY (url_hash);


--
-- Name: users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (_id);


--
-- Name: comments_pkey; Type: CONSTRAINT; Schema: ruby; Owner: -
--

ALTER TABLE ONLY ruby.comments
    ADD CONSTRAINT comments_pkey PRIMARY KEY (_id);


--
-- Name: posts_pkey; Type: CONSTRAINT; Schema: ruby; Owner: -
--

ALTER TABLE ONLY ruby.posts
    ADD CONSTRAINT posts_pkey PRIMARY KEY (_id);


--
-- Name: tags_pkey; Type: CONSTRAINT; Schema: ruby; Owner: -
--

ALTER TABLE ONLY ruby.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (_id);


--
-- Name: bot_filters__index_filtered; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX bot_filters__index_filtered ON public.bot_filters USING btree (filtered);


--
-- Name: bot_filters__index_tab_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX bot_filters__index_tab_id ON public.bot_filters USING btree (tab_id);


--
-- Name: lessraw_medium__index_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_medium__index_event_type ON public.lessraw_medium USING btree (event_type);


--
-- Name: lessraw_medium_index_enviroment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_medium_index_enviroment ON public.lessraw_medium USING btree (environment);


--
-- Name: lessraw_medium_index_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_medium_index_timestamp ON public.lessraw_medium USING btree ("timestamp");


--
-- Name: lessraw_small__index_tab_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_small__index_tab_id ON public.lessraw_small USING btree (tab_id);


--
-- Name: lessraw_small_index_enviroment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_small_index_enviroment ON public.lessraw_small USING btree (environment);


--
-- Name: lessraw_small_index_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_small_index_event_type ON public.lessraw_small USING btree (event_type);


--
-- Name: lessraw_small_index_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX lessraw_small_index_timestamp ON public.lessraw_small USING btree ("timestamp");


--
-- Name: raw__index_ab_test_groups; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw__index_ab_test_groups ON public.raw USING btree (((event ->> 'abTestGroups'::text)));


--
-- Name: raw__index_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw__index_event_type ON public.raw USING btree (event_type);


--
-- Name: raw__index_tab_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw__index_tab_id ON public.raw USING btree (((event ->> 'tabId'::text)));


--
-- Name: raw_event_type_post_id_navigate; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_navigate ON public.raw USING btree (public.get_post_id_from_path((event ->> 'to'::text))) WHERE ((event_type)::text = 'navigate'::text);


--
-- Name: raw_event_type_post_id_page_load_finished; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_page_load_finished ON public.raw USING btree (public.get_post_id_from_path((event ->> 'url'::text))) WHERE ((event_type)::text = 'pageLoadFinished'::text);


--
-- Name: raw_event_type_post_id_timer_event; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_post_id_timer_event ON public.raw USING btree (public.get_post_id_from_path((event ->> 'path'::text))) WHERE ((event_type)::text = 'timerEvent'::text);


--
-- Name: raw_event_type_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_event_type_timestamp ON public.raw USING btree (event_type, "timestamp");


--
-- Name: raw_index_enviroment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_index_enviroment ON public.raw USING btree (environment);


--
-- Name: raw_index_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_index_timestamp ON public.raw USING btree ("timestamp");


--
-- Name: raw_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_timestamp ON public.raw USING btree ("timestamp");


--
-- Name: user_agents__index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX user_agents__index ON public.user_agents USING btree (ua_hash);


--
-- Name: lessraw_with_inviews__index_event_type; Type: INDEX; Schema: ruby; Owner: -
--

CREATE INDEX lessraw_with_inviews__index_event_type ON ruby.lessraw_with_inviews USING btree (event_type);


--
-- Name: lessraw_with_inviews_index_enviroment; Type: INDEX; Schema: ruby; Owner: -
--

CREATE INDEX lessraw_with_inviews_index_enviroment ON ruby.lessraw_with_inviews USING btree (environment);


--
-- Name: lessraw_with_inviews_index_timestamp; Type: INDEX; Schema: ruby; Owner: -
--

CREATE INDEX lessraw_with_inviews_index_timestamp ON ruby.lessraw_with_inviews USING btree ("timestamp");


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
