BEGIN;

TRUNCATE TABLE raw;

CREATE TABLE get_post_id_from_path_test (
  path text,
  expected_post_id TEXT,
  description TEXT
);

INSERT INTO get_post_id_from_path_test VALUES (
  '/',
  NULL,
  'Nothing there'
), (
  '/posts/7irLXuSJGW7CjfunB/new-start-here',
  '7irLXuSJGW7CjfunB',
  'Simple post'
), (
  '/s/6gFGprxo27o7desCs/p/yBHqxKYS5rZot3gdq',
  'yBHqxKYS5rZot3gdq',
  'Post viewed in sequence view'
);

CREATE FUNCTION test_get_post_id_from_path ()
RETURNS SETOF TEXT AS $$
DECLARE
  test get_post_id_from_path_test;
BEGIN
  FOR test IN (SELECT * FROM get_post_id_from_path_test) LOOP
    RETURN NEXT is(
      get_post_id_from_path(test.path),
      test.expected_post_id,
      test.description
    );
  END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT plan(3);

SELECT test_get_post_id_from_path();

SELECT finish();

ROLLBACK;
