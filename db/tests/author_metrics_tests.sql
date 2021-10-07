BEGIN;

SELECT plan(7);

SELECT is(
  get_post_id_from_path('/'),
  NULL,
  'returns null if no post id in string'
);

SELECT is(
  get_post_id_from_path('/posts/7irLXuSJGW7CjfunB/new-start-here'),
  '7irLXuSJGW7CjfunB',
  'returns post id from relative path'
);

SELECT is(
  get_post_id_from_path('/s/6gFGprxo27o7desCs/p/yBHqxKYS5rZot3gdq'),
  'yBHqxKYS5rZot3gdq',
  'returns post id from relative path in sequence view'
);

SELECT is(
  get_post_id_from_path('http://localhost:3000/posts/7irLXuSJGW7CjfunB/new-start-here'),
  '7irLXuSJGW7CjfunB',
  'returns post id from absolute path (http)'
);

SELECT is(
  get_post_id_from_path('http://localhost:3000/s/6gFGprxo27o7desCs/p/yBHqxKYS5rZot3gdq'),
  'yBHqxKYS5rZot3gdq',
  'returns post id from absolute path in sequence view (http)'
);

SELECT is(
  get_post_id_from_path('https://forum.effectivealtruism.org/posts/7irLXuSJGW7CjfunB/new-start-here'),
  '7irLXuSJGW7CjfunB',
  'returns post id from absolute path (https)'
);

SELECT is(
  get_post_id_from_path('https://forum.effectivealtruism.org/s/6gFGprxo27o7desCs/p/yBHqxKYS5rZot3gdq'),
  'yBHqxKYS5rZot3gdq',
  'returns post id from absolute path in sequence view (https)'
);

SELECT finish();

ROLLBACK;
