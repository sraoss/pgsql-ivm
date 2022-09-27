test_decoding_sources = files(
  'test_decoding.c',
)

test_decoding = shared_module('test_decoding',
  test_decoding_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += test_decoding

tests += {
  'name': 'test_decoding',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'ddl',
      'xact',
      'rewrite',
      'toast',
      'permissions',
      'decoding_in_xact',
      'decoding_into_rel',
      'binary',
      'prepared',
      'replorigin',
      'time',
      'messages',
      'spill',
      'slot',
      'truncate',
      'stream',
      'stats',
      'twophase',
      'twophase_stream',
    ],
    'regress_args': [
      '--temp-config', files('logical.conf'),
    ],
  },
  'isolation': {
    'specs': [
      'mxact',
      'delayed_startup',
      'ondisk_startup',
      'concurrent_ddl_dml',
      'oldest_xmin',
      'snapshot_transfer',
      'subxact_without_top',
      'concurrent_stream',
      'twophase_snapshot',
      'slot_creation_error',
    ],
    'regress_args': [
      '--temp-config', files('logical.conf'),
    ],
  },
  'tap': {
    'tests': [
      't/001_repl_stats.pl',
    ],
  },
}