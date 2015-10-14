-module(test_engine_util).
-compile(export_all).


get_engine() ->
    {ok, {_, Engine}} = application:get_env(couch, test_engine),
    Engine.


rootdir() ->
    config:get("couchdb", "database_dir", ".").


dbpath() ->
    binary_to_list(filename:join(rootdir(), couch_uuids:random())).