-module(engine_tests_07_purge_docs).
-compile(export_all).


-include("eunit/include/eunit.hrl").
-include("couch/include/couch_db.hrl").


purge_docs_test_() ->
    test_engine_util:gather(?MODULE).


cet_purge_simple() ->
    ok.