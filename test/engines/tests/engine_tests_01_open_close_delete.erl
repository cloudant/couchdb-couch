-module(engine_tests_01_open_close_delete).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").


open_close_delete_test_() ->
    test_engine_util:gather(?MODULE).


cet_open_non_existent() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertThrow({not_found, no_db_file}, Engine:init(DbPath, [])),
    ?assertEqual(false, Engine:exists(DbPath)).


cet_open_create() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertMatch({ok, _}, Engine:init(DbPath, [create])),
    ?assertEqual(true, Engine:exists(DbPath)).


cet_open_when_exists() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertMatch({ok, _}, Engine:init(DbPath, [create])),
    ?assertThrow({error, eexist}, Engine:init(DbPath, [create])).


cet_terminate() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    {ok, St} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St),
    ?assertEqual(true, Engine:exists(DbPath)).


cet_rapid_recycle() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St0} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St0),

    lists:foreach(fun(_) ->
        {ok, St1} = Engine:init(DbPath, []),
        Engine:terminate(normal, St1)
    end, lists:seq(1, 100)).


cet_delete() ->
    Engine = test_engine_util:get_engine(),
    RootDir = test_engine_util:rootdir(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    {ok, St} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St),
    ?assertEqual(true, Engine:exists(DbPath)),
    ?assertEqual(ok, Engine:delete(RootDir, DbPath, true)),
    ?assertEqual(false, Engine:exists(DbPath)).
