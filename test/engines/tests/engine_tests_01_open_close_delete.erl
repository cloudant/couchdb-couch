-module(engine_tests_01_open_close_delete).


-include_lib("eunit/include/eunit.hrl").


open_non_existent_test() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertThrow({not_found, no_db_file}, Engine:init(DbPath, [])),
    ?assertEqual(false, Engine:exists(DbPath)).


open_create_test() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertMatch({ok, _}, Engine:init(DbPath, [create])),
    ?assertEqual(true, Engine:exists(DbPath)).


open_when_exists_test() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    ?assertMatch({ok, _}, Engine:init(DbPath, [create])),
    ?assertThrow({error, eexist}, Engine:init(DbPath, [create])).


terminate_test() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    {ok, St} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St),
    ?assertEqual(true, Engine:exists(DbPath)).


rapid_recycle_test() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St0} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St0),

    lists:foreach(fun(_) ->
        {ok, St1} = Engine:init(DbPath, []),
        Engine:terminate(normal, St1)
    end, lists:seq(1, 100)).


delete_test() ->
    Engine = test_engine_util:get_engine(),
    RootDir = test_engine_util:rootdir(),
    DbPath = test_engine_util:dbpath(),

    ?assertEqual(false, Engine:exists(DbPath)),
    {ok, St} = Engine:init(DbPath, [create]),
    Engine:terminate(normal, St),
    ?assertEqual(true, Engine:exists(DbPath)),
    ?assertEqual(ok, Engine:delete(RootDir, DbPath, true)),
    ?assertEqual(false, Engine:exists(DbPath)).
