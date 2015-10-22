-module(engine_tests_02_get_set_props).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").


get_set_props_test_() ->
    test_engine_util:gather(?MODULE).


cet_default_props() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St} = Engine:init(DbPath, [create]),

    Node = node(),

    ?assertEqual(0, Engine:get(St, doc_count)),
    ?assertEqual(0, Engine:get(St, del_doc_count)),
    ?assertEqual(true, is_list(Engine:get(St, size_info))),
    ?assertEqual(true, is_integer(Engine:get(St, disk_version))),
    ?assertEqual(0, Engine:get(St, update_seq)),
    ?assertEqual(0, Engine:get(St, purge_seq)),
    ?assertEqual([], Engine:get(St, last_purged)),
    ?assertEqual([], Engine:get(St, security)),
    ?assertEqual(1000, Engine:get(St, revs_limit)),
    ?assertMatch(<<_:32/binary>>, Engine:get(St, uuid)),
    ?assertEqual([{Node, 0}], Engine:get(St, epochs)),
    ?assertEqual(0, Engine:get(St, compacted_seq)).


cet_set_security() ->
    check_prop_set(security, [], [{<<"readers">>, []}]).


cet_set_revs_limit() ->
    check_prop_set(revs_limit, 1000, 50).


cet_set_epochs() ->
    TestValue = [
        {'other_node@127.0.0.1', 0},
        {node(), 0}
    ],
    % This looks weird. I need to move the epochs
    % logic to a central place so this isn't the case here.
    % The weirdness is because epochs are modified when
    % we use couch_bt_engine_header.
    CommittedValue = [
        {node(), 1},
        {'other_node@127.0.0.1', 0}
    ],
    check_prop_set(epochs, [{node(), 0}], TestValue, CommittedValue).


cet_set_compact_seq() ->
    check_prop_set(compacted_seq, 0, 12).


check_prop_set(Name, Default, Value) ->
    check_prop_set(Name, Default, Value, Value).


check_prop_set(Name, Default, Value, CommittedValue) ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St0} = Engine:init(DbPath, [create]),
    ?assertEqual(Default, Engine:get(St0, Name)),

    {ok, St1} = Engine:set(St0, Name, Value),
    ?assertEqual(Value, Engine:get(St1, Name)),

    Engine:terminate(normal, St1),

    {ok, St2} = Engine:init(DbPath, []),
    ?assertEqual(Default, Engine:get(St2, Name)),

    {ok, St3} = Engine:set(St2, Name, Value),
    ?assertEqual(Value, Engine:get(St3, Name)),

    {ok, St4} = Engine:commit_data(St3),
    Engine:terminate(normal, St4),

    {ok, St5} = Engine:init(DbPath, []),
    ?assertEqual(CommittedValue, Engine:get(St5, Name)).

    