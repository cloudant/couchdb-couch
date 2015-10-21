-module(engine_tests_08_compaction).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


compaction_test_() ->
    test_engine_util:gather(?MODULE).


cet_compact_empty() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),
    Db1 = test_engine_util:db_as_term(Engine, St1),
    {ok, St2, DbName, _, Term} = test_engine_util:compact(Engine, St1, Path),
    {ok, St3, undefined} = Engine:finish_compaction(St2, DbName, [], Term),
    Db2 = test_engine_util:db_as_term(Engine, St3),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_compact_doc() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),
    Actions = [{create, {<<"foo">>, []}}],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    Db1 = test_engine_util:db_as_term(Engine, St2),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),
    {ok, St4, undefined} = Engine:finish_compaction(St3, DbName, [], Term),
    Db2 = test_engine_util:db_as_term(Engine, St4),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_compact_local_doc() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),
    Actions = [{create, {<<"_local/foo">>, []}}],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    Db1 = test_engine_util:db_as_term(Engine, St2),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),
    {ok, St4, undefined} = Engine:finish_compaction(St3, DbName, [], Term),
    Db2 = test_engine_util:db_as_term(Engine, St4),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_compact_with_everything() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),

    % Add a whole bunch of docs
    DocActions = lists:map(fun(Seq) ->
        {create, {docid(Seq), [{<<"int">>, Seq}]}}
    end, lists:seq(1, 1000)),
        
    LocalActions = lists:map(fun(I) ->
        {create, {local_docid(I), [{<<"int">>, I}]}}
    end, lists:seq(1, 25)),

    Actions1 = DocActions ++ LocalActions,

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, St3} = Engine:set(St2, security, [{<<"readers">>, <<"ohai">>}]),
    {ok, St4} = Engine:set(St3, revs_limit, 500),

    FakeEpochs = [
        {'test_engine@127.0.0.1',100},
        {'other_node@127.0.0.1', 0}
    ],

    {ok, St5} = Engine:set(St4, epochs, FakeEpochs),

    Actions2 = [
        {create, {<<"foo">>, []}},
        {create, {<<"bar">>, [{<<"hooray">>, <<"purple">>}]}},
        {conflict, {<<"bar">>, [{<<"booo">>, false}]}}
    ],

    {ok, St6} = test_engine_util:apply_actions(Engine, St5, Actions2),
    
    [FooFDI, BarFDI] = Engine:open_docs(St6, [<<"foo">>, <<"bar">>]),

    FooRev = test_engine_util:prev_rev(FooFDI),
    BarRev = test_engine_util:prev_rev(BarFDI),

    Actions3 = [
        {batch, [
            {purge, {<<"foo">>, FooRev#rev_info.rev}},
            {purge, {<<"bar">>, BarRev#rev_info.rev}}
        ]}
    ],

    {ok, St7} = test_engine_util:apply_actions(Engine, St6, Actions3),

    PurgedIdRevs = [
        {<<"bar">>, [BarRev#rev_info.rev]},
        {<<"foo">>, [FooRev#rev_info.rev]}
    ],

    ?assertEqual(PurgedIdRevs, lists:sort(Engine:get(St7, last_purged))),

    [Att0, Att1, Att2, Att3, Att4] = test_engine_util:prep_atts(Engine, St7, [
            {<<"ohai.txt">>, crypto:rand_bytes(2048)},
            {<<"stuff.py">>, crypto:rand_bytes(32768)},
            {<<"a.erl">>, crypto:rand_bytes(29)},
            {<<"a.hrl">>, crypto:rand_bytes(5000)},
            {<<"a.app">>, crypto:rand_bytes(400)}
        ]),

    Actions4 = [
        {create, {<<"small_att">>, [], [Att0]}},
        {create, {<<"large_att">>, [], [Att1]}},
        {create, {<<"multi_att">>, [], [Att2, Att3, Att4]}}
    ],
    {ok, St8} = test_engine_util:apply_actions(Engine, St7, Actions4),
    {ok, St9} = Engine:commit_data(St8),

    Db1 = test_engine_util:db_as_term(Engine, St9),

    Config = [
        {"database_compaction", "doc_buffer_size", "1024"},
        {"database_compaction", "checkpoint_after", "2048"}
    ],

    {ok, St10, DbName, _, Term} = test_engine_util:with_config(Config, fun() ->
        test_engine_util:compact(Engine, St9, Path)
    end),

    {ok, St11, undefined} = Engine:finish_compaction(St10, DbName, [], Term),
    Db2 = test_engine_util:db_as_term(Engine, St11),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


cet_recompact_updates() ->
    {ok, Engine, Path, St1} = test_engine_util:init_engine(dbpath),

    Actions1 = [
        {create, {<<"foo">>, []}},
        {create, {<<"bar">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, St3, DbName, _, Term} = test_engine_util:compact(Engine, St2, Path),

    Actions2 = [
        {update, {<<"foo">>, [{<<"updated">>, true}]}},
        {create, {<<"baz">>, []}}
    ],

    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions2),
    Db1 = test_engine_util:db_as_term(Engine, St4),

    {ok, St5, NewPid} = Engine:finish_compaction(St4, DbName, [], Term),

    ?assertEqual(true, is_pid(NewPid)),
    Ref = erlang:monitor(process, NewPid),

    NewTerm = receive
        {'$gen_cast', {compact_done, Engine, Term0}} ->
            Term0;
        {'DOWN', Ref, _, _, Reason} ->
            erlang:error({compactor_died, Reason})
        after 10000 ->
            erlang:error(compactor_timed_out)
    end,

    {ok, St6, undefined} = Engine:finish_compaction(St5, DbName, [], NewTerm),
    Db2 = test_engine_util:db_as_term(Engine, St6),
    Diff = test_engine_util:term_diff(Db1, Db2),
    ?assertEqual(nodiff, Diff).


docid(I) ->
    Str = io_lib:format("~4..0b", [I]),
    iolist_to_binary(Str).


local_docid(I) ->
    Str = io_lib:format("_local/~4..0b", [I]),
    iolist_to_binary(Str).

    