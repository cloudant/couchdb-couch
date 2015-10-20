-module(engine_tests_07_purge_docs).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


purge_docs_test_() ->
    test_engine_util:gather(?MODULE).


cet_purge_simple() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),

    ?assertEqual(1, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(1, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], Engine:get(St2, last_purged)),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),

    ?assertEqual(0, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(2, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], Engine:get(St3, last_purged)).


cet_purge_conflicts() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {conflict, {<<"foo">>, [{<<"vsn">>, 2}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),

    ?assertEqual(1, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], Engine:get(St2, last_purged)),

    [FDI1] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev1 = test_engine_util:prev_rev(FDI1),
    Rev1 = PrevRev1#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev1}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),

    ?assertEqual(1, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(4, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev1]}], Engine:get(St3, last_purged)),

    [FDI2] = Engine:open_docs(St3, [<<"foo">>]),
    PrevRev2 = test_engine_util:prev_rev(FDI2),
    Rev2 = PrevRev2#rev_info.rev,

    Actions3 = [
        {purge, {<<"foo">>, Rev2}}
    ],
    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions3),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(5, Engine:get(St4, update_seq)),
    ?assertEqual(2, Engine:get(St4, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev2]}], Engine:get(St4, last_purged)).


cet_add_delete_purge() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {delete, {<<"foo">>, [{<<"vsn">>, 2}]}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),

    ?assertEqual(0, Engine:get(St2, doc_count)),
    ?assertEqual(1, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], Engine:get(St2, last_purged)),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),

    ?assertEqual(0, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(3, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], Engine:get(St3, last_purged)).


cet_add_two_purge_one() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {create, {<<"bar">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),

    ?assertEqual(2, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], Engine:get(St2, last_purged)),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),

    ?assertEqual(1, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(3, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], Engine:get(St3, last_purged)).
