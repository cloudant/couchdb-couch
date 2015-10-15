-module(engine_tests_03_read_write_docs).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


read_write_docs_test_() ->
    test_engine_util:gather(?MODULE).


cet_read_empty_docs() ->
    {ok, Engine, St} = test_engine_util:init_engine(),

    ?assertEqual([not_found], Engine:open_docs(St, [<<"foo">>])),
    ?assertEqual(
        [not_found, not_found],
        Engine:open_docs(St, [<<"a">>, <<"b">>])
    ).


cet_read_empty_local_docs() ->
    {ok, Engine, St} = test_engine_util:init_engine(),

    ?assertEqual([not_found], Engine:open_local_docs(St, [<<"_local/foo">>])),
    ?assertEqual(
        [not_found, not_found],
        Engine:open_local_docs(St, [<<"_local/a">>, <<"_local/b">>])
    ).


cet_write_one_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(1, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(1, Engine:get(St4, update_seq)),

    [FDI] = Engine:open_docs(St4, [<<"foo">>]),
    #rev_info{
        body_sp = DocPtr
    } = test_engine_util:prev_rev(FDI),
    {ok, {Body0, _Atts}} = Engine:read_doc(St4, DocPtr),
    Body1 = if not is_binary(Body0) -> Body0; true ->
        binary_to_term(Body0)
    end,
    ?assertEqual([{<<"vsn">>, 1}], Body1).


cet_write_two_docs() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {create, {<<"bar">>, [{<<"stuff">>, true}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(2, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(2, Engine:get(St4, update_seq)),

    Resps = Engine:open_docs(St4, [<<"foo">>, <<"bar">>]),
    ?assertEqual(false, lists:member(not_found, Resps)).


cet_write_three_doc_batch() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {batch, [
            {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
            {create, {<<"bar">>, [{<<"stuff">>, true}]}},
            {create, {<<"baz">>, []}}
        ]}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(3, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(3, Engine:get(St4, update_seq)),

    Resps = Engine:open_docs(St4, [<<"foo">>, <<"bar">>, <<"baz">>]),
    ?assertEqual(false, lists:member(not_found, Resps)).


cet_update_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {update, {<<"foo">>, [{<<"vsn">>, 2}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(1, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(2, Engine:get(St4, update_seq)),

    [FDI] = Engine:open_docs(St4, [<<"foo">>]),
    #rev_info{
        body_sp = DocPtr
    } = test_engine_util:prev_rev(FDI),
    {ok, {Body0, _Atts}} = Engine:read_doc(St4, DocPtr),
    Body1 = if not is_binary(Body0) -> Body0; true ->
        binary_to_term(Body0)
    end,
    ?assertEqual([{<<"vsn">>, 2}], Body1).


cet_delete_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {delete, {<<"foo">>, []}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(1, Engine:get(St4, del_doc_count)),
    ?assertEqual(2, Engine:get(St4, update_seq)),

    [FDI] = Engine:open_docs(St4, [<<"foo">>]),
    #rev_info{
        body_sp = DocPtr
    } = test_engine_util:prev_rev(FDI),
    {ok, {Body0, _Atts}} = Engine:read_doc(St4, DocPtr),
    Body1 = if not is_binary(Body0) -> Body0; true ->
        binary_to_term(Body0)
    end,
    ?assertEqual([], Body1).


cet_write_local_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"_local/foo">>, [{<<"yay">>, false}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(0, Engine:get(St4, update_seq)),

    [not_found] = Engine:open_docs(St4, [<<"_local/foo">>]),
    [#doc{} = Doc] = Engine:open_local_docs(St4, [<<"_local/foo">>]),
    ?assertEqual([{<<"yay">>, false}], Doc#doc.body).


cet_write_mixed_batch() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {batch, [
            {create, {<<"bar">>, []}},
            {create, {<<"_local/foo">>, [{<<"yay">>, false}]}}
        ]}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(1, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(1, Engine:get(St4, update_seq)),

    [#full_doc_info{}] = Engine:open_docs(St4, [<<"bar">>]),
    [not_found] = Engine:open_docs(St4, [<<"_local/foo">>]),

    [not_found] = Engine:open_local_docs(St4, [<<"bar">>]),
    [#doc{}] = Engine:open_local_docs(St4, [<<"_local/foo">>]).


cet_update_local_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"_local/foo">>, []}},
        {update, {<<"_local/foo">>, [{<<"stuff">>, null}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(0, Engine:get(St4, update_seq)),

    [not_found] = Engine:open_docs(St4, [<<"_local/foo">>]),
    [#doc{} = Doc] = Engine:open_local_docs(St4, [<<"_local/foo">>]),
    ?assertEqual([{<<"stuff">>, null}], Doc#doc.body).


cet_delete_local_doc() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    ?assertEqual(0, Engine:get(St1, doc_count)),
    ?assertEqual(0, Engine:get(St1, del_doc_count)),
    ?assertEqual(0, Engine:get(St1, update_seq)),

    Actions = [
        {create, {<<"_local/foo">>, []}},
        {delete, {<<"_local/foo">>, []}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),
    {ok, St4} = Engine:init(DbPath, []),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(0, Engine:get(St4, update_seq)),

    [not_found] = Engine:open_docs(St4, [<<"_local/foo">>]),
    ?assertEqual([not_found], Engine:open_local_docs(St4, [<<"_local/foo">>])).
