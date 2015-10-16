-module(test_engine_util).
-compile(export_all).


-include_lib("couch/include/couch_db.hrl").


gather(Module) ->
    Exports = Module:module_info(exports),
    Tests = lists:foldl(fun({Fun, Arity}, Acc) ->
        case {atom_to_list(Fun), Arity} of
            {[$c, $e, $t, $_ | _], 0} ->
                [{spawn, fun Module:Fun/Arity} | Acc];
            _ ->
                Acc
        end
    end, [], Exports),
    lists:reverse(Tests).


rootdir() ->
    config:get("couchdb", "database_dir", ".").


dbpath() ->
    binary_to_list(filename:join(rootdir(), couch_uuids:random())).


get_engine() ->
    {ok, {_, Engine}} = application:get_env(couch, test_engine),
    Engine.


init_engine() ->
    init_engine(default).


init_engine(default) ->
    Engine = get_engine(),
    DbPath = dbpath(),
    {ok, St} = Engine:init(DbPath, [create]),
    {ok, Engine, St};

init_engine(dbpath) ->
    Engine = get_engine(),
    DbPath = dbpath(),
    {ok, St} = Engine:init(DbPath, [create]),
    {ok, Engine, DbPath, St}.


apply_actions(_Engine, St, []) ->
    {ok, St};

apply_actions(Engine, St, [Action | Rest]) ->
    NewSt = apply_action(Engine, St, Action),
    apply_actions(Engine, NewSt, Rest).


apply_action(Engine, St, {batch, BatchActions}) ->
    apply_batch(Engine, St, BatchActions);

apply_action(Engine, St, Action) ->
    apply_batch(Engine, St, [Action]).


apply_batch(Engine, St, Actions) ->
    UpdateSeq = Engine:get(St, update_seq) + 1,
    AccIn = {UpdateSeq, [], []},
    AccOut = lists:foldl(fun(Action, Acc) ->
        {SeqAcc, DocAcc, LDocAcc} = Acc,
        case Action of
            {_, {<<"_local/", _/binary>>, _}} ->
                LDoc = gen_local_write(Engine, St, Action),
                {SeqAcc, DocAcc, [LDoc | LDocAcc]};
            _ ->
                Doc = gen_write(Engine, St, Action, SeqAcc),
                {SeqAcc + 1, [Doc | DocAcc], LDocAcc}
        end
    end, AccIn, Actions),
    {_, Docs0, LDocs} = AccOut,
    Docs = lists:reverse(Docs0),
    {ok, NewSt} = Engine:write_doc_infos(St, Docs, LDocs, []),
    NewSt.


gen_local_write(Engine, St, {Action, {DocId, Body}}) ->
    PrevRev = case Engine:open_local_docs(St, [DocId]) of
        [not_found] ->
            0;
        [#doc{revs = {0, []}}] ->
            0;
        [#doc{revs = {0, [RevStr | _]}}] ->
            list_to_integer(binary_to_list(RevStr))
    end,
    {RevId, Deleted} = case Action of
        Action when Action == create; Action == update ->
            {list_to_binary(integer_to_list(PrevRev + 1)), false};
        delete ->
            {<<"0">>, true}
    end,
    #doc{
        id = DocId,
        revs = {0, [RevId]},
        body = Body,
        deleted = Deleted
    }.


gen_write(Engine, St, {Action, {DocId, Body}}, UpdateSeq) ->
    gen_write(Engine, St, {Action, {DocId, Body, []}}, UpdateSeq);

gen_write(Engine, St, {create, {DocId, Body, Atts}}, UpdateSeq) ->
    [not_found] = Engine:open_docs(St, [DocId]),

    Rev = crypto:hash(md5, term_to_binary({DocId, Body, Atts})),
    Summary = make_doc_summary(Engine, St, {Body, Atts}),
    {ok, Ptr, Len} = Engine:write_doc_summary(St, Summary),

    Sizes = #size_info{
        active = Len,
        external = erlang:external_size({Body, Atts})
    },

    Leaf = #leaf{
        deleted = false,
        ptr = Ptr,
        seq = UpdateSeq,
        sizes = Sizes,
        atts = Atts
    },

    {not_found, #full_doc_info{
        id = DocId,
        deleted = false,
        update_seq = UpdateSeq,
        rev_tree = [{0, {Rev, Leaf, []}}],
        sizes = Sizes
    }};

gen_write(Engine, St, {Action, {DocId, Body, Atts}}, UpdateSeq) ->
    [#full_doc_info{} = PrevFDI] = Engine:open_docs(St, [DocId]),

    #full_doc_info{
        id = DocId,
        rev_tree = PrevRevTree
    } = PrevFDI,

    #rev_info{
        rev = PrevRev
    } = prev_rev(PrevFDI),

    Rev = crypto:hash(md5, term_to_binary({DocId, PrevRev, Body, Atts})),
    Summary = make_doc_summary(Engine, St, {Body, Atts}),
    {ok, Ptr, Len} = Engine:write_doc_summary(St, Summary),

    Deleted = case Action of
        update -> false;
        delete -> true
    end,

    Sizes = #size_info{
        active = Len,
        external = erlang:external_size({Body, Atts})
    },

    Leaf = #leaf{
        deleted = Deleted,
        ptr = Ptr,
        seq = UpdateSeq,
        sizes = Sizes,
        atts = Atts
    },

    {RevPos, PrevRevId} = PrevRev,
    Tree = {RevPos, {PrevRevId, ?REV_MISSING, [{Rev, Leaf, []}]}},
    RevsLimit = Engine:get(St, revs_limit),
    {NewTree, new_leaf} = couch_key_tree:merge(PrevRevTree, Tree, RevsLimit),

    NewFDI = PrevFDI#full_doc_info{
        deleted = Deleted,
        update_seq = UpdateSeq,
        rev_tree = NewTree,
        sizes = Sizes
    },

    {PrevFDI, NewFDI}.


make_doc_summary(Engine, St, DocData) ->
    {_, Ref} = spawn_monitor(fun() ->
        exit({result, Engine:make_doc_summary(St, DocData)})
    end),
    receive
        {'DOWN', Ref, _, _, {result, Summary}} ->
            Summary
    after 1000 ->
        erlang:error(make_doc_summary_timeout)
    end.


prev_rev(#full_doc_info{} = FDI) ->
    #doc_info{
        revs = [#rev_info{} = PrevRev]
    } = couch_doc:to_doc_info(FDI),
    PrevRev.
    
