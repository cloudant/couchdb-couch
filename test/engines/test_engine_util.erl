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
    AccIn = {UpdateSeq, [], [], []},
    AccOut = lists:foldl(fun(Action, Acc) ->
        {SeqAcc, DocAcc, LDocAcc, PurgeAcc} = Acc,
        case Action of
            {_, {<<"_local/", _/binary>>, _}} ->
                LDoc = gen_local_write(Engine, St, Action),
                {SeqAcc, DocAcc, [LDoc | LDocAcc], PurgeAcc};
            _ ->
                case gen_write(Engine, St, Action, SeqAcc) of
                    {_OldFDI, _NewFDI} = Pair ->
                        {SeqAcc + 1, [Pair | DocAcc], LDocAcc, PurgeAcc};
                    {Pair, NewSeqAcc, NewPurgeInfo} ->
                        NewPurgeAcc = [NewPurgeInfo | PurgeAcc],
                        {NewSeqAcc, [Pair | DocAcc], LDocAcc, NewPurgeAcc}
                end
        end
    end, AccIn, Actions),
    {_, Docs0, LDocs, PurgeIdRevs} = AccOut,
    Docs = lists:reverse(Docs0),
    {ok, NewSt} = Engine:write_doc_infos(St, Docs, LDocs, PurgeIdRevs),
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

gen_write(Engine, St, {create, {DocId, Body, Atts0}}, UpdateSeq) ->
    [not_found] = Engine:open_docs(St, [DocId]),
    Atts = [couch_att:to_disk_term(Att) || Att <- Atts0],

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

gen_write(Engine, St, {purge, {DocId, PrevRevs, _}}, UpdateSeq) ->
    [#full_doc_info{} = PrevFDI] = Engine:open_docs(St, [DocId]),

    #full_doc_info{
        rev_tree = PrevTree
    } = PrevFDI,

    {NewTree, RemRevs} = couch_key_tree:remove_leafs(PrevTree, PrevRevs),
    RemovedAll = lists:sort(RemRevs) == lists:sort(PrevRevs),
    if RemovedAll -> ok; true ->
        % If we didn't purge all the requested revisions
        % then its a bug in the test.
        erlang:error({invalid_purge_test_revs, PrevRevs})
    end,

    case NewTree of
        [] ->
            % We've completely purged the document
            {{PrevFDI, not_found}, UpdateSeq + 1, {DocId, RemRevs}};
        _ ->
            % We have to relabel the update_seq of all
            % leaves. See couch_db_updater for details.
            {NewNewTree, NewUpdateSeq} = couch_key_tree:mapfold(fun
                (_RevId, Leaf, leaf, InnerSeqAcc) ->
                    {Leaf#leaf{seq = InnerSeqAcc + 1}, InnerSeqAcc + 1};
                (_RevId, Value, _Type, InnerSeqAcc) ->
                    {Value, InnerSeqAcc}
            end, UpdateSeq, NewTree),
            NewFDI = PrevFDI#full_doc_info{
                update_seq = NewUpdateSeq,
                rev_tree = NewNewTree
            },
            {{PrevFDI, NewFDI}, NewUpdateSeq + 1, {DocId, RemRevs}}
    end;
    

gen_write(Engine, St, {Action, {DocId, Body, Atts0}}, UpdateSeq) ->
    [#full_doc_info{} = PrevFDI] = Engine:open_docs(St, [DocId]),
    Atts = [couch_att:to_disk_term(Att) || Att <- Atts0],

    #full_doc_info{
        id = DocId,
        rev_tree = PrevRevTree
    } = PrevFDI,

    #rev_info{
        rev = PrevRev
    } = prev_rev(PrevFDI),

    Rev = gen_revision(Action, DocId, PrevRev, Body, Atts),
    Summary = make_doc_summary(Engine, St, {Body, Atts}),
    {ok, Ptr, Len} = Engine:write_doc_summary(St, Summary),

    Deleted = case Action of
        update -> false;
        conflict -> false;
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
    Tree = gen_tree(Action, RevPos, PrevRevId, Rev, Leaf),
    RevsLimit = Engine:get(St, revs_limit),
    {NewTree, new_leaf} = couch_key_tree:merge(PrevRevTree, Tree, RevsLimit),

    NewFDI = PrevFDI#full_doc_info{
        deleted = couch_doc:is_deleted(NewTree),
        update_seq = UpdateSeq,
        rev_tree = NewTree,
        sizes = Sizes
    },

    {PrevFDI, NewFDI}.


gen_revision(conflict, DocId, _PrevRev, Body, Atts) ->
    crypto:hash(md5, term_to_binary({DocId, Body, Atts}));
gen_revision(delete, DocId, PrevRev, Body, Atts) ->
    gen_revision(update, DocId, PrevRev, Body, Atts);
gen_revision(update, DocId, PrevRev, Body, Atts) ->
    crypto:hash(md5, term_to_binary({DocId, PrevRev, Body, Atts})).


gen_tree(conflict, _RevPos, _PrevRevId, Rev, Leaf) ->
    [{0, {Rev, Leaf, []}}];
gen_tree(delete, RevPos, PrevRevId, Rev, Leaf) ->
    gen_tree(update, RevPos, PrevRevId, Rev, Leaf);
gen_tree(update, RevPos, PrevRevId, Rev, Leaf) ->
    {RevPos, {PrevRevId, ?REV_MISSING, [{Rev, Leaf, []}]}}.


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


prep_atts(_Engine, _St, []) ->
    [];

prep_atts(Engine, St, [{FileName, Data} | Rest]) ->
    {_, Ref} = spawn_monitor(fun() ->
        {ok, Stream} = Engine:open_write_stream(St, []),
        exit(write_att(Stream, FileName, Data, Data))
    end),
    Att = receive
        {'DOWN', Ref, _, _, Resp} ->
            Resp
        after 5000 ->
            erlang:error(attachment_write_timeout)
    end,
    [Att | prep_atts(Engine, St, Rest)].


write_att(Stream, FileName, OrigData, <<>>) ->
    {StreamEngine, Len, Len, Md5, Md5} = couch_stream:close(Stream),
    couch_util:check_md5(Md5, crypto:hash(md5, OrigData)),
    Len = size(OrigData),
    couch_att:new([
        {name, FileName},
        {type, <<"application/octet-stream">>},
        {data, {stream, StreamEngine}},
        {att_len, Len},
        {disk_len, Len},
        {md5, Md5},
        {encoding, identity}
    ]);

write_att(Stream, FileName, OrigData, Data) ->
    {Chunk, Rest} = case size(Data) > 4096 of
        true ->
            <<Head:4096/binary, Tail/binary>> = Data,
            {Head, Tail};
        false ->
            {Data, <<>>}
    end,
    ok = couch_stream:write(Stream, Chunk),
    write_att(Stream, FileName, OrigData, Rest).
    

prev_rev(#full_doc_info{} = FDI) ->
    #doc_info{
        revs = [#rev_info{} = PrevRev]
    } = couch_doc:to_doc_info(FDI),
    PrevRev.
