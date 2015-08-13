-module(couch_bt_engine).


-export([
    exists/1,
    init/2,
    terminate/2,
    handle_info/2,

    close/1,

    delete/2,
    delete/3,

    sync/1,

    increment_update_seq/1,
    store_security/2,

    open_docs/2,
    open_local_docs/2,
    read_doc/2,
    get_design_docs/1,

    make_summary/2,
    write_summary/2,
    write_doc_infos/4,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    get_security/1,
    get_last_purged/1,
    get_doc_count/1,
    get_del_doc_count/1,
    get_size_info/1,

    start_compaction/3,
    finish_compaction/2,

    get/2,
    get/3,
    set/3
]).


-export([

    id_tree_split/1,
    id_tree_join/2,
    id_tree_reduce/2,

    seq_tree_split/1,
    seq_tree_join/2,
    seq_tree_reduce/2
]).


-include_lib("couch/include/couch_db.hrl").
-include("couch_bt_engine.hrl").


exists(FilePath) ->
    case filelib:is_file(FilePath) of
        true ->
            true;
        false ->
            filelib:is_file(FilePath ++ ".compact")
    end.


init(FilePath, Options) ->
    {ok, Fd} = open_db_file(FilePath, Options),
    Header = case lists:member(create, Options) of
        true ->
            delete_compaction_files(FilePath),
            Header0 = couch_bt_engine_header:new(),
            ok = couch_file:write_header(Fd, Header0),
            Header0;
        false ->
            case couch_file:read_header(Fd) of
                {ok, Header0} ->
                    Header0;
                no_valid_header ->
                    delete_compaction_files(FilePath),
                    Header0 =  couch_db_header:new(),
                    ok = couch_file:write_header(Fd, Header0),
                    Header0
            end
    end,
    {ok, init_state(FilePath, Fd, Header, Options)}.


terminate(_Reason, St) ->
    % If the reason we died is because our fd disappeared
    % then we don't need to try closing it again.
    if St#st.fd_monitor == closed -> ok; true ->
        ok = couch_file:close(St#st.fd)
    end,
    couch_util:shutdown_sync(St#st.fd),
    ok.


handle_info({'DOWN', Ref, _, _, _}, #st{fd_monitor=Ref} = St) ->
    {stop, normal, St#st{fd=undefined, fd_monitor=closed}}.


close(St) ->
    erlang:demonitor(St#st.fd_monitor, [flush]).


delete(St, RootDir) ->
    delete(RootDir, St#st.filepath).


delete(RootDir, FilePath, Async) ->
    %% Delete any leftover compaction files. If we don't do this a
    %% subsequent request for this DB will try to open them to use
    %% as a recovery.
    lists:foreach(fun(Ext) ->
        couch_file:delete(RootDir, FilePath ++ Ext)
    end, [".compact", ".compact.data", ".compact.meta"]),

    % Delete the actual database file
    couch_file:delete(RootDir, FilePath, Async).


sync(#st{fd = Fd}) ->
    ok = couch_file:sync(Fd).


commit_data(St) ->
    #st{
        fd = Fd,
        fsync_options = FsyncOptions,
        header = OldHeader,
        needs_commit = NeedsCommit
    } = St,

    NewHeader = update_header(St, OldHeader),

    case NewHeader /= OldHeader orelse NeedsCommit of
        true ->
            Before = lists:member(before_header, FsyncOptions),
            After = lists:member(after_header, FsyncOptions),

            if Before -> couch_file:sync(Fd); true -> ok end,
            ok = couch_file:write_header(Fd, NewHeader),
            if After -> couch_file:sync(Fd); true -> ok end,

            St#st{
                header = NewHeader,
                needs_commit = false
            };
        false ->
            St
    end.


get(#st{} = St, DbProp) ->
    ?MODULE:get(St, DbProp, undefined).


get(#st{header = Header}, DbProp, Default) ->
    couch_bt_engine_header:get(Header, DbProp, Default).


set(#st{} = St, DbProp, Value) ->
    #st{
        header = Header
    } = St,
    {ok, #st{
        header = couch_bt_engine_header:set(Header, DbProp, Value),
        needs_commit = true
    }}.


increment_update_seq(#st{} = St) ->
    Current = ?MODULE:get(St, update_seq),
    ?MODULE:set(St, update_seq, Current + 1).


store_security(#st{} = St, NewSecurity) ->
    Options = [{compression, St#st.compression}],
    {ok, Ptr, _} = couch_file:append_term(St#st.fd, NewSecurity, Options),
    set(St, security_ptr, Ptr).


open_docs(#st{} = St, DocIds) ->
    Results = couch_btree:lookup(St#st.id_tree, DocIds),
    lists:map(fun
        ({ok, FDI}) -> FDI;
        (not_found) -> not_found
    end, Results).


open_local_docs(#st{} = St, DocIds) ->
    % Need to convert the old style local doc
    % to a #doc{} record like used to happen
    % in open_doc_int
    erlang:error(vomit),
    Results = couch_btree:lookup(St#st.local_tree, DocIds),
    lists:map(fun
        ({ok, Doc}) -> Doc;
        (not_found) -> not_found
    end, Results).


read_doc(#st{} = St, Pos) ->
    couch_file:pread_term(St#st.fd, Pos).


get_design_docs(St) ->
    FoldFun = pipe([fun skip_deleted/4], fun
        (#full_doc_info{deleted = true}, _Reds, Acc) ->
            {ok, Acc};
        (#full_doc_info{id = Id} = FDI, _Reds, Acc) ->
            case Id of
                <<?DESIGN_DOC_PREFIX, _/binary>> ->
                    {ok, [FDI | Acc]};
                _ ->
                    {stop, Acc}
            end
    end),
    KeyOpts = [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}],
    {ok, _, Docs} = couch_btree:fold(St#st.id_tree, FoldFun, [], KeyOpts),
    {ok, lists:reverse(Docs)}.


make_summary(#st{} = St, {Body0, Atts0}) ->
    Comp = St#st.compression,
    Body = case couch_compress:is_compressed(Body0, Comp) of
        true -> Body0;
        % pre 1.2 database file format
        false -> couch_compress:compress(Body0, Comp)
    end,
    Atts = case couch_compress:is_compressed(Atts0, Comp) of
        true -> Atts0;
        false -> couch_compress:compress(Atts0, Comp)
    end,
    SummaryBin = ?term_to_bin({Body, Atts}),
    couch_file:assemble_file_chunk(SummaryBin, couch_util:md5(SummaryBin)).


write_summary(St, SummaryBinary) ->
    #st{
        fd = Fd
    } = St,
    couch_file:append_raw_chunk(Fd, SummaryBinary).


write_doc_infos(#st{} = St, FullDocInfos, RemoveSeqs, LocalDocs) ->
    #st{
        id_tree = IdTree,
        seq_tree = SeqTree,
        local_tree = LocalTree
    } = St,
    {ok, IdTree2} = couch_btree:add_remove(IdTree, FullDocInfos, []),
    {ok, SeqTree2} = couch_btree:add_remove(SeqTree, FullDocInfos, RemoveSeqs),
    {ok, LocalTree2} = couch_btree:add_remove(LocalTree, LocalDocs, []),
    St#st{
        id_tree = IdTree2,
        seq_tree = SeqTree2,
        local_tree = LocalTree2
    }.


open_write_stream(#st{} = St, Options) ->
    couch_stream:open({couch_bt_engine_stream, {St#st.fd, []}}, Options).


open_read_stream(#st{} = St, StreamSt) ->
    {couch_bt_engine_stream, {St#st.fd, StreamSt}}.


is_active_stream(#st{} = St, {couch_bt_engine_stream, Fd, _}) ->
    St#st.fd == Fd;
is_active_stream(_, _) ->
    false.


get_security(#st{} = St) ->
    case ?MODULE:get(St, security_ptr, nil) of
        nil ->
            {ok, []};
        Pointer ->
            couch_file:pread_term(St#st.fd, Pointer)
    end.


get_last_purged(#st{} = St) ->
    case ?MODULE:get(St, purged_docs, nil) of
        nil ->
            {ok, []};
        Pointer ->
            couch_file:pread_term(St#st.fd, Pointer)
    end.


get_doc_count(#st{} = St) ->
    {ok, {Count, _, _}} = couch_btree:full_reduce(St#st.id_tree),
    {ok, Count}.


get_del_doc_count(#st{} = St) ->
    {ok, {_, DelCount, _}} = couch_btree:full_reduce(St#st.id_tree),
    {ok, DelCount}.


get_size_info(#st{} = St) ->
    {ok, FileSize} = couch_file:bytes(St#st.fd),
    {ok, DbReduction} = couch_btree:full_reduce(St#st.id_tree),
    SizeInfo0 = element(3, DbReduction),
    SizeInfo = case SizeInfo0 of
        SI when is_record(SI, size_info) ->
            SI;
        {AS, ES} ->
            #size_info{active=AS, external=ES};
        AS ->
            #size_info{active=AS}
    end,
    ActiveSize = active_size(St, SizeInfo),
    ExternalSize = SizeInfo#size_info.external,
    [
        {active, ActiveSize},
        {external, ExternalSize},
        {file, FileSize}
    ].


start_compaction(St, DbName, Parent) ->
    spawn_link(couch_bt_engine_compactor, start, [St, DbName, Parent]).


finish_compaction(#st{} = OldSt, #st{} = NewSt1) ->
    #st{
        filepath = FilePath,
        local_tree = OldLocal
    } = OldSt,
    #st{
        filepath = CompactDataPath,
        local_tree = NewLocal1
    } = NewSt1,

    % suck up all the local docs into memory and write them to the new db
    LoadFun = fun(Value, _Offset, Acc) ->
        {ok, [Value | Acc]}
    end,
    {ok, _, LocalDocs} = couch_btree:foldl(OldLocal, LoadFun, []),
    {ok, NewLocal2} = couch_btree:add(NewLocal1, LocalDocs),

    NewSt2 = ?MODULE:set(NewSt1, compact_seq, ?MODULE:get(OldSt, update_seq)),
    NewSt3 = commit_data(NewSt2#st{
        local_tree = NewLocal2
    }),

    % Rename our *.compact.data file to *.compact so that if we
    % die between deleting the old file and renaming *.compact
    % we can recover correctly.
    ok = file:rename(CompactDataPath, FilePath ++ ".compact"),

    % Remove the uncompacted database file
    RootDir = config:get("couchdb", "database_dir", "."),
    delete(RootDir, FilePath, true),

    % Move our compacted file into its final location
    ok = file:rename(FilePath ++ ".compact", FilePath),

    % Delete the old meta compaction file after promoting
    % the compaction file.
    delete(RootDir, FilePath ++ ".compact.meta"),

    % We're finished with our old state
    close(OldSt),

    % And return our finished new state
    NewSt3#st{
        filepath = FilePath
    }.


id_tree_split(#full_doc_info{}=Info) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = Deleted,
        sizes = SizeInfo,
        rev_tree = Tree
    } = Info,
    {Id, {Seq, ?b2i(Deleted), split_sizes(SizeInfo), disk_tree(Tree)}}.


id_tree_join(Id, {HighSeq, Deleted, DiskTree}) ->
    % Handle old formats before data_size was added
    id_tree_join(Id, {HighSeq, Deleted, #size_info{}, DiskTree});

id_tree_join(Id, {HighSeq, Deleted, Sizes, DiskTree}) ->
    #full_doc_info{
        id = Id,
        update_seq = HighSeq,
        deleted = ?i2b(Deleted),
        sizes = couch_db_updater:upgrade_sizes(Sizes),
        rev_tree = rev_tree(DiskTree)
    }.


id_tree_reduce(reduce, FullDocInfos) ->
    lists:foldl(fun(Info, {NotDeleted, Deleted, Sizes}) ->
        Sizes2 = reduce_sizes(Sizes, Info#full_doc_info.sizes),
        case Info#full_doc_info.deleted of
        true ->
            {NotDeleted, Deleted + 1, Sizes2};
        false ->
            {NotDeleted + 1, Deleted, Sizes2}
        end
    end, {0, 0, #size_info{}}, FullDocInfos);
id_tree_reduce(rereduce, Reds) ->
    lists:foldl(fun
        ({NotDeleted, Deleted}, {AccNotDeleted, AccDeleted, _AccSizes}) ->
            % pre 1.2 format, will be upgraded on compaction
            {AccNotDeleted + NotDeleted, AccDeleted + Deleted, nil};
        ({NotDeleted, Deleted, Sizes}, {AccNotDeleted, AccDeleted, AccSizes}) ->
            AccSizes2 = reduce_sizes(AccSizes, Sizes),
            {AccNotDeleted + NotDeleted, AccDeleted + Deleted, AccSizes2}
    end, {0, 0, #size_info{}}, Reds).


seq_tree_split(#full_doc_info{}=Info) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = Del,
        sizes = SizeInfo,
        rev_tree = Tree
    } = Info,
    {Seq, {Id, ?b2i(Del), split_sizes(SizeInfo), disk_tree(Tree)}}.


seq_tree_join(Seq, {Id, Del, DiskTree}) when is_integer(Del) ->
    seq_tree_join(Seq, {Id, Del, {0, 0}, DiskTree});

seq_tree_join(Seq, {Id, Del, Sizes, DiskTree}) when is_integer(Del) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = ?i2b(Del),
        sizes = join_sizes(Sizes),
        rev_tree = rev_tree(DiskTree)
    };

seq_tree_join(KeySeq, {Id, RevInfos, DeletedRevInfos}) ->
    % Older versions stored #doc_info records in the seq_tree.
    % Compact to upgrade.
    Revs = lists:map(fun({Rev, Seq, Bp}) ->
        #rev_info{rev = Rev, seq = Seq, deleted = false, body_sp = Bp}
    end, RevInfos),
    DeletedRevs = lists:map(fun({Rev, Seq, Bp}) ->
        #rev_info{rev = Rev, seq = Seq, deleted = true, body_sp = Bp}
    end, DeletedRevInfos),
    #doc_info{
        id = Id,
        high_seq = KeySeq,
        revs = Revs ++ DeletedRevs
    }.


seq_tree_reduce(reduce, DocInfos) ->
    % count the number of documents
    length(DocInfos);
seq_tree_reduce(rereduce, Reds) ->
    lists:sum(Reds).


open_db_file(FilePath, Options) ->
    case couch_file:open(FilePath, Options) of
        {ok, Fd} ->
            {ok, Fd};
        {error, enoent} ->
            % Couldn't find file. is there a compact version? This ca
            % happen (rarely) if we crashed during the file switch.
            case couch_file:open(FilePath ++ ".compact", [nologifmissing]) of
                {ok, Fd} ->
                    Fmt = "Recovering from compaction file: ~s~s",
                    couch_log:info(Fmt, [FilePath, ".compact"]),
                    ok = file:rename(FilePath ++ ".compact", FilePath),
                    ok = couch_file:sync(Fd),
                    {ok, Fd};
                {error, enoent} ->
                    throw({not_found, no_db_file})
            end;
        Error ->
            throw(Error)
    end.


init_state(FilePath, Fd, Header0, Options) ->
    DefaultFSync = "[before_header, after_header, on_file_open]",
    FsyncStr = config:get("couchdb", "fsync_options", DefaultFSync),
    {ok, FsyncOptions} = couch_util:parse_term(FsyncStr),

    case lists:member(on_file_open, FsyncOptions) of
        true -> ok = couch_file:sync(Fd);
        _ -> ok
    end,

    Header = couch_bt_engine_header:upgrade(Header0),
    Compression = couch_compress:get_compression_method(),

    IdTreeState = couch_db_header:id_tree_state(Header),
    {ok, IdTree} = couch_btree:open(IdTreeState, Fd, [
            {split, fun ?MODULE:btree_by_id_split/1},
            {join, fun ?MODULE:btree_by_id_join/2},
            {reduce, fun ?MODULE:btree_by_id_reduce/2},
            {compression, Compression}
        ]),

    SeqTreeState = couch_db_header:seq_tree_state(Header),
    {ok, SeqTree} = couch_btree:open(SeqTreeState, Fd, [
            {split, fun ?MODULE:btree_by_seq_split/1},
            {join, fun ?MODULE:btree_by_seq_join/2},
            {reduce, fun ?MODULE:btree_by_seq_reduce/2},
            {compression, Compression}
        ]),

    LocalTreeState = couch_db_header:local_tree_state(Header),
    {ok, LocalTree} = couch_btree:open(LocalTreeState, Fd, [
            {compression, Compression}
        ]),

    ok = couch_file:set_db_pid(Fd, self()),

    St = #st{
        filepath = FilePath,
        fd = Fd,
        fd_monitor = erlang:monitor(process, Fd),
        fsync_options = FsyncOptions,
        header = Header,
        needs_commit = false,
        id_tree = IdTree,
        seq_tree = SeqTree,
        local_tree = LocalTree,
        compression = Compression
    },

    % If we just created a new UUID while upgrading a
    % database then we want to flush that to disk or
    % we risk sending out the uuid and having the db
    % crash which would result in it generating a new
    % uuid each time it was reopened.
    case Header /= Header0 of
        true ->
            {ok, commit_data(St)};
        false ->
            {ok, St}
    end.


update_header(St, Header) ->
    couch_bt_engine_header:set(Header, [
        {seq_tree_state, couch_btree:get_state(St#st.seq_tree)},
        {id_tree_state, couch_btree:get_state(St#st.id_tree)},
        {local_tree_state, couch_btree:get_state(St#st.local_tree)}
    ]).


delete_compaction_files(FilePath) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    delete_compaction_files(RootDir, FilePath).


delete_compaction_files(RootDir, FilePath) ->
    lists:foreach(fun(Ext) ->
        couch_file:delete(RootDir, FilePath ++ Ext)
    end, [".compact", ".compact.data", ".compact.meta"]).


rev_tree(DiskTree) ->
    couch_key_tree:map(fun
        (_RevId, {Del, Ptr, Seq}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq
            };
        (_RevId, {Del, Ptr, Seq, Size}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq,
                sizes = couch_db_updater:upgrade_sizes(Size)
            };
        (_RevId, {Del, Ptr, Seq, Sizes, Atts}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq,
                sizes = couch_db_updater:upgrade_sizes(Sizes),
                atts = Atts
            };
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING
    end, DiskTree).


disk_tree(RevTree) ->
    couch_key_tree:map(fun
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING;
        (_RevId, #leaf{} = Leaf) ->
            #leaf{
                deleted = Del,
                ptr = Ptr,
                seq = Seq,
                sizes = Sizes,
                atts = Atts
            } = Leaf,
            {?b2i(Del), Ptr, Seq, split_sizes(Sizes), Atts}
    end, RevTree).


split_sizes(#size_info{}=SI) ->
    {SI#size_info.active, SI#size_info.external}.


join_sizes({Active, External}) when is_integer(Active), is_integer(External) ->
    #size_info{active=Active, external=External}.


reduce_sizes(nil, _) ->
    nil;
reduce_sizes(_, nil) ->
    nil;
reduce_sizes(#size_info{}=S1, #size_info{}=S2) ->
    #size_info{
        active = S1#size_info.active + S2#size_info.active,
        external = S1#size_info.external + S2#size_info.external
    };
reduce_sizes(S1, S2) ->
    US1 = couch_db_updater:upgrade_sizes(S1),
    US2 = couch_db_updater:upgrade_sizes(S2),
    reduce_sizes(US1, US2).


active_size(#st{} = St, Size) when is_integer(Size) ->
    active_size(St, #size_info{active=Size});
active_size(#st{} = St, #size_info{} = SI) ->
    Trees = [
        St#st.id_tree,
        St#st.seq_tree,
        St#st.local_tree
    ],
    lists:foldl(fun(T, Acc) ->
        case couch_btree:size(T) of
            _ when Acc == null ->
                null;
            undefined ->
                null;
            Size ->
                Acc + Size
        end
    end, SI#size_info.active, Trees).


skip_deleted(traverse, LK, {Undeleted, _, _} = Reds, Acc) when Undeleted == 0 ->
    {skip, LK, Reds, Acc};
skip_deleted(Case, A, B, C) ->
    {Case, A, B, C}.

stop_on_leaving_namespace(NS) ->
    fun
        (visit, #full_doc_info{id = Key} = FullInfo, Reds, Acc) ->
            case has_prefix(Key, NS) of
                true ->
                    {visit, FullInfo, Reds, Acc};
                false ->
                    {stop, FullInfo, Reds, Acc}
            end;
        (Case, KV, Reds, Acc) ->
            {Case, KV, Reds, Acc}
    end.

has_prefix(Bin, Prefix) ->
    S = byte_size(Prefix),
    case Bin of
        <<Prefix:S/binary, "/", _/binary>> ->
            true;
        _Else ->
            false
    end.

pipe(Filters, Final) ->
    Wrap =
        fun
            (visit, KV, Reds, Acc) ->
                Final(KV, Reds, Acc);
            (skip, _KV, _Reds, Acc) ->
                {skip, Acc};
            (stop, _KV, _Reds, Acc) ->
                {stop, Acc};
            (traverse, _, _, Acc) ->
                {ok, Acc}
        end,
    do_pipe(Filters, Wrap).

do_pipe([], Fun) -> Fun;
do_pipe([Filter|Rest], F0) ->
    F1 = fun(C0, KV0, Reds0, Acc0) ->
        {C, KV, Reds, Acc} = Filter(C0, KV0, Reds0, Acc0),
        F0(C, KV, Reds, Acc)
    end,
    do_pipe(Rest, F1).

set_namespace_range(Options, undefined) -> Options;
set_namespace_range(Options, NS) ->
    %% FIXME depending on order we might need to swap keys
    SK = select_gt(
           proplists:get_value(start_key, Options, <<"">>),
           <<NS/binary, "/">>),
    EK = select_lt(
           proplists:get_value(end_key, Options, <<NS/binary, "0">>),
           <<NS/binary, "0">>),
    [{start_key, SK}, {end_key_gt, EK}].

select_gt(V1, V2) when V1 < V2 -> V2;
select_gt(V1, _V2) -> V1.

select_lt(V1, V2) when V1 > V2 -> V2;
select_lt(V1, _V2) -> V1.
