-module(couch_bt_engine).


-export([
    exists/1,
    init/2,
    delete/3,

    sync/1,

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



-record(st, {
    filepath,
    fd,
    fd_monitor,
    fsync_options,
    header,
    needs_commit,
    id_tree,
    seq_tree,
    local_tree
}).


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
                    delete_compaction_Files(FilePath),
                    Header0 =  couch_db_header:new(),
                    ok = couch_file:write_header(Fd, Header0),
                    Header0
            end
    end,
    State = init_state(FilePath, Fd, Header, Options),

    % we don't load validation funs here because the fabric query is liable to
    % race conditions.  Instead see couch_db:validate_doc_update, which loads
    % them lazily
    {ok, Db#db{main_pid = self()}}.


terminate(_Reason, St) ->
    % If the reason we died is because our fd disappeared
    % then we don't need to try closing it again.
    if St#st.fd_monitor == closed -> ok; true ->
        ok = couch_file:close(Db#db.fd)
    end,
    couch_util:shutdown_sync(Fd),
    ok.


close(St) ->
    erlang:demonitor(St#st.fd_monitor, [flush]).


delete(St, RootDir) ->
    delete(RootDir, St#st.filepath).


delete(RootDir, FilePath, Async) ->
    %% Delete any leftover compaction files. If we don't do this a
    %% subsequent request for this DB will try to open them to use
    %% as a recovery.
    lists:foreach(fun(Ext) ->
        couch_file:delete(Server#server.root_dir, FullFilepath ++ Ext)
    end, [".compact", ".compact.data", ".compact.meta"]),

    % Delete the actual database file
    couch_file:delete(RootDir, FilePath, Async).


sync(#st{fd = Fd}) ->
    ok = couch_file:sync(Fd).


commit_data(St) ->
    #st{
        fd = Fd,
        filepath = FilePath,
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


is_current_stream(#st{} = St, StreamFd) ->
    StreamFd == St#st.fd;
is_current_stream(_, _) ->
    false.


write_summary(St, SummaryBinary) ->
    #st{
        fd = Fd
    } = St,
    couch_file:append_raw_chunk(Fd, SummaryBinary).


write_doc_infos(St, FullDocInfos, RemoveSeqs, LocalDocs) ->
    ok.


start_compaction(St) ->
    ok.


finalize_compaction(St) ->
    % Assert St#st.filepath ends with .compact.data
    DestPath = remove_data_suffix(St#st.filepath),
    ok = file:rename(St#st.filepath, DestPath),
    couch_file:delete(RootDir, Filepath ++ ".meta"),
    {ok, St#st{filepath = DestPath}}.
    


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
        sizes = upgrade_sizes(Sizes),
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
        #rev_info{rev = Rev, Seq = Seq, deleted = true, body_sp = Bp}
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
            case couch_file:open(Filepath ++ ".compact", [nologifmissing]) of
                {ok, Fd} ->
                    Fmt = "Recovering from compaction file: ~s~s",
                    couch_log:info(Fmt, [Filepath, ".compact"]),
                    ok = file:rename(Filepath ++ ".compact", Filepath),
                    ok = couch_file:sync(Fd),
                    {ok, Fd};
                {error, enoent} ->
                    throw({not_found, no_db_file})
            end;
        Error ->
            throw(Error)
    end.


init_state(FilePath, Fd, Header0, Options) ->
    DefaultFSync = "[before_header, after_header, on_file_open]"
    FsyncStr = config:get("couchdb", "fsync_options", DefaultFSync),
    {ok, FsyncOptions} = couch_util:parse_term(FsyncStr),

    case lists:member(on_file_open, FsyncOptions) of
        true -> ok = Engine:sync(EngineState);
        _ -> ok
    end,

    Header = couch_bt_engine_header:upgrade(Header0),
    Compression = couch_compress:get_compression_method(),

    IdTreeState = couch_db_header:id_tree_state(Header),
    {ok, IdBTree} = couch_btree:open(IdTreeState, Fd, [
            {split, fun ?MODULE:btree_by_id_split/1},
            {join, fun ?MODULE:btree_by_id_join/2},
            {reduce, fun ?MODULE:btree_by_id_reduce/2},
            {compression, Compression}
        ]),

    SeqTreeState = couch_db_header:seq_tree_state(Header),
    {ok, SeqBTree} = couch_btree:open(SeqTreeState, Fd, [
            {split, fun ?MODULE:btree_by_seq_split/1},
            {join, fun ?MODULE:btree_by_seq_join/2},
            {reduce, fun ?MODULE:btree_by_seq_reduce/2},
            {compression, Compression}
        ]),

    LocalTreeState = couch_db_header:local_tree_state(Header),
    {ok, LocalBTree} = couch_btree:open(LocalTreeState, Fd, [
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
        id_tree = IdBtree,
        seq_tree = SeqBtree,
        local_tree = LocalDocsBtree
    },

    % If we just created a new UUID while upgrading a
    % database then we want to flush that to disk or
    % we risk sending out the uuid and having the db
    % crash which would result in it generating a new
    % uuid each time it was reopened.
    case Header /= Header0 of
        true ->
            {ok, sync_header(St, Header)};
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
        couch_file:delete(Server#server.root_dir, FullFilepath ++ Ext)
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
                sizes = upgrade_sizes(Size)
            };
        (_RevId, {Del, Ptr, Seq, Sizes, Atts}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq,
                sizes = upgrade_sizes(Sizes),
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


upgrade_sizes(#size_info{}=SI) ->
    SI;
upgrade_sizes({D, E}) ->
    #size_info{active=D, external=E};
upgrade_sizes(S) when is_integer(S) ->
    #size_info{active=S, external=0}.


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
    reduce_sizes(upgrade_sizes(S1), upgrade_sizes(S2)).




