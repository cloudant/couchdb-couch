-module(couch_bt_engine_compactor).

copy_doc_attachments(#db{fd = SrcFd} = SrcDb, SrcSp, DestFd) ->
    {ok, {BodyData, BinInfos0}} = couch_db:read_doc(SrcDb, SrcSp),
    BinInfos = case BinInfos0 of
    _ when is_binary(BinInfos0) ->
        couch_compress:decompress(BinInfos0);
    _ when is_list(BinInfos0) ->
        % pre 1.2 file format
        BinInfos0
    end,
    % copy the bin values
    NewBinInfos = lists:map(
        fun({Name, Type, BinSp, AttLen, RevPos, ExpectedMd5}) ->
            % 010 UPGRADE CODE
            {NewBinSp, AttLen, AttLen, ActualMd5, _IdentityMd5} =
                couch_stream:copy_to_new_stream(SrcFd, BinSp, DestFd),
            check_md5(ExpectedMd5, ActualMd5),
            {Name, Type, NewBinSp, AttLen, AttLen, RevPos, ExpectedMd5, identity};
        ({Name, Type, BinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc1}) ->
            {NewBinSp, AttLen, _, ActualMd5, _IdentityMd5} =
                couch_stream:copy_to_new_stream(SrcFd, BinSp, DestFd),
            check_md5(ExpectedMd5, ActualMd5),
            Enc = case Enc1 of
            true ->
                % 0110 UPGRADE CODE
                gzip;
            false ->
                % 0110 UPGRADE CODE
                identity;
            _ ->
                Enc1
            end,
            {Name, Type, NewBinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc}
        end, BinInfos),
    {BodyData, NewBinInfos}.

merge_lookups(Infos, []) ->
    Infos;
merge_lookups([], _) ->
    [];
merge_lookups([#doc_info{}=DI | RestInfos], [{ok, FDI} | RestLookups]) ->
    % Assert we've matched our lookups
    if DI#doc_info.id == FDI#full_doc_info.id -> ok; true ->
        erlang:error({mismatched_doc_infos, DI#doc_info.id})
    end,
    [FDI | merge_lookups(RestInfos, RestLookups)];
merge_lookups([FDI | RestInfos], Lookups) ->
    [FDI | merge_lookups(RestInfos, Lookups)].

check_md5(Md5, Md5) -> ok;
check_md5(_, _) -> throw(md5_mismatch).

copy_docs(Db, #db{fd = DestFd} = NewDb, MixedInfos, Retry) ->
    DocInfoIds = [Id || #doc_info{id=Id} <- MixedInfos],
    LookupResults = couch_btree:lookup(Db#db.id_tree, DocInfoIds),
    % COUCHDB-968, make sure we prune duplicates during compaction
    NewInfos0 = lists:usort(fun(#full_doc_info{id=A}, #full_doc_info{id=B}) ->
        A =< B
    end, merge_lookups(MixedInfos, LookupResults)),

    NewInfos1 = lists:map(fun(Info) ->
        {NewRevTree, FinalAcc} = couch_key_tree:mapfold(fun
            (_Rev, #leaf{ptr=Sp}=Leaf, leaf, SizesAcc) ->
                {Body, AttInfos} = copy_doc_attachments(Db, Sp, DestFd),
                SummaryChunk = make_doc_summary(NewDb, {Body, AttInfos}),
                ExternalSize = ?term_size(SummaryChunk),
                {ok, Pos, SummarySize} = couch_file:append_raw_chunk(
                    DestFd, SummaryChunk),
                AttSizes = [{element(3,A), element(4,A)} || A <- AttInfos],
                NewLeaf = Leaf#leaf{
                    ptr = Pos,
                    sizes = #size_info{
                        active = SummarySize,
                        external = ExternalSize
                    },
                    atts = AttSizes
                },
                {NewLeaf, add_sizes(leaf, NewLeaf, SizesAcc)};
            (_Rev, _Leaf, branch, SizesAcc) ->
                {?REV_MISSING, SizesAcc}
        end, {0, 0, []}, Info#full_doc_info.rev_tree),
        {FinalAS, FinalES, FinalAtts} = FinalAcc,
        TotalAttSize = lists:foldl(fun({_, S}, A) -> S + A end, 0, FinalAtts),
        NewActiveSize = FinalAS + TotalAttSize,
        NewExternalSize = FinalES + TotalAttSize,
        Info#full_doc_info{
            rev_tree = NewRevTree,
            sizes = #size_info{
                active = NewActiveSize,
                external = NewExternalSize
            }
        }
    end, NewInfos0),

    NewInfos = stem_full_doc_infos(Db, NewInfos1),
    RemoveSeqs =
    case Retry of
    nil ->
        [];
    OldDocIdTree ->
        % Compaction is being rerun to catch up to writes during the
        % first pass. This means we may have docs that already exist
        % in the seq_tree in the .data file. Here we lookup any old
        % update_seqs so that they can be removed.
        Ids = [Id || #full_doc_info{id=Id} <- NewInfos],
        Existing = couch_btree:lookup(OldDocIdTree, Ids),
        [Seq || {ok, #full_doc_info{update_seq=Seq}} <- Existing]
    end,

    {ok, SeqTree} = couch_btree:add_remove(
            NewDb#db.seq_tree, NewInfos, RemoveSeqs),

    FDIKVs = lists:map(fun(#full_doc_info{id=Id, update_seq=Seq}=FDI) ->
        {{Id, Seq}, FDI}
    end, NewInfos),
    {ok, IdEms} = couch_emsort:add(NewDb#db.id_tree, FDIKVs),
    update_compact_task(length(NewInfos)),
    NewDb#db{id_tree=IdEms, seq_tree=SeqTree}.


copy_compact(Db, NewDb0, Retry) ->
    Compression = couch_compress:get_compression_method(),
    NewDb = NewDb0#db{compression=Compression},
    TotalChanges = couch_db:count_changes_since(Db, NewDb#db.update_seq),
    BufferSize = list_to_integer(
        config:get("database_compaction", "doc_buffer_size", "524288")),
    CheckpointAfter = couch_util:to_integer(
        config:get("database_compaction", "checkpoint_after",
            BufferSize * 10)),

    EnumBySeqFun =
    fun(DocInfo, _Offset,
            {AccNewDb, AccUncopied, AccUncopiedSize, AccCopiedSize}) ->

        Seq = case DocInfo of
            #full_doc_info{} -> DocInfo#full_doc_info.update_seq;
            #doc_info{} -> DocInfo#doc_info.high_seq
        end,

        AccUncopiedSize2 = AccUncopiedSize + ?term_size(DocInfo),
        if AccUncopiedSize2 >= BufferSize ->
            NewDb2 = copy_docs(
                Db, AccNewDb, lists:reverse([DocInfo | AccUncopied]), Retry),
            AccCopiedSize2 = AccCopiedSize + AccUncopiedSize2,
            if AccCopiedSize2 >= CheckpointAfter ->
                CommNewDb2 = commit_compaction_data(NewDb2#db{update_seq=Seq}),
                {ok, {CommNewDb2, [], 0, 0}};
            true ->
                {ok, {NewDb2#db{update_seq = Seq}, [], 0, AccCopiedSize2}}
            end;
        true ->
            {ok, {AccNewDb, [DocInfo | AccUncopied], AccUncopiedSize2,
                AccCopiedSize}}
        end
    end,

    TaskProps0 = [
        {type, database_compaction},
        {database, Db#db.name},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ],
    case (Retry =/= nil) and couch_task_status:is_task_added() of
    true ->
        couch_task_status:update([
            {retry, true},
            {progress, 0},
            {changes_done, 0},
            {total_changes, TotalChanges}
        ]);
    false ->
        couch_task_status:add_task(TaskProps0),
        couch_task_status:set_update_frequency(500)
    end,

    {ok, _, {NewDb2, Uncopied, _, _}} =
        couch_btree:foldl(Db#db.seq_tree, EnumBySeqFun,
            {NewDb, [], 0, 0},
            [{start_key, NewDb#db.update_seq + 1}]),

    NewDb3 = copy_docs(Db, NewDb2, lists:reverse(Uncopied), Retry),

    % copy misc header values
    if NewDb3#db.security /= Db#db.security ->
        {ok, Ptr, _} = couch_file:append_term(
            NewDb3#db.fd, Db#db.security,
            [{compression, NewDb3#db.compression}]),
        NewDb4 = NewDb3#db{security=Db#db.security, security_ptr=Ptr};
    true ->
        NewDb4 = NewDb3
    end,

    commit_compaction_data(NewDb4#db{update_seq=Db#db.update_seq}).


start_copy_compact(#db{}=Db) ->
    erlang:put(io_priority, {db_compact, Db#db.name}),
    #db{name=Name, filepath=Filepath, options=Options, header=Header} = Db,
    couch_log:debug("Compaction process spawned for db \"~s\"", [Name]),

    {ok, NewDb, DName, DFd, MFd, Retry} =
        open_compaction_files(Name, Header, Filepath, Options),
    erlang:monitor(process, MFd),

    % This is a bit worrisome. init_db/4 will monitor the data fd
    % but it doesn't know about the meta fd. For now I'll maintain
    % that the data fd is the old normal fd and meta fd is special
    % and hope everything works out for the best.
    unlink(DFd),

    NewDb1 = copy_purge_info(Db, NewDb),
    NewDb2 = copy_compact(Db, NewDb1, Retry),
    NewDb3 = sort_meta_data(NewDb2),
    NewDb4 = commit_compaction_data(NewDb3),
    NewDb5 = copy_meta_data(NewDb4),
    NewDb6 = sync_header(NewDb5, db_to_header(NewDb5, NewDb5#db.header)),
    close_db(NewDb6),

    ok = couch_file:close(MFd),
    gen_server:cast(Db#db.main_pid, {compact_done, DName}).


open_compaction_files(DbName, SrcHdr, DbFilePath, Options) ->
    DataFile = DbFilePath ++ ".compact.data",
    MetaFile = DbFilePath ++ ".compact.meta",
    {ok, DataFd, DataHdr} = open_compaction_file(DataFile),
    {ok, MetaFd, MetaHdr} = open_compaction_file(MetaFile),
    DataHdrIsDbHdr = couch_db_header:is_header(DataHdr),
    case {DataHdr, MetaHdr} of
        {#comp_header{}=A, #comp_header{}=A} ->
            DbHeader = A#comp_header.db_header,
            Db0 = init_db(DbName, DataFile, DataFd, DbHeader, Options),
            Db1 = bind_emsort(Db0, MetaFd, A#comp_header.meta_state),
            {ok, Db1, DataFile, DataFd, MetaFd, Db0#db.id_tree};
        _ when DataHdrIsDbHdr ->
            ok = reset_compaction_file(MetaFd, couch_db_header:from(SrcHdr)),
            Db0 = init_db(DbName, DataFile, DataFd, DataHdr, Options),
            Db1 = bind_emsort(Db0, MetaFd, nil),
            {ok, Db1, DataFile, DataFd, MetaFd, Db0#db.id_tree};
        _ ->
            Header = couch_db_header:from(SrcHdr),
            ok = reset_compaction_file(DataFd, Header),
            ok = reset_compaction_file(MetaFd, Header),
            Db0 = init_db(DbName, DataFile, DataFd, Header, Options),
            Db1 = bind_emsort(Db0, MetaFd, nil),
            {ok, Db1, DataFile, DataFd, MetaFd, nil}
    end.


open_compaction_file(FilePath) ->
    case couch_file:open(FilePath, [nologifmissing]) of
        {ok, Fd} ->
            case couch_file:read_header(Fd) of
                {ok, Header} -> {ok, Fd, Header};
                no_valid_header -> {ok, Fd, nil}
            end;
        {error, enoent} ->
            {ok, Fd} = couch_file:open(FilePath, [create]),
            {ok, Fd, nil}
    end.


reset_compaction_file(Fd, Header) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, Header).


copy_purge_info(OldDb, NewDb) ->
    OldHdr = OldDb#db.header,
    NewHdr = NewDb#db.header,
    OldPurgeSeq = couch_db_header:purge_seq(OldHdr),
    if OldPurgeSeq > 0 ->
        {ok, PurgedIdsRevs} = couch_db:get_last_purged(OldDb),
        Opts = [{compression, NewDb#db.compression}],
        {ok, Ptr, _} = couch_file:append_term(NewDb#db.fd, PurgedIdsRevs, Opts),
        NewNewHdr = couch_db_header:set(NewHdr, [
            {purge_seq, OldPurgeSeq},
            {purged_docs, Ptr}
        ]),
        NewDb#db{header = NewNewHdr};
    true ->
        NewDb
    end.


commit_compaction_data(#db{}=Db) ->
    % Compaction needs to write headers to both the data file
    % and the meta file so if we need to restart we can pick
    % back up from where we left off.
    commit_compaction_data(Db, couch_emsort:get_fd(Db#db.id_tree)),
    commit_compaction_data(Db, Db#db.fd).


commit_compaction_data(#db{header=OldHeader}=Db0, Fd) ->
    % Mostly copied from commit_data/2 but I have to
    % replace the logic to commit and fsync to a specific
    % fd instead of the Filepath stuff that commit_data/2
    % does.
    DataState = couch_db_header:id_tree_state(OldHeader),
    MetaFd = couch_emsort:get_fd(Db0#db.id_tree),
    MetaState = couch_emsort:get_state(Db0#db.id_tree),
    Db1 = bind_id_tree(Db0, Db0#db.fd, DataState),
    Header = db_to_header(Db1, OldHeader),
    CompHeader = #comp_header{
        db_header = Header,
        meta_state = MetaState
    },
    ok = couch_file:sync(Fd),
    ok = couch_file:write_header(Fd, CompHeader),
    Db2 = Db1#db{
        waiting_delayed_commit=nil,
        header=Header,
        committed_update_seq=Db1#db.update_seq
    },
    bind_emsort(Db2, MetaFd, MetaState).


bind_emsort(Db, Fd, nil) ->
    {ok, Ems} = couch_emsort:open(Fd),
    Db#db{id_tree=Ems};
bind_emsort(Db, Fd, State) ->
    {ok, Ems} = couch_emsort:open(Fd, [{root, State}]),
    Db#db{id_tree=Ems}.


bind_id_tree(Db, Fd, State) ->
    {ok, IdBtree} = couch_btree:open(State, Fd, [
        {split, fun ?MODULE:btree_by_id_split/1},
        {join, fun ?MODULE:btree_by_id_join/2},
        {reduce, fun ?MODULE:btree_by_id_reduce/2}
    ]),
    Db#db{id_tree=IdBtree}.


sort_meta_data(Db0) ->
    {ok, Ems} = couch_emsort:merge(Db0#db.id_tree),
    Db0#db{id_tree=Ems}.


copy_meta_data(#db{fd=Fd, header=Header}=Db) ->
    Src = Db#db.id_tree,
    DstState = couch_db_header:id_tree_state(Header),
    {ok, IdTree0} = couch_btree:open(DstState, Fd, [
        {split, fun ?MODULE:btree_by_id_split/1},
        {join, fun ?MODULE:btree_by_id_join/2},
        {reduce, fun ?MODULE:btree_by_id_reduce/2}
    ]),
    {ok, Iter} = couch_emsort:iter(Src),
    Acc0 = #merge_st{
        id_tree=IdTree0,
        seq_tree=Db#db.seq_tree,
        rem_seqs=[],
        infos=[]
    },
    Acc = merge_docids(Iter, Acc0),
    {ok, IdTree} = couch_btree:add(Acc#merge_st.id_tree, Acc#merge_st.infos),
    {ok, SeqTree} = couch_btree:add_remove(
        Acc#merge_st.seq_tree, [], Acc#merge_st.rem_seqs
    ),
    Db#db{id_tree=IdTree, seq_tree=SeqTree}.


merge_docids(Iter, #merge_st{infos=Infos}=Acc) when length(Infos) > 1000 ->
    #merge_st{
        id_tree=IdTree0,
        seq_tree=SeqTree0,
        rem_seqs=RemSeqs
    } = Acc,
    {ok, IdTree1} = couch_btree:add(IdTree0, Infos),
    {ok, SeqTree1} = couch_btree:add_remove(SeqTree0, [], RemSeqs),
    Acc1 = Acc#merge_st{
        id_tree=IdTree1,
        seq_tree=SeqTree1,
        rem_seqs=[],
        infos=[]
    },
    merge_docids(Iter, Acc1);
merge_docids(Iter, #merge_st{curr=Curr}=Acc) ->
    case next_info(Iter, Curr, []) of
        {NextIter, NewCurr, FDI, Seqs} ->
            Acc1 = Acc#merge_st{
                infos = [FDI | Acc#merge_st.infos],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = NewCurr
            },
            merge_docids(NextIter, Acc1);
        {finished, FDI, Seqs} ->
            Acc#merge_st{
                infos = [FDI | Acc#merge_st.infos],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = undefined
            };
        empty ->
            Acc
    end.


next_info(Iter, undefined, []) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, Seq}, FDI}, NextIter} ->
            next_info(NextIter, {Id, Seq, FDI}, []);
        finished ->
            empty
    end;
next_info(Iter, {Id, Seq, FDI}, Seqs) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, NSeq}, NFDI}, NextIter} ->
            next_info(NextIter, {Id, NSeq, NFDI}, [Seq | Seqs]);
        {ok, {{NId, NSeq}, NFDI}, NextIter} ->
            {NextIter, {NId, NSeq, NFDI}, FDI, Seqs};
        finished ->
            {finished, FDI, Seqs}
    end.


update_compact_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
    0 ->
        0;
    _ ->
        (Changes2 * 100) div Total
    end,
    couch_task_status:update([{changes_done, Changes2}, {progress, Progress}]).

