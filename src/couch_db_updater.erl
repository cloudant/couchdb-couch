% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_db_updater).
-behaviour(gen_server).
-vsn(1).

-export([btree_by_id_split/1, btree_by_id_join/2, btree_by_id_reduce/2]).
-export([btree_by_seq_split/1, btree_by_seq_join/2, btree_by_seq_reduce/2]).
-export([make_doc_summary/2]).
-export([init/1,terminate/2,handle_call/3,handle_cast/2,code_change/3,handle_info/2]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_db_int.hrl").

-record(comp_header, {
    db_header,
    meta_state
}).

-record(merge_st, {
    id_tree,
    seq_tree,
    curr,
    rem_seqs,
    infos
}).


init({Engine, DbName, FilePath, Options}) ->
    erlang:put(io_priority, {db_update, DbName}),
    try
        Db = case Engine:init(DbName, FilePath, Options) of
            {ok, EngineState0} ->
                init_db(DbName, Engine, EngineState, Options);
            Error ->
                throw(Error)
        end,
        maybe_track_db(Db),
        % we don't load validation funs here because the fabric query is liable to
        % race conditions.  Instead see couch_db:validate_doc_update, which loads
        % them lazily
        proc_lib:init_ack({ok, Db#db{main_pid = self()}})
    catch
        throw:Error ->
            proc_lib:init_ack(Error)
    end.


terminate(Reason, Db) ->
    {Engine, EngineState} = Db#db.engine,
    Engine:terminate(Reason, EngineState),
    ok.

handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};
handle_call(full_commit, _From, #db{waiting_delayed_commit=nil}=Db) ->
    {reply, ok, Db}; % no data waiting, return ok immediately
handle_call(full_commit, _From,  Db) ->
    {reply, ok, commit_data(Db)};
handle_call({full_commit, RequiredSeq}, _From, Db)
        when RequiredSeq =< Db#db.committed_update_seq ->
    {reply, ok, Db};
handle_call({full_commit, _}, _, Db) ->
    {reply, ok, commit_data(Db)}; % commit the data and return ok
handle_call(start_compact, _From, Db) ->
    {noreply, NewDb} = handle_cast(start_compact, Db),
    {reply, {ok, NewDb#db.compactor_pid}, NewDb};
handle_call(compactor_pid, _From, #db{compactor_pid = Pid} = Db) ->
    {reply, Pid, Db};
handle_call(cancel_compact, _From, #db{compactor_pid = nil} = Db) ->
    {reply, ok, Db};
handle_call(cancel_compact, _From, #db{compactor_pid = Pid} = Db) ->
    unlink(Pid),
    exit(Pid, kill),
    RootDir = config:get("couchdb", "database_dir", "."),
    ok = couch_file:delete(RootDir, Db#db.filepath ++ ".compact"),
    Db2 = Db#db{compactor_pid = nil},
    ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
    {reply, ok, Db2};

handle_call({set_security, NewSec}, _From, #db{} = Db) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    {ok, NewState1} = Engine:store_security(EngineState, NewSec),
    {ok, NewState2} = Engine:increment_update_seq(NewState1),
    {ok, NewState3} = Engine:commit_data(NewState2),
    Db2 = Db#db{
        engine = {Engine, NewState2},
        security = NewSec
    },
    ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
    {reply, ok, Db2};

handle_call({set_revs_limit, Limit}, _From, Db) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    {ok, NewState1} = Engine:set(EngineState, revs_limit, Limit),
    {ok, NewState2} = Engine:increment_update_seq(NewState1),
    {ok, NewState3} = Engine:commit_data(NewState2),
    Db2 = Db#db{
        engine = {Engine, NewState3}
    },
    ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
    {reply, ok, Db2};

handle_call({purge_docs, _IdRevs}, _From,
        #db{compactor_pid=Pid}=Db) when Pid /= nil ->
    {reply, {error, purge_during_compaction}, Db};
handle_call({purge_docs, IdRevs}, _From, Db) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,

    DocIds = [Id || {Id, _Revs} <- IdRevs],
    OldDocInfos = Engine:open_docs(EngineState, DocIds),

    NewDocInfos = lists:flatmap(fun
        ({{_Id, Revs}, #full_doc_info{rev_tree = Tree} = FDI}) ->
            case couch_key_tree:remove_leafs(Tree, Revs) of
                {_, [] = _RemovedRevs} -> % no change
                    [];
                {NewTree, RemovedRevs} ->
                    NewFDI = FDI#full_doc_info{rev_tree = NewTree},
                    [{NewFDI, RemovedRevs}]
            end;
        (_, not_found) ->
            []
    end, lists:zip(IdRevs, OldDocInfos)),

    InitAcc = {Engine:get(EngineState, update_seq), [], [], []},
    FinalAcc = lists:foldl(fun({#full_doc_info{} = OldFDI, RemRevs}, Acc) ->
        #full_doc_info{
            id = Id,
            update_seq = RemSeq,
            rev_tree = OldTree
        } = OldFDI,
        {SeqAcc, FDIAcc, RemSeqAcc, IdRevsAcc} = Acc,

        % Its possible to purge the #leaf{} that contains
        % the update_seq where this doc sits in the update_seq
        % sequence. Rather than do a bunch of complicated checks
        % we just re-label every #leaf{} and reinsert it into
        % the update_seq sequence.
        {NewTree, NewSeqAcc} = couch_key_tree:mapfold(fun
            (_RevId, Leaf, leaf, InnerSeqAcc) ->
                {Leaf#leaf{seq = InnerSeqAcc + 1}, InnerSeqAcc + 1}
            (_RevId, Value, _Type, InnerSeqAcc) ->
                {Value, InnerSeqAcc}
        end, SeqAcc, Tree),

        NewFDI = OldFDI#full_doc_info{
            update_seq = NewSeqAcc,
            rev_tree = NewTree
        },
        
        NewFDIAcc = [NewFDI | FDIAcc],
        NewRemSeqAcc = [RemSeq | RemSeqAcc],
        NewIdRevsAcc = [{Id, RemRevs} | IdRevsAcc],
        {NewSeqAcc, NewFDIAcc, NewRemSeqacc, NewIdRevsAcc}
    end, InitAcc, NewDocInfos)

    {FinalSeq, FDIs, RemSeqs, PurgedIdRevs} = FinalAcc,

    {ok, NewState1} = Engine:set(EngineState, update_seq, FinalSeq + 1)
    {ok, NewState2} = Engine:update_docs(NewState1, FDIs, RemSeqs),
    {ok, NewState3} = Engine:store_purged(NewState2, PurgedIdRevs),
    {ok, NewState4} = Engine:commit_data(NewState3),

    Db2 = Db#db{
        engine = {Engine, NewState4}
    },

    ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
    couch_event:notify(Db#db.name, updated),

    PurgeSeq = Engine:get(NewState4, purge_seq),
    {reply, {ok, PurgeSeq, PurgedIdRevs}, Db2}.


handle_cast({load_validation_funs, ValidationFuns}, Db) ->
    Db2 = Db#db{validate_doc_funs = ValidationFuns},
    ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
    {noreply, Db2};
handle_cast(start_compact, Db) ->
    case Db#db.compactor_pid of
    nil ->
        couch_log:info("Starting compaction for db \"~s\"", [Db#db.name]),
        Pid = spawn_link(fun() -> start_copy_compact(Db) end),
        Db2 = Db#db{compactor_pid=Pid},
        ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
        {noreply, Db2};
    _ ->
        % compact currently running, this is a no-op
        {noreply, Db}
    end;
handle_cast({compact_done, CompactFilepath}, #db{filepath=Filepath}=Db) ->
    {ok, NewFd} = couch_file:open(CompactFilepath),
    {ok, NewHeader0} = couch_file:read_header(NewFd),
    NewHeader = couch_db_header:set(NewHeader0, [
        {compacted_seq, Db#db.update_seq}
    ]),
    #db{update_seq=NewSeq} = NewDb =
        init_db(Db#db.name, Filepath, NewFd, NewHeader, Db#db.options),
    unlink(NewFd),
    case Db#db.update_seq == NewSeq of
    true ->
        % suck up all the local docs into memory and write them to the new db
        {ok, _, LocalDocs} = couch_btree:foldl(Db#db.local_tree,
                fun(Value, _Offset, Acc) -> {ok, [Value | Acc]} end, []),
        {ok, NewLocalBtree} = couch_btree:add(NewDb#db.local_tree, LocalDocs),

        NewDb2 = commit_data(NewDb#db{
            local_tree = NewLocalBtree,
            main_pid = self(),
            filepath = Filepath,
            instance_start_time = Db#db.instance_start_time,
            revs_limit = Db#db.revs_limit
        }),

        couch_log:debug("CouchDB swapping files ~s and ~s.",
                        [Filepath, CompactFilepath]),
        ok = file:rename(CompactFilepath, Filepath ++ ".compact"),
        RootDir = config:get("couchdb", "database_dir", "."),
        couch_file:delete(RootDir, Filepath),
        ok = file:rename(Filepath ++ ".compact", Filepath),
        % Delete the old meta compaction file after promoting
        % the compaction file.
        couch_file:delete(RootDir, Filepath ++ ".compact.meta"),
        close_db(Db),
        NewDb3 = refresh_validate_doc_funs(NewDb2),
        ok = gen_server:call(couch_server, {db_updated, NewDb3}, infinity),
        couch_event:notify(NewDb3#db.name, compacted),
        couch_log:info("Compaction for db \"~s\" completed.", [Db#db.name]),
        {noreply, NewDb3#db{compactor_pid=nil}};
    false ->
        couch_log:info("Compaction file still behind main file "
                       "(update seq=~p. compact update seq=~p). Retrying.",
                       [Db#db.update_seq, NewSeq]),
        close_db(NewDb),
        Pid = spawn_link(fun() -> start_copy_compact(Db) end),
        Db2 = Db#db{compactor_pid=Pid},
        ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
        {noreply, Db2}
    end;

handle_cast(Msg, #db{name = Name} = Db) ->
    couch_log:error("Database `~s` updater received unexpected cast: ~p",
                    [Name, Msg]),
    {stop, Msg, Db}.


handle_info({update_docs, Client, GroupedDocs, NonRepDocs, MergeConflicts,
        FullCommit}, Db) ->
    GroupedDocs2 = sort_and_tag_grouped_docs(Client, GroupedDocs),
    if NonRepDocs == [] ->
        {GroupedDocs3, Clients, FullCommit2} = collect_updates(GroupedDocs2,
                [Client], MergeConflicts, FullCommit);
    true ->
        GroupedDocs3 = GroupedDocs2,
        FullCommit2 = FullCommit,
        Clients = [Client]
    end,
    NonRepDocs2 = [{Client, NRDoc} || NRDoc <- NonRepDocs],
    try update_docs_int(Db, GroupedDocs3, NonRepDocs2, MergeConflicts,
                FullCommit2) of
    {ok, Db2, UpdatedDDocIds} ->
        ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
        case {get_update_seq(Db), get_update_seq(Db2)} of
            {Seq, Seq} -> ok;
            _ -> couch_event:notify(Db2#db.name, updated)
        end,
        [catch(ClientPid ! {done, self()}) || ClientPid <- Clients],
        Db3 = case length(UpdatedDDocIds) > 0 of
            true ->
                % Ken and ddoc_cache are the only things that
                % use the unspecified ddoc_updated message. We
                % should update them to use the new message per
                % ddoc.
                lists:foreach(fun(DDocId) ->
                    couch_event:notify(Db2#db.name, {ddoc_updated, DDocId})
                end, UpdatedDDocIds),
                couch_event:notify(Db2#db.name, ddoc_updated),
                ddoc_cache:evict(Db2#db.name, UpdatedDDocIds),
                refresh_validate_doc_funs(Db2);
            false ->
                Db2
        end,
        {noreply, Db3, hibernate}
    catch
        throw: retry ->
            [catch(ClientPid ! {retry, self()}) || ClientPid <- Clients],
            {noreply, Db, hibernate}
    end;
handle_info(delayed_commit, #db{waiting_delayed_commit=nil}=Db) ->
    %no outstanding delayed commits, ignore
    {noreply, Db};
handle_info(delayed_commit, Db) ->
    case commit_data(Db) of
        Db ->
            {noreply, Db};
        Db2 ->
            ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
            {noreply, Db2}
    end;
handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _Pid, Reason}, Db) ->
    {stop, Reason, Db};
handle_info({'DOWN', Ref, _, _, Reason}, #db{fd_monitor=Ref, name=Name} = Db) ->
    couch_log:error("DB ~s shutting down - Fd ~p", [Name, Reason]),
    {stop, normal, Db#db{fd=undefined, fd_monitor=closed}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

sort_and_tag_grouped_docs(Client, GroupedDocs) ->
    % These groups should already be sorted but sometimes clients misbehave.
    % The merge_updates function will fail and the database can end up with
    % duplicate documents if the incoming groups are not sorted, so as a sanity
    % check we sort them again here. See COUCHDB-2735.
    Cmp = fun([#doc{id=A}|_], [#doc{id=B}|_]) -> A < B end,
    lists:map(fun(DocGroup) ->
        [{Client, maybe_tag_doc(D)} || D <- DocGroup]
    end, lists:sort(Cmp, GroupedDocs)).

maybe_tag_doc(#doc{id=Id, revs={Pos,[_Rev|PrevRevs]}, meta=Meta0}=Doc) ->
    case lists:keymember(ref, 1, Meta0) of
        true ->
            Doc;
        false ->
            Key = {Id, {Pos-1, PrevRevs}},
            Doc#doc{meta=[{ref, Key} | Meta0]}
    end.

merge_updates([[{_,#doc{id=X}}|_]=A|RestA], [[{_,#doc{id=X}}|_]=B|RestB]) ->
    [A++B | merge_updates(RestA, RestB)];
merge_updates([[{_,#doc{id=X}}|_]|_]=A, [[{_,#doc{id=Y}}|_]|_]=B) when X < Y ->
    [hd(A) | merge_updates(tl(A), B)];
merge_updates([[{_,#doc{id=X}}|_]|_]=A, [[{_,#doc{id=Y}}|_]|_]=B) when X > Y ->
    [hd(B) | merge_updates(A, tl(B))];
merge_updates([], RestB) ->
    RestB;
merge_updates(RestA, []) ->
    RestA.

collect_updates(GroupedDocsAcc, ClientsAcc, MergeConflicts, FullCommit) ->
    receive
        % Only collect updates with the same MergeConflicts flag and without
        % local docs. It's easier to just avoid multiple _local doc
        % updaters than deal with their possible conflicts, and local docs
        % writes are relatively rare. Can be optmized later if really needed.
        {update_docs, Client, GroupedDocs, [], MergeConflicts, FullCommit2} ->
            GroupedDocs2 = sort_and_tag_grouped_docs(Client, GroupedDocs),
            GroupedDocsAcc2 =
                merge_updates(GroupedDocsAcc, GroupedDocs2),
            collect_updates(GroupedDocsAcc2, [Client | ClientsAcc],
                    MergeConflicts, (FullCommit or FullCommit2))
    after 0 ->
        {GroupedDocsAcc, ClientsAcc, FullCommit}
    end.


init_db(DbName, Engine, EngineState, Options) ->
    % convert start time tuple to microsecs and store as a binary string
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    StartTime = ?l2b(io_lib:format("~p",
            [(MegaSecs*1000000*1000000) + (Secs*1000000) + MicroSecs])),

    BDU = couch_util:get_value(before_doc_update, Options, nil),
    ADR = couch_util:get_value(after_doc_read, Options, nil),

    #db{
        name = DbName,
        engine = {Engine, EngineState},
        committed_update_seq = Engine:get(EngineState, update_seq),
        update_seq = Engine:get(EngineState, update_seq),
        security = Engine:get(EngineState, security, []),
        instance_start_time = StartTime,
        revs_limit = Engine:get(EngineState, revs_limit, 1000),
        fsync_options = FsyncOptions,
        options = Options,
        compression = couch_compress:get_compression_method(),
        before_doc_update = BDU,
        after_doc_read = ADR
    }.


close_db(#db{fd_monitor = Ref}) ->
    erlang:demonitor(Ref).


refresh_validate_doc_funs(#db{name = <<"shards/", _/binary>> = Name} = Db) ->
    spawn(fabric, reset_validation_funs, [mem3:dbname(Name)]),
    Db#db{validate_doc_funs = undefined};
refresh_validate_doc_funs(Db0) ->
    Db = Db0#db{user_ctx=?ADMIN_USER},
    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    ProcessDocFuns = lists:flatmap(
        fun(DesignDocInfo) ->
            {ok, DesignDoc} = couch_db:open_doc_int(
                Db, DesignDocInfo, [ejson_body]),
            case couch_doc:get_validate_doc_fun(DesignDoc) of
            nil -> [];
            Fun -> [Fun]
            end
        end, DesignDocs),
    Db#db{validate_doc_funs=ProcessDocFuns}.

% rev tree functions

flush_trees(_Db, [], AccFlushedTrees) ->
    {ok, lists:reverse(AccFlushedTrees)};
flush_trees(#db{} = Db,
        [InfoUnflushed | RestUnflushed], AccFlushed) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    #full_doc_info{update_seq=UpdateSeq, rev_tree=Unflushed} = InfoUnflushed,
    {Flushed, FinalAcc} = couch_key_tree:mapfold(
        fun(_Rev, Value, Type, SizesAcc) ->
            case Value of
            #doc{deleted = IsDeleted, body = {summary, _, _, _} = DocSummary} ->
                {summary, Summary, AttSizeInfo, AttsStream} = DocSummary,
                % this node value is actually an unwritten document summary,
                % write to disk.
                % make sure the Fd in the written bins is the same Fd we are
                % and convert bins, removing the FD.
                % All bins should have been written to disk already.
                if AttsStream == nil -> ok; true ->
                    case Engine:is_current_stream(EngineState, AttsStream) of
                        true ->
                            ok;
                        false ->
                            % Stream where the attachments were written to is
                            % no longer the current attachment stream. This
                            % can happen when a database is switched at
                            % compaction time.
                            couch_log:debug("Stream where the attachments are"
                                            " written has changed."
                                            " Possibly retrying.", []),
                            throw(retry)
                    end
                end,
                ExternalSize = ?term_size(Summary),
                {ok, NewState, NewSummaryPointer, SummarySize} =
                    Engine:write_summary(EngineStaet, Summary),
                Leaf = #leaf{
                    deleted = IsDeleted,
                    ptr = NewSummaryPointer,
                    seq = UpdateSeq,
                    sizes = #size_info{
                        active = SummarySize,
                        external = ExternalSize
                    },
                    atts = AttSizeInfo
                },
                {Leaf, add_sizes(Type, Leaf, SizesAcc)};
            #leaf{} ->
                {Value, add_sizes(Type, Value, SizesAcc)};
            _ ->
                {Value, SizesAcc}
            end
        end, {0, 0, []}, Unflushed),
    {FinalAS, FinalES, FinalAtts} = FinalAcc,
    TotalAttSize = lists:foldl(fun({_, S}, A) -> S + A end, 0, FinalAtts),
    NewInfo = InfoUnflushed#full_doc_info{
        rev_tree = Flushed,
        sizes = #size_info{
            active = FinalAS + TotalAttSize,
            external = FinalES + TotalAttSize
        }
    },
    flush_trees(Db, RestUnflushed, [NewInfo | AccFlushed]).

add_sizes(Type, #leaf{sizes=Sizes, atts=AttSizes}, Acc) ->
    % Maybe upgrade from disk_size only
    #size_info{
        active = ActiveSize,
        external = ExternalSize
    } = upgrade_sizes(Sizes),
    {ASAcc, ESAcc, AttsAcc} = Acc,
    NewASAcc = ActiveSize + ASAcc,
    NewESAcc = ESAcc + if Type == leaf -> ExternalSize; true -> 0 end,
    NewAttsAcc = lists:umerge(AttSizes, AttsAcc),
    {NewASAcc, NewESAcc, NewAttsAcc}.

send_result(Client, Doc, NewResult) ->
    % used to send a result to the client
    catch(Client ! {result, self(), {doc_tag(Doc), NewResult}}).

doc_tag(#doc{meta=Meta}) ->
    case lists:keyfind(ref, 1, Meta) of
        {ref, Ref} -> Ref;
        false -> throw(no_doc_tag);
        Else -> throw({invalid_doc_tag, Else})
    end.

merge_rev_trees(_Limit, _Merge, [], [], AccNewInfos, AccRemoveSeqs, AccSeq) ->
    {ok, lists:reverse(AccNewInfos), AccRemoveSeqs, AccSeq};
merge_rev_trees(Limit, MergeConflicts, [NewDocs|RestDocsList],
        [OldDocInfo|RestOldInfo], AccNewInfos, AccRemoveSeqs, AccSeq) ->
    erlang:put(last_id_merged, OldDocInfo#full_doc_info.id), % for debugging
    NewDocInfo0 = lists:foldl(fun({Client, NewDoc}, OldInfoAcc) ->
        merge_rev_tree(OldInfoAcc, NewDoc, Client, Limit, MergeConflicts)
    end, OldDocInfo, NewDocs),
    % When MergeConflicts is false, we updated #full_doc_info.deleted on every
    % iteration of merge_rev_tree. However, merge_rev_tree does not update
    % #full_doc_info.deleted when MergeConflicts is true, since we don't need
    % to know whether the doc is deleted between iterations. Since we still
    % need to know if the doc is deleted after the merge happens, we have to
    % set it here.
    NewDocInfo1 = case MergeConflicts of
        true ->
            NewDocInfo0#full_doc_info{
                deleted = couch_doc:is_deleted(NewDocInfo0)
            };
        false ->
            NewDocInfo0
    end,
    if NewDocInfo1 == OldDocInfo ->
        % nothing changed
        merge_rev_trees(Limit, MergeConflicts, RestDocsList, RestOldInfo,
            AccNewInfos, AccRemoveSeqs, AccSeq);
    true ->
        % We have updated the document, give it a new update_seq. Its
        % important to note that the update_seq on OldDocInfo should
        % be identical to the value on NewDocInfo1.
        OldSeq = OldDocInfo#full_doc_info.update_seq,
        NewDocInfo2 = NewDocInfo1#full_doc_info{
            update_seq = AccSeq + 1
        },
        RemoveSeqs = case OldSeq of
            0 -> AccRemoveSeqs;
            _ -> [OldSeq | AccRemoveSeqs]
        end,
        merge_rev_trees(Limit, MergeConflicts, RestDocsList, RestOldInfo,
            [NewDocInfo2|AccNewInfos], RemoveSeqs, AccSeq+1)
    end.

merge_rev_tree(OldInfo, NewDoc, Client, Limit, false)
        when OldInfo#full_doc_info.deleted ->
    % We're recreating a document that was previously
    % deleted. To check that this is a recreation from
    % the root we assert that the new document has a
    % revision depth of 1 (this is to avoid recreating a
    % doc from a previous internal revision) and is also
    % not deleted. To avoid expanding the revision tree
    % unnecessarily we create a new revision based on
    % the winning deleted revision.

    {RevDepth, _} = NewDoc#doc.revs,
    NewDeleted = NewDoc#doc.deleted,
    case RevDepth == 1 andalso not NewDeleted of
        true ->
            % Update the new doc based on revisions in OldInfo
            #doc_info{revs=[WinningRev | _]} = couch_doc:to_doc_info(OldInfo),
            #rev_info{rev={OldPos, OldRev}} = WinningRev,
            NewRevId = couch_db:new_revid(NewDoc#doc{revs={OldPos, [OldRev]}}),
            NewDoc2 = NewDoc#doc{revs={OldPos + 1, [NewRevId, OldRev]}},

            % Merge our modified new doc into the tree
            #full_doc_info{rev_tree=OldTree} = OldInfo,
            NewTree0 = couch_doc:to_path(NewDoc2),
            case couch_key_tree:merge(OldTree, NewTree0, Limit) of
                {NewTree1, new_leaf} ->
                    % We changed the revision id so inform the caller
                    send_result(Client, NewDoc, {ok, {OldPos+1, NewRevId}}),
                    OldInfo#full_doc_info{
                        rev_tree = NewTree1,
                        deleted = false
                    };
                _ ->
                    throw(doc_recreation_failed)
            end;
        _ ->
            send_result(Client, NewDoc, conflict),
            OldInfo
    end;
merge_rev_tree(OldInfo, NewDoc, Client, Limit, false) ->
    % We're attempting to merge a new revision into an
    % undeleted document. To not be a conflict we require
    % that the merge results in extending a branch.

    OldTree = OldInfo#full_doc_info.rev_tree,
    NewTree0 = couch_doc:to_path(NewDoc),
    NewDeleted = NewDoc#doc.deleted,
    case couch_key_tree:merge(OldTree, NewTree0, Limit) of
        {NewTree, new_leaf} when not NewDeleted ->
            OldInfo#full_doc_info{
                rev_tree = NewTree,
                deleted = false
            };
        {NewTree, new_leaf} when NewDeleted ->
            % We have to check if we just deleted this
            % document completely or if it was a conflict
            % resolution.
            OldInfo#full_doc_info{
                rev_tree = NewTree,
                deleted = couch_doc:is_deleted(NewTree)
            };
        _ ->
            send_result(Client, NewDoc, conflict),
            OldInfo
    end;
merge_rev_tree(OldInfo, NewDoc, _Client, Limit, true) ->
    % We're merging in revisions without caring about
    % conflicts. Most likely this is a replication update.
    OldTree = OldInfo#full_doc_info.rev_tree,
    NewTree0 = couch_doc:to_path(NewDoc),
    {NewTree, _} = couch_key_tree:merge(OldTree, NewTree0, Limit),
    OldInfo#full_doc_info{rev_tree = NewTree}.

stem_full_doc_infos(#db{revs_limit=Limit}, DocInfos) ->
    [Info#full_doc_info{rev_tree=couch_key_tree:stem(Tree, Limit)} ||
            #full_doc_info{rev_tree=Tree}=Info <- DocInfos].

update_docs_int(Db, DocsList, NonRepDocs, MergeConflicts, FullCommit) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,

    UpdateSeq = Engine:get(EngineState, update_seq),
    RevsLimit = Engine:get(EngineState, revs_limit),

    Ids = [Id || [{_Client, #doc{id=Id}}|_] <- DocsList],
    % lookup up the old documents, if they exist.
    OldDocLookups = Engine:open_docs(EngineState, Ids),
    OldDocInfos = lists:zipwith(fun
        (_Id, #full_doc_info{} = FDI}) ->
            FDI;
        (Id, not_found) ->
            #full_doc_info{id=Id}
    end, Ids, OldDocLookups),
    % Merge the new docs into the revision trees.
    {ok, NewFullDocInfos, RemoveSeqs, NewSeq} = merge_rev_trees(RevsLimit,
            MergeConflicts, DocsList, OldDocInfos, [], [], LastSeq),

    % Write out the document summaries (the bodies are stored in the nodes of
    % the trees, the attachments are already written to disk)
    {ok, IndexFDIs} = flush_trees(Db2, NewFullDocInfos, []),

    NewNonRepDocs = update_local_doc_revs(NonRepDocs),

    {ok, NewState1} = Engine:write_doc_info(
            EngineState, IndexFDIs, RemSeqs, NewNonRepDocs),
    {ok, NewState2} = Engine:set(NewState1, update_seq, NewSeq),

    Db2 = Db#db{
        engine = {Engine, NewState2}
    },

    WriteCount = length(IndexFullDocInfos),
    couch_stats:increment_counter([couchdb, document_inserts],
         WriteCount - length(RemoveSeqs)),
    couch_stats:increment_counter([couchdb, document_writes], WriteCount),
    couch_stats:increment_counter(
        [couchdb, local_document_writes],
        length(NonRepDocs)
    ),

    % Check if we just updated any design documents, and update the validation
    % funs if we did.
    UpdatedDDocIds = lists:flatmap(fun
        (<<"_design/", _/binary>> = Id) -> [Id];
        (_) -> []
    end, Ids),

    Db3 = case length(UpdatedDDocIds) > 0 of
        true ->
            couch_event:notify(Db3#db.name, ddoc_updated),
            ddoc_cache:evict(Db3#db.name, UpdatedDDocIds),
            refresh_validate_doc_funs(Db2);
        false ->
            Db2
    end,

    {ok, commit_data(Db3, not FullCommit), UpdatedDDocIds}.


update_local_doc_revs(Docs) ->
    lists:map(fun({Client, NewDoc}) ->
        #doc{
            deleted = Delete,
            revs = {0, PrevRevs}
        } = NewDoc,
        case PrevRevs of
            [RevStr | _] ->
                PrevRev = list_to_integer(?b2l(RevStr));
            [] ->
                PrevRev = 0
        end,
        NewRev = case Delete of
            false ->
                {0, ?l2b(integer_to_list(PrevRev + 1))};
            true  ->
                {0, <<"0">>}
        end,
        send_result(Client, NewDoc, {ok, NewRev}),
        NewDoc#doc{
            revs = {0, [NewRev]}
        }
    end, Docs).


commit_data(Db) ->
    commit_data(Db, false).

commit_data(#db{waiting_delayed_commit = nil} = Db, true) ->
    TRef = erlang:send_after(1000, self(), delayed_commit),
    Db#db{waiting_delayed_commit = TRef};
commit_data(Db, true) ->
    Db;
commit_data(Db, _) ->
    #db{
        engine = {Engine, EngineState},
        waiting_delayed_commit = Timer
    } = Db,
    if is_reference(Timer) -> erlang:cancel_timer(Timer); true -> ok end,
    NewState = Engine:commit_data(EngineState),
    Db#db{
        engine = {Engine, NewState},
        waiting_delayed_commit = nil,
        committed_update_seq = Engine:get(NewState, update_seq),
    }.


maybe_track_db(#db{options = Options}) ->
    case lists:member(sys_db, Options) of
        true ->
            ok;
        false ->
            couch_stats_process_tracker:track([couchdb, open_databases]);
    end.


get_update_seq(Db) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    Engine:get(EngineState, update_seq).


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


make_doc_summary(#db{compression = Comp}, {Body0, Atts0}) ->
    Body = case couch_compress:is_compressed(Body0, Comp) of
    true ->
        Body0;
    false ->
        % pre 1.2 database file format
        couch_compress:compress(Body0, Comp)
    end,
    Atts = case couch_compress:is_compressed(Atts0, Comp) of
    true ->
        Atts0;
    false ->
        couch_compress:compress(Atts0, Comp)
    end,
    SummaryBin = ?term_to_bin({Body, Atts}),
    couch_file:assemble_file_chunk(SummaryBin, couch_crypto:hash(md5, SummaryBin)).
