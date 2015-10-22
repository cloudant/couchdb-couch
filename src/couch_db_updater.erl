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
    couch_server:clear_compaction_data(Db#db.name),
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
            % For now we only support compacting to the same
            % storage engine. After the first round of patches
            % we'll add a field that sets the target engine
            % type to compact to with a new copy compactor.
            couch_log:info("Starting compaction for db \"~s\"", [Db#db.name]),
            {Engine, EngineState} = Db#db.engine,
            Pid = Engine:start_compaction(
                    EngineState, Db#db.name, Db#db.options, self()),
            Db2 = Db#db{compactor_pid = Pid},
            ok = gen_server:call(couch_server, {db_updated, Db2}, infinity),
            {noreply, Db2};
        _ ->
            % compact currently running, this is a no-op
            {noreply, Db}
    end;
handle_cast({compact_done, CompactEngine, CompactFilePath}, #db{} = OldDb) ->
    NewDb = case Db#db.engine of
        {CompactEngine, _} ->
            finish_engine_compaction(OldDb, CompactFilePath);
        {_DifferentEngine, _} ->
            finish_engine_swap(OldDb, CompactEngine, CompactFilePath)
    end,
    {noreply, NewDb};

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

    {ok, SecProps} = Engine:get_security(EngineState),

    BDU = couch_util:get_value(before_doc_update, Options, nil),
    ADR = couch_util:get_value(after_doc_read, Options, nil),

    #db{
        name = DbName,
        engine = {Engine, EngineState},
        committed_update_seq = Engine:get(EngineState, update_seq),
        security = SecProps,
        instance_start_time = StartTime,
        options = Options,
        before_doc_update = BDU,
        after_doc_read = ADR
    }.


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
                    case couch_db:is_active_stream(Db, AttsStream) of
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
                    Engine:write_summary(EngineState, Summary),
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


finish_engine_compaction(OldDb, CompactFilePath)
    #db{
        engine = {Engine, OldState}
    } = OldDb,
    {ok, NewState1} = Engine:init(CompactFilePath, OldDb#db.options),
    OldSeq = Engine:get(OldState, update_seq),
    NewSeq = Engine:get(NewState1, update_seq),
    case OldSeq == NewSeq of
        true ->
            {ok, NewState2} = Engine:finish_compaction(OldState, NewState1),
            NewDb1 = OldDb#db{
                engine = {Engine, NewState2},
                compaction_pid = nil
            }
            NewDb2 = refresh_validate_doc_funs(NewDb1),
            ok = gen_server:call(couch_server, {db_updated, NewDb2}, infinity),
            couch_event:notify(NewDb2#db.name, compacted),
            Arg = [NewDb2#db.name],
            couch_log:info("Compaction for db \"~s\" completed.", Arg),
            NewDb2;
        false ->
            ok = Engine:close(NewState1),
            NewDb = OldDb#db{
                compaction_pid = Engine:start_compaction(
                        OldState, OldDb#db.name, OldDb#db.options, self())
            },
            couch_log:info("Compaction file still behind main file "
                           "(update seq=~p. compact update seq=~p). Retrying.",
                           [OldSeq, NewSeq]),
            ok = gen_server:call(couch_server, {db_updated, NewDb}, infinity),
            NewDb
    end.


finish_engine_swap(OldDb, NewEngine, CompactFilePath) ->
    erlang:error(explode).


make_doc_summary(Db, DocParts) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    Engine:make_doc_summary(EngineState, DocParts).
