-module(couch_db_engine).


-include("couch_db.hrl").


-type filepath() -> iolist().
-type docid() -> binary().
-type rev() -> {non_negative_integer(), binary()}.
-type revs() -> [rev()].

-type db_open_option() -> create.
-type db_open_options() -> [db_open_options()].

-type db_handle() -> any().

-type doc_fold_fun() -> fun(#full_doc_info{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}.

-type local_doc_fold_fun() -> fun(#doc{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}.

-type changes_fold_fun() -> fun(#doc_info{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}.


-callback exists(DbPath::filepath()) -> boolean.


-callback delete(RootDir::filepath(), DbPath::filepath(), Async::boolean()) ->
        ok.


-callback delete_compaction_files(RootDir::filepath(), DbPath::filepath()) ->
        ok.


-callback init(DbPath::filepath(), db_open_options()) ->
    {ok, DbHandle::db_handle()}.


-callback terminate(Reason::any(), DbHandle::db_handle()) -> Ignored::any().


-callback handle_info(Msg::any(), DbHandle::db_handle()) ->
    {noreply, NewDbHandle:db_handle()} |
    {noreply, NewDbHandle:db_handle(), Timeout::timeout()} |
    {stop, Reason::any(), NewDbHandle::db_handle()}.


-callback incref(DbHandle::db_handle()) -> {ok, NewDbHandle::db_handle()}.
-callback decref(DbHandle::db_handle()) -> ok.
-callback monitored_by(DbHande::db_handle()) -> [pid()].


-callback get(DbHandle::db_handle(), Property::atom()) -> Value::any().
-callback get(DbHandle::db_handle(), Property::atom(), Default::any()) ->
        Value::any().


-callback set(DbHandle::db_handle(), Property::atom(), Value::any()) ->
        {ok, NewDbHandle::db_handle()}.


-callback open_docs(DbHandle::db_handle(), DocIds::[docid()]) ->
        [#full_doc_info{} | not_found].


-callabck open_local_docs(DbHandle::db_handle(), DocIds::[docid()]) ->
        [#doc{} | not_found].


-callback read_doc(DbHandle::db_handle(), DocPtr::any()) ->
        {ok, DocParts::any()}.


-callback make_doc_summary(DbHandle::db_handle(), DocParts::any()) ->
        binary().


-callback write_doc_summary(DbHandle::db_handle(), DocSummary::binary()) ->
        {ok, DocPtr::any(), SummarySize::non_neg_integer()}.


-callback write_doc_infos(
    DbHandle::db_handle(),
    Pairs::doc_pairs(),
    RemSeqs::[non_neg_integer()],
    LocalDocs::[#doc{}]) ->
        {ok, NewDbHandle::db_handle()}.


-callback commit_data(DbHandle::db_handle()) ->
        {ok, NewDbHande::db_handle()}.


-callback open_write_stream(
    DbHandle::db_handle(),
    Options::write_stream_options()) ->
        {ok, pid()}.


-callback open_read_stream(DbHandle::db_handle(), StreamDiskInfo::any()) ->
        {Module::atom(), ReadStreamState::any()}.


-callback is_active_stream(DbHandle::db_handle(), ReadStreamState::any()) ->
        boolean().


-callback fold_docs(
    DbHandle::db_handle(),
    UserFold::doc_fold_fun(),
    UserAcc::any(),
    doc_fold_options()) ->
        {ok, LastUserAcc::any()}.


-callback fold_local_docs(
    DbHandle::db_handle(),
    UserFold::local_doc_fold_fun(),
    UserAcc::any(),
    local_doc_fold_options()) ->
        {ok, LastUserAcc::any()}.


-callback fold_changes(
    DbHandle::db_handle(),
    StartSeq::non_negative_integer(),
    UserFold::changes_fold_fun(),
    UserAcc::any(),
    changes_fold_options()) ->
        {ok, LastUserAcc::any()}.


-callback count_changes_since(
    DbHandle::db_handle(),
    UpdateSeq::non_negative_integer()) ->
        TotalChanges::non_negative_integer().


-callback start_compaction(
    DbHandle::db_handle(),
    DbName::binary(),
    Options::compaction_options(),
    Parent::pid()) ->
        CompactorPid::pid().


-callback finish_compaction(
    OldDbHandle::db_handle(),
    NewDbHandle::db_handle()) ->
        {ok, CompactedDbHandle::db_handle()}.


-include("couch_db_int.hrl").


-export([
    exists/2,
    delete/4,
    delete_compaction_files/3,

    init/3,
    terminate/2,
    handle_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

    get/2,
    get/3,
    set/3,

    open_docs/2,
    open_local_docs/2,
    read_doc/2,

    make_doc_summary/2,
    write_doc_summary/2,
    write_doc_infos/4,
    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,

    start_compaction/1,
    finish_compaction/2
]).


exists(Engine, DbPath) ->
    Engine:exists(DbPath).


delete(Engine, RootDir, DbPath, Async) ->
    Engine:delete(RootDir, DbPath, Async).


delete_compaction_files(Engine, RootDir, Dbpath) ->
    Engine:delete_compaction_files(RootDir, DbPath).


init(Engine, DbPath, Options) ->
    case Engine:init(DbPath, Options) of
         {ok, EngineState} ->
             {ok, {Engine, EngineState}};
         Error ->
             throw(Error)
    end.


terminate(Reason, #db{} = Db) ->
    invoke(Db, terminate, []).


handle_info(Msg, #db{} = Db) ->
    #db{
        name = Name,
        engine = {Engine, EngineState}
    } = Db,
    case Engine:handle_info(Msg, EngineState) of
        {noreply, NewState} ->
            {noreply, Db#db{engine = {Engine, NewState}}};
        {noreply, NewState, Timeout} ->
            {noreply, Db#db{engine = {Engine, NewState}}, Timeout};
        {stop, Reason, NewState} ->
            couch_log:error("DB ~s shutting down: ~p", [Name, Msg]),
            {stop, Reason, Db#db{engine = {Engine, NewState}}}
    end.


incref(#db{} = Db) ->
    invoke_update(Db, incref, []).


decref(#db{} = Db) ->
    invoke(Db, decref, []).


monitored_by(#db{} = Db) ->
    invoke(Db, monitored_by, []).


get(#db{} = Db, Property) ->
    get(Db, Property, undefined).


get(#db{} = Db, engine, _) ->
    #db{
        engine = {Engine, _}
    } = Db,
    Engine;

get(#db{} = Db, Property, Default) ->
    invoke(Db, get, [Property, Default]).


set(#db{} = Db, Property, Value) ->
    invoke_update(Db, set, [Property, Value]).


open_docs(#db{} = Db, DocIds) ->
    invoke(Db, open_docs, [DocIds]).


open_local_docs(#db{} = Db, DocIds) ->
    invoke(Db, open_local_docs, [DocIds]).


read_doc(#db{} = Db, DocPtr) ->
    invoke(Db, read_doc, [DocPtr]).


make_doc_summary(#db{} = Db, DocParts) ->
    invoke(Db, make_doc_summary, [DocParts]).


write_doc_summary(#db{} = Db, DocSummary) ->
    invoke(Db, write_doc_summary, [DocSummary]).


write_doc_infos(#db{} = Db, DocUpdates, LocalDocs, PurgedDocIdRevs) ->
    invoke_update(Db, write_docs, [DocUpdates, LocalDocs, PurgedDocIdsRevs]).


commit_data(#db{} = Db) ->
    invoke_update(Db, commit_data, []).


open_write_stream(#db{} = Db, Options) ->
    invoke(Db, open_write_stream, [Options]).


open_read_stream(#db{} = Db, StreamDiskInfo) ->
    invoke(Db, open_read_stream, [StreamDiskInfo]).


is_active_stream(#db{} = Db, ReadStreamState) ->
    invoke(Db, is_active_stream, [ReadStreamState]).


fold_docs(#db{} = Db, UserFun, UserAcc, Options) ->
    invoke(Db, fold_docs, UserFun, UserAcc, Options).


fold_local_docs(#db{} = Db, UserFun, UserAcc, Options) ->
    invoke(Db, fold_local_docs, UserFun, UserAcc, Options).


fold_changes(#db{} = Db, StartSeq, UserFun, UserAcc, Options) ->
    invoke(Db, fold_changes, StartSeq, UserFun, UserAcc, Options).


count_changes_since(#db{} = Db, StartSeq) ->
    invoke(Db, count_changes_since, StartSeq).


start_compaction(#db{} = Db, DbName, Options, Parent) ->
    #db{
        name = DbName,
        options = Options
    } = Db,
    CompactorPid = invoke(Db, start_compaction, DbName, Options, self()).
    {ok, Db#db{
        compactor_pid = Pid
    }}.


finish_compaction(OldDb, CompactFilePath) ->
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
                compactor_pid = nil
            },
            % Why do we refresh validation functions here?
            %NewDb2 = refresh_validate_doc_funs(NewDb1),
            NewDb2 = NewDb1,
            ok = gen_server:call(couch_server, {db_updated, NewDb2}, infinity),
            couch_event:notify(NewDb2#db.name, compacted),
            Arg = [NewDb2#db.name],
            couch_log:info("Compaction for db \"~s\" completed.", Arg),
            NewDb2;
        false ->
            ok = Engine:decref(NewState1),
            NewDb = OldDb#db{
                compactor_pid = Engine:start_compaction(
                        OldState, OldDb#db.name, OldDb#db.options, self())
            },
            couch_log:info("Compaction file still behind main file "
                           "(update seq=~p. compact update seq=~p). Retrying.",
                           [OldSeq, NewSeq]),
            ok = gen_server:call(couch_server, {db_updated, NewDb}, infinity),
            NewDb
    end.


invoke(Db, Fun) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    Engine:Fun(EngineState).


invoke(Db, Fun, Arg1) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    Engine:Fun(EngineState, Arg1).


invoke(Db, Fun, Arg1, Arg2) ->
    #db{
        engine = {Engine, EngineState}
    },
    Engine:Fun(EngineState, Arg1, Arg2).


invoke_update(Db, Fun) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    {ok, NewEngineState} = Engine:Fun(EngineState),
    {ok, Db#db{
        engine = {Engine, NewEngineState}
    }}.


invoke_update(Db, Fun, Arg1) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    {ok, NewEngineState} = Engine:Fun(EngineState, Arg1),
    {ok, Db#db{
        engine = {Engine, NewEngineState}
    }}.


invoke_update(Db, Fun, Arg1, Arg2, Arg3) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    {ok, NewEngineState} =
            erlang:apply(Engine, Fun, EngineState, Arg1, Arg2, Arg3),
    {ok, Db#db{
        engine = {Engine, NewEngineState}
    }}.
