-module(couch_db_engine).


-include("couch_db.hrl").


-type filepath() :: iolist().
-type docid() :: binary().
-type rev() :: {non_neg_integer(), binary()}.
-type revs() :: [rev()].

-type doc_pair() :: {
        #full_doc_info{} | not_found,
        #full_doc_info{} | not_found
    }.

-type doc_pairs() :: [doc_pair()].

-type db_open_options() :: [
        create
    ].

-type write_stream_option() :: compressed. % Need to enumerate
-type write_stream_options() :: [write_stream_option()].

-type doc_fold_options() :: [
        {start_key, Key::any()} |
        {end_key, Key::any()} |
        {end_key_gt, Key::any()} |
        {dir, fwd | rev}
    ].

-type changes_fold_options() :: [
        % Need to enumerate these
    ].

-type db_handle() :: any().

-type doc_fold_fun() :: fun((#full_doc_info{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}).

-type local_doc_fold_fun() :: fun((#doc{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}).

-type changes_fold_fun() :: fun((#doc_info{}, UserAcc::any()) ->
        {ok, NewUserAcc::any()} |
        {stop, NewUserAcc::any()}).


-callback exists(DbPath::filepath()) -> boolean.


-callback delete(RootDir::filepath(), DbPath::filepath(), Async::boolean()) ->
        ok.


-callback delete_compaction_files(RootDir::filepath(), DbPath::filepath()) ->
        ok.


-callback init(DbPath::filepath(), db_open_options()) ->
    {ok, DbHandle::db_handle()}.


-callback terminate(Reason::any(), DbHandle::db_handle()) -> Ignored::any().


-callback handle_call(Msg::any(), DbHandle::db_handle()) ->
        {reply, Resp::any(), NewDbHandle::db_handle()} |
        {stop, Reason::any(), Resp::any(), NewDbHandle::db_handle()}.


-callback handle_info(Msg::any(), DbHandle::db_handle()) ->
    {noreply, NewDbHandle::db_handle()} |
    {noreply, NewDbHandle::db_handle(), Timeout::timeout()} |
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


-callback open_local_docs(DbHandle::db_handle(), DocIds::[docid()]) ->
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
    LocalDocs::[#doc{}],
    PurgedDocIdRevs::[{docid(), revs()}]) ->
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
    doc_fold_options()) ->
        {ok, LastUserAcc::any()}.


-callback fold_changes(
    DbHandle::db_handle(),
    StartSeq::non_neg_integer(),
    UserFold::changes_fold_fun(),
    UserAcc::any(),
    changes_fold_options()) ->
        {ok, LastUserAcc::any()}.


-callback count_changes_since(
    DbHandle::db_handle(),
    UpdateSeq::non_neg_integer()) ->
        TotalChanges::non_neg_integer().


-callback start_compaction(
    DbHandle::db_handle(),
    DbName::binary(),
    Options::db_open_options(),
    Parent::pid()) ->
        CompactorPid::pid().


-callback finish_compaction(
    OldDbHandle::db_handle(),
    DbName::binary(),
    Options::db_open_options(),
    Parent::pid()) ->
        {ok, CompactedDbHandle::db_handle()}.


-include("couch_db_int.hrl").


-export([
    exists/2,
    delete/4,
    delete_compaction_files/3,

    init/3,
    terminate/2,
    handle_call/3,
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


delete_compaction_files(Engine, RootDir, DbPath) ->
    Engine:delete_compaction_files(RootDir, DbPath).


init(Engine, DbPath, Options) ->
    case Engine:init(DbPath, Options) of
         {ok, EngineState} ->
             {ok, {Engine, EngineState}};
         Error ->
             throw(Error)
    end.


terminate(Reason, #db{} = Db) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:terminate(EngineState, Reason).


handle_call(Msg, _From, #db{} = Db) ->
    #db{
        engine = {Engine, EngineState}
    } = Db,
    case Engine:handle_call(Msg, EngineState) of
        {reply, Resp, NewState} ->
            {reply, Resp, Db#db{engine = {Engine, NewState}}};
        {stop, Reason, Resp, NewState} ->
            {stop, Reason, Resp, Db#db{engine = {Engine, NewState}}}
    end.


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
    #db{engine = {Engine, EngineState}} = Db,
    {ok, NewState} = Engine:incref(EngineState),
    {ok, Db#db{engine = {Engine, NewState}}}.


decref(#db{} = Db) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:decref(EngineState).


monitored_by(#db{} = Db) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:monitored_by(EngineState).


get(#db{} = Db, Property) ->
    get(Db, Property, undefined).


get(#db{} = Db, engine, _) ->
    #db{engine = {Engine, _}} = Db,
    Engine;

get(#db{} = Db, Property, Default) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:get(EngineState, Property, Default).


set(#db{} = Db, Property, Value) ->
    #db{engine = {Engine, EngineState}} = Db,
    {ok, NewSt} = Engine:set(EngineState, Property, Value),
    {ok, Db#db{engine = {Engine, NewSt}}}.


open_docs(#db{} = Db, DocIds) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:open_docs(EngineState, DocIds).


open_local_docs(#db{} = Db, DocIds) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:open_local_docs(EngineState, DocIds).


read_doc(#db{} = Db, DocPtr) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:read_doc(EngineState, DocPtr).


make_doc_summary(#db{} = Db, DocParts) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:make_doc_summary(EngineState, DocParts).


write_doc_summary(#db{} = Db, DocSummary) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:write_doc_summary(EngineState, DocSummary).


write_doc_infos(#db{} = Db, DocUpdates, LocalDocs, PurgedDocIdRevs) ->
    #db{engine = {Engine, EngineState}} = Db,
    {ok, NewSt} = Engine:write_docs(
            EngineState, DocUpdates, LocalDocs, PurgedDocIdRevs),
    {ok, Db#db{engine = {Engine, NewSt}}}.


commit_data(#db{} = Db) ->
    #db{engine = {Engine, EngineState}} = Db,
    {ok, NewSt} = Engine:commit_data(EngineState),
    {ok, Db#db{engine = {Engine, NewSt}}}.


open_write_stream(#db{} = Db, Options) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:open_write_stream(EngineState, Options).


open_read_stream(#db{} = Db, StreamDiskInfo) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:open_read_stream(EngineState, StreamDiskInfo).


is_active_stream(#db{} = Db, ReadStreamState) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:is_active_stream(EngineState, ReadStreamState).


fold_docs(#db{} = Db, UserFun, UserAcc, Options) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:fold_docs(EngineState, UserFun, UserAcc, Options).


fold_local_docs(#db{} = Db, UserFun, UserAcc, Options) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:fold_local_docs(EngineState, UserFun, UserAcc, Options).


fold_changes(#db{} = Db, StartSeq, UserFun, UserAcc, Options) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:fold_changes(EngineState, StartSeq, UserFun, UserAcc, Options).


count_changes_since(#db{} = Db, StartSeq) ->
    #db{engine = {Engine, EngineState}} = Db,
    Engine:count_changes_since(EngineState, StartSeq).


start_compaction(#db{} = Db) ->
    #db{
        engine = {Engine, EngineState},
        name = DbName,
        options = Options
    } = Db,
    {ok, NewEngineState, Pid} = Engine:start_compaction(
            EngineState, DbName, Options, self()),
    {ok, Db#db{
        engine = {Engine, NewEngineState},
        compactor_pid = Pid
    }}.


finish_compaction(Db, CompactInfo) ->
    #db{
        engine = {Engine, St},
        name = DbName,
        options = Options
    } = Db,
    NewDb = case Engine:finish_compaction(St, DbName, Options, CompactInfo) of
        {ok, NewState, undefined} ->
            couch_event:notify(DbName, compacted),
            Db#db{
                engine = {Engine, NewState},
                compactor_pid = nil
            };
        {ok, NewState, CompactorPid} when is_pid(CompactorPid) ->
            Db#db{
                engine = {Engine, NewState},
                compactor_pid = CompactorPid
            }
    end,
    ok = gen_server:call(couch_server, {db_updated, NewDb}, infinity),
    {ok, NewDb}.
