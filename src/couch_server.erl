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

-module(couch_server).
-behaviour(gen_server).
-behaviour(config_listener).
-vsn(2).

-export([open/2,create/2,delete/2,get_version/0,get_version/1,get_uuid/0]).
-export([all_databases/0, all_databases/2]).
-export([init/1, handle_call/3,sup_start_link/0]).
-export([handle_cast/2,code_change/3,handle_info/2,terminate/2]).
-export([dev_start/0,is_admin/2,has_admins/0,get_stats/0]).
-export([close_lru/0]).
-export([delete_compaction_files/1]).

% config_listener api
-export([handle_config_change/5, handle_config_terminate/3]).

-export([delete_file/3]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_server_int.hrl").

-define(MAX_DBS_OPEN, 100).

-record(server,{
    root_dir = [],
    engines = [],
    max_dbs_open=?MAX_DBS_OPEN,
    dbs_open=0,
    start_time="",
    update_lru_on_read=true,
    lru = couch_lru:new()
    }).

dev_start() ->
    couch:stop(),
    up_to_date = make:all([load, debug_info]),
    couch:start().

get_version() ->
    case application:get_key(couch, vsn) of
        {ok, Version} -> Version;
        undefined -> "0.0.0"
    end.
get_version(short) ->
  %% strip git hash from version string
  [Version|_Rest] = string:tokens(get_version(), "+"),
  Version.


get_uuid() ->
    case config:get("couchdb", "uuid", undefined) of
        undefined ->
            UUID = couch_uuids:random(),
            config:set("couchdb", "uuid", ?b2l(UUID)),
            UUID;
        UUID -> ?l2b(UUID)
    end.

get_stats() ->
    {ok, #server{start_time=Time,dbs_open=Open}} =
            gen_server:call(couch_server, get_server),
    [{start_time, ?l2b(Time)}, {dbs_open, Open}].

sup_start_link() ->
    gen_server:start_link({local, couch_server}, couch_server, [], []).


open(DbName, Options0) when is_list(DbName) ->
    open(iolist_to_binary(DbName), Options0);
open(DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
    case ets:lookup(couch_dbs, DbName) of
    [#srv_entry{db=Db, lock=Lock}] when Lock =/= locked ->
        update_lru(DbName, Options),
        {ok, NewDb} = couch_db:incref(Db),
        couch_db:set_user_ctx(NewDb, Ctx);
    _ ->
        Timeout = couch_util:get_value(timeout, Options, infinity),
        case gen_server:call(couch_server, {open, DbName, Options}, Timeout) of
        {ok, Db} ->
            update_lru(DbName, Options),
            {ok, NewDb} = couch_db:incref(Db),
            couch_db:set_user_ctx(NewDb, Ctx);
        Error ->
            Error
        end
    end.

update_lru(DbName, Options) ->
    case lists:member(sys_db, Options) of
        false -> gen_server:cast(couch_server, {update_lru, DbName});
        true -> ok
    end.

close_lru() ->
    gen_server:call(couch_server, close_lru).

create(DbName, Options0) when is_list(DbName) ->
    create(iolist_to_binary(DbName), Options0);
create(DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    case gen_server:call(couch_server, {create, DbName, Options}, infinity) of
    {ok, Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, NewDb} = couch_db:incref(Db),
        couch_db:set_user_ctx(NewDb, Ctx);
    Error ->
        Error
    end.

delete(DbName, Options) when is_list(DbName) ->
    delete(iolist_to_binary(DbName), Options);
delete(DbName, Options) ->
    gen_server:call(couch_server, {delete, DbName, Options}, infinity).

delete_compaction_files(DbName) when is_list(DbName) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    lists:foreach(fun({Ext, Module}) ->
        FilePath = make_filepath(RootDir, DbName, Ext),
        Module:delete_compaction_files(RootDir, FilePath)
    end, get_configured_engines()),
    ok;
delete_compaction_files(DbName) when is_binary(DbName) ->
    delete_compaction_files(?b2l(DbName)).

maybe_add_sys_db_callbacks(DbName, Options) when is_binary(DbName) ->
    maybe_add_sys_db_callbacks(?b2l(DbName), Options);
maybe_add_sys_db_callbacks(DbName, Options) ->
    DbsDbName = config:get("mem3", "shards_db", "_dbs"),
    NodesDbName = config:get("mem3", "nodes_db", "_nodes"),
    IsReplicatorDb = DbName == config:get("replicator", "db", "_replicator") orelse
	path_ends_with(DbName, <<"_replicator">>),
    IsUsersDb = DbName ==config:get("couch_httpd_auth", "authentication_db", "_users") orelse
	path_ends_with(DbName, <<"_users">>),
    if
	DbName == DbsDbName ->
	    [sys_db | Options];
	DbName == NodesDbName ->
	    [sys_db | Options];
	IsReplicatorDb ->
	    [{before_doc_update, fun couch_replicator_manager:before_doc_update/2},
	     {after_doc_read, fun couch_replicator_manager:after_doc_read/2},
	     sys_db | Options];
	IsUsersDb ->
	    [{before_doc_update, fun couch_users_db:before_doc_update/2},
	     {after_doc_read, fun couch_users_db:after_doc_read/2},
	     sys_db | Options];
	true ->
	    Options
    end.

path_ends_with(Path, Suffix) ->
    Suffix == couch_db:normalize_dbname(Path).

check_dbname(#server{}, DbName) ->
    couch_db:validate_dbname(DbName).

is_admin(User, ClearPwd) ->
    case config:get("admins", User) of
    "-hashed-" ++ HashedPwdAndSalt ->
        [HashedPwd, Salt] = string:tokens(HashedPwdAndSalt, ","),
        couch_util:to_hex(couch_crypto:hash(sha, ClearPwd ++ Salt)) == HashedPwd;
    _Else ->
        false
    end.

has_admins() ->
    config:get("admins") /= [].

hash_admin_passwords() ->
    hash_admin_passwords(true).

hash_admin_passwords(Persist) ->
    lists:foreach(
        fun({User, ClearPassword}) ->
            HashedPassword = couch_passwords:hash_admin_password(ClearPassword),
            config:set("admins", User, ?b2l(HashedPassword), Persist)
        end, couch_passwords:get_unhashed_admins()).

init([]) ->
    % read config and register for configuration changes

    % just stop if one of the config settings change. couch_server_sup
    % will restart us and then we will pick up the new settings.

    RootDir = config:get("couchdb", "database_dir", "."),
    Engines = get_configured_engines(),
    MaxDbsOpen = list_to_integer(
            config:get("couchdb", "max_dbs_open", integer_to_list(?MAX_DBS_OPEN))),
    UpdateLruOnRead =
        config:get("couchdb", "update_lru_on_read", "true") =:= "true",
    ok = config:listen_for_changes(?MODULE, nil),
    ok = couch_file:init_delete_dir(RootDir),
    hash_admin_passwords(),
    EtsOpts = [set, protected, named_table, {keypos, #srv_entry.name}],
    ets:new(couch_dbs, EtsOpts),
    ets:new(couch_dbs_pid_to_name, [set, protected, named_table]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                engines = Engines,
                max_dbs_open=MaxDbsOpen,
                update_lru_on_read=UpdateLruOnRead,
                start_time=couch_util:rfc1123_date()}}.

terminate(Reason, Srv) ->
    couch_log:error("couch_server terminating with ~p, state ~2048p",
                    [Reason,
                     Srv#server{lru = redacted}]),
    ets:foldl(fun(#srv_entry{db=Db}, _) ->
        couch_db:shutdown(Db)
    end, nil, couch_dbs),
    ok.

handle_config_change("couchdb", "database_dir", _, _, _) ->
    exit(whereis(couch_server), config_change),
    remove_handler;
handle_config_change("couchdb", "update_lru_on_read", "true", _, _) ->
    {ok, gen_server:call(couch_server,{set_update_lru_on_read,true})};
handle_config_change("couchdb", "update_lru_on_read", _, _, _) ->
    {ok, gen_server:call(couch_server,{set_update_lru_on_read,false})};
handle_config_change("couchdb", "max_dbs_open", Max, _, _) when is_list(Max) ->
    {ok, gen_server:call(couch_server,{set_max_dbs_open,list_to_integer(Max)})};
handle_config_change("couchdb", "max_dbs_open", _, _, _) ->
    {ok, gen_server:call(couch_server,{set_max_dbs_open,?MAX_DBS_OPEN})};
handle_config_change("couchdb_engines", _, _, _, _) ->
    {ok, gen_server:call(couch_server,reload_engines)};
handle_config_change("admins", _, _, Persist, _) ->
    % spawn here so couch event manager doesn't deadlock
    {ok, spawn(fun() -> hash_admin_passwords(Persist) end)};
handle_config_change("httpd", "authentication_handlers", _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd", "bind_address", _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd", "port", _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd", "max_connections", _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd", "default_handler", _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd_global_handlers", _, _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change("httpd_db_handlers", _, _, _, _) ->
    {ok, couch_httpd:stop()};
handle_config_change(_, _, _, _, _) ->
    {ok, nil}.

handle_config_terminate(_, stop, _) -> ok;
handle_config_terminate(_, _, _) ->
    spawn(fun() ->
        timer:sleep(5000),
        config:listen_for_changes(?MODULE, nil)
    end).



all_databases() ->
    {ok, DbList} = all_databases(
        fun(DbName, Acc) -> {ok, [DbName | Acc]} end, []),
    {ok, lists:usort(DbList)}.

all_databases(Fun, Acc0) ->
    {ok, #server{root_dir=Root}} = gen_server:call(couch_server, get_server),
    NormRoot = couch_util:normpath(Root),
    FinalAcc = try
    filelib:fold_files(Root,
        "^[a-z0-9\\_\\$()\\+\\-]*" % stock CouchDB name regex
        "(\\.[0-9]{10,})?"         % optional shard timestamp
        "\\.couch$",               % filename extension
        true,
            fun(Filename, AccIn) ->
                NormFilename = couch_util:normpath(Filename),
                case NormFilename -- NormRoot of
                [$/ | RelativeFilename] -> ok;
                RelativeFilename -> ok
                end,
                case Fun(?l2b(filename:rootname(RelativeFilename, ".couch")), AccIn) of
                {ok, NewAcc} -> NewAcc;
                {stop, NewAcc} -> throw({stop, Fun, NewAcc})
                end
            end, Acc0)
    catch throw:{stop, Fun, Acc1} ->
         Acc1
    end,
    {ok, FinalAcc}.


make_room(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> maybe_close_lru_db(Server);
        true -> {ok, Server}
    end.

maybe_close_lru_db(#server{dbs_open=NumOpen, max_dbs_open=MaxOpen}=Server)
        when NumOpen < MaxOpen ->
    {ok, Server};
maybe_close_lru_db(#server{lru=Lru}=Server) ->
    try
        {ok, db_closed(Server#server{lru = couch_lru:close(Lru)}, [])}
    catch error:all_dbs_active ->
        {error, all_dbs_active}
    end.

open_async(Server, From, DbName, {Module, Filepath}, Options) ->
    Parent = self(),
    T0 = os:timestamp(),
    Opener = spawn_link(fun() ->
        Res = couch_db:start_link(Module, DbName, Filepath, Options),
        case {Res, lists:member(create, Options)} of
            {{ok, _Db}, true} ->
                couch_event:notify(DbName, created);
            _ ->
                ok
        end,
        gen_server:call(Parent, {open_result, T0, DbName, Res}, infinity),
        unlink(Parent)
    end),
    ReqType = case lists:member(create, Options) of
        true -> create;
        false -> open
    end,
    % icky hack of field values - compactor_pid used to store clients
    % and fd used for opening request info
    true = ets:insert(couch_dbs, #srv_entry{
        name = DbName,
        pid = Opener,
        waiters = [From],
        lock = locked,
        req_type = ReqType,
        db_options = Options
    }),
    true = ets:insert(couch_dbs_pid_to_name, {Opener, DbName}),
    db_opened(Server, Options).

handle_call(close_lru, _From, #server{lru=Lru} = Server) ->
    try
        {reply, ok, db_closed(Server#server{lru = couch_lru:close(Lru)}, [])}
    catch error:all_dbs_active ->
        {reply, {error, all_dbs_active}, Server}
    end;
handle_call(open_dbs_count, _From, Server) ->
    {reply, Server#server.dbs_open, Server};
handle_call({set_update_lru_on_read, UpdateOnRead}, _From, Server) ->
    {reply, ok, Server#server{update_lru_on_read=UpdateOnRead}};
handle_call({set_max_dbs_open, Max}, _From, Server) ->
    {reply, ok, Server#server{max_dbs_open=Max}};
handle_call(reload_engines, _From, Server) ->
    {reply, ok, Server#server{engines = get_configured_engines()}};
handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call({open_result, T0, DbName, {ok, Db}}, {FromPid, _Tag}, Server) ->
    DbPid = couch_db:pid(Db),
    link(DbPid),
    true = ets:delete(couch_dbs_pid_to_name, FromPid),
    OpenTime = timer:now_diff(os:timestamp(), T0) / 1000,
    couch_stats:update_histogram([couchdb, db_open_time], OpenTime),
    [#srv_entry{}=SE] = ets:lookup(couch_dbs, DbName),
    #srv_entry{
        waiters = Waiters,
        req_type = ReqType
    } = SE,
    [gen_server:reply(From, {ok, Db}) || From <- Waiters],
    % Cancel the creation request if it exists.
    case ReqType of
        {create, DbName, _Filepath, _Options, CrFrom} ->
            gen_server:reply(CrFrom, file_exists);
        _ ->
            ok
    end,
    true = ets:insert(couch_dbs, SE#srv_entry{
        name = DbName,
        db = Db,
        pid = DbPid,
        lock = unlocked,
        waiters = undefined,
        start_time = couch_db:get_instance_start_time(Db)
    }),
    true = ets:insert(couch_dbs_pid_to_name, {DbPid, DbName}),
    Lru = case couch_db:is_system_db(Db) of
        false ->
            couch_lru:insert(DbName, Server#server.lru);
        true ->
            Server#server.lru
    end,
    {reply, ok, Server#server{lru = Lru}};
handle_call({open_result, T0, DbName, {error, eexist}}, From, Server) ->
    handle_call({open_result, T0, DbName, file_exists}, From, Server);
handle_call({open_result, _T0, DbName, Error}, {FromPid, _Tag}, Server) ->
    [#srv_entry{}=SE] = ets:lookup(couch_dbs, DbName),
    #srv_entry{
        waiters = Waiters
    } = SE,
    [gen_server:reply(From, Error) || From <- Waiters],
    couch_log:info("open_result error ~p for ~s", [Error, DbName]),
    true = ets:delete(couch_dbs, DbName),
    true = ets:delete(couch_dbs_pid_to_name, FromPid),
    NewServer = case SE#srv_entry.req_type of
        {create, DbName, Filepath, Options, CrFrom} ->
            open_async(Server, CrFrom, DbName, Filepath, Options);
        _ ->
            Server
    end,
    {reply, ok, db_closed(NewServer, SE#srv_entry.db_options)};
handle_call({open, DbName, Options}, From, Server) ->
    case ets:lookup(couch_dbs, DbName) of
    [] ->
        DbNameList = binary_to_list(DbName),
        case check_dbname(Server, DbNameList) of
        ok ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                Engine = get_engine(Server, DbNameList),
                {noreply, open_async(Server2, From, DbName, Engine, Options)};
            CloseError ->
                {reply, CloseError, Server}
            end;
        Error ->
            {reply, Error, Server}
        end;
    [#srv_entry{waiters = Waiters} = Entry] when is_list(Waiters) ->
        true = ets:insert(couch_dbs, Entry#srv_entry{
                waiters = [From | Waiters]
        }),
        if length(Waiters) =< 10 -> ok; true ->
            Fmt = "~b clients waiting to open db ~s",
            couch_log:info(Fmt, [length(Waiters), DbName])
        end,
        {noreply, Server};
    [#srv_entry{db=Db}] ->
        {reply, {ok, Db}, Server}
    end;
handle_call({create, DbName, Options}, From, Server) ->
    DbNameList = binary_to_list(DbName),
    Engine = get_engine(Server, DbNameList),
    case check_dbname(Server, DbNameList) of
    ok ->
        case ets:lookup(couch_dbs, DbName) of
        [] ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                Opts = [create | Options],
                {noreply, open_async(Server2, From, DbName, Engine, Opts)};
            CloseError ->
                {reply, CloseError, Server}
            end;
        [#srv_entry{req_type=open}=Entry] ->
            % We're trying to create a database while someone is in
            % the middle of trying to open it. We allow one creator
            % to wait while we figure out if it'll succeed.
            % icky hack of field values - fd used to store create request
            CrOptions = [create | Options],
            ReqType = {create, DbName, Engine, CrOptions, From},
            true = ets:insert(couch_dbs, Entry#srv_entry{req_type = ReqType}),
            {noreply, Server};
        [_AlreadyRunningDb] ->
            {reply, file_exists, Server}
        end;
    Error ->
        {reply, Error, Server}
    end;
handle_call({delete, DbName, Options}, _From, Server) ->
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        Server2 =
        case ets:lookup(couch_dbs, DbName) of
        [] -> Server;
        [#srv_entry{pid=Pid, waiters=Waiters}=SE] when is_list(Waiters) ->
            true = ets:delete(couch_dbs, DbName),
            true = ets:delete(couch_dbs_pid_to_name, Pid),
            couch_util:shutdown_sync(Pid),
            [gen_server:reply(F, not_found) || F <- Waiters],
            db_closed(Server, SE#srv_entry.db_options);
        [#srv_entry{pid=Pid}=SE] ->
            true = ets:delete(couch_dbs, DbName),
            true = ets:delete(couch_dbs_pid_to_name, Pid),
            couch_util:shutdown_sync(Pid),
            db_closed(Server, SE#srv_entry.db_options)
        end,

        couch_db_plugin:on_delete(DbName, Options),

        {Module, Filepath} = get_engine(Server, DbNameList),
        Async = not lists:member(sync, Options),
        delete_compaction_files(RootDir, FilePath),

        case Module:delete(Server#server.root_dir, FilePath, Async) of
        ok ->
            couch_event:notify(DbName, deleted),
            {reply, ok, Server2};
        {error, enoent} ->
            {reply, not_found, Server2};
        Else ->
            {reply, Else, Server2}
        end;
    Error ->
        {reply, Error, Server}
    end;
handle_call({db_updated, Db}, _From, Server0) ->
    true = couch_db:is_db(Db),
    DbName = couch_db:name(Db),
    StartTime = couch_db:get_instance_start_time(Db),
    Server = try ets:lookup(couch_dbs, DbName) of
        [#srv_entry{start_time=StartTime}=SE] ->
            true = ets:insert(couch_dbs, SE#srv_entry{
                name = DbName,
                db = Db,
                pid = couch_db:pid(Db),
                lock = unlocked,
                waiters = undefined,
                start_time = StartTime
            }),
            Lru = case couch_db:is_system_db(Db) of
                false -> couch_lru:update(DbName, Server0#server.lru);
                true -> Server0#server.lru
            end,
            Server0#server{lru = Lru};
        _ ->
            Server0
    catch _:_ ->
        Server0
    end,
    {reply, ok, Server}.

handle_cast({update_lru, DbName}, #server{lru = Lru, update_lru_on_read=true} = Server) ->
    {noreply, Server#server{lru = couch_lru:update(DbName, Lru)}};
handle_cast({update_lru, _DbName}, Server) ->
    {noreply, Server};
handle_cast(Msg, Server) ->
    {stop, {unknown_cast_message, Msg}, Server}.

code_change(_OldVsn, #server{}=State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _Pid, config_change}, Server) ->
    {stop, config_change, Server};
handle_info({'EXIT', Pid, Reason}, Server) ->
    case ets:lookup(couch_dbs_pid_to_name, Pid) of
    [{Pid, DbName}] ->
        [#srv_entry{}=SE] = ets:lookup(couch_dbs, DbName),
        #srv_entry{
            waiters = Waiters,
            db_options = DbOptions
        } = SE,
        if Reason /= snappy_nif_not_loaded -> ok; true ->
            Msg = io_lib:format("To open the database `~s`, Apache CouchDB "
                "must be built with Erlang OTP R13B04 or higher.", [DbName]),
            couch_log:error(Msg, [])
        end,
        couch_log:info("db ~s died with reason ~p", [DbName, Reason]),
        if not is_list(Waiters) -> ok; true ->
            [gen_server:reply(From, Reason) || From <- Waiters]
        end,
        true = ets:delete(couch_dbs, DbName),
        true = ets:delete(couch_dbs_pid_to_name, Pid),
        {noreply, db_closed(Server, DbOptions)};
    [] ->
        {noreply, Server}
    end;
handle_info(Info, Server) ->
    {stop, {unknown_message, Info}, Server}.

db_opened(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> Server#server{dbs_open=Server#server.dbs_open + 1};
        true -> Server
    end.

db_closed(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> Server#server{dbs_open=Server#server.dbs_open - 1};
        true -> Server
    end.

delete_file(RootDir, FullFilePath, Options) ->
    Async = not lists:member(sync, Options),
    RenameOnDelete = config:get_boolean("couchdb", "rename_on_delete", false),
    case {Async, RenameOnDelete} of
        {_, true} ->
            rename_on_delete(FullFilePath);
        {Async, false} ->
            case couch_file:delete(RootDir, FullFilePath, Async) of
                ok -> {ok, deleted};
                Else -> Else
            end
    end.

rename_on_delete(Original) ->
    DeletedFileName = deleted_filename(Original),
    case file:rename(Original, DeletedFileName) of
        ok -> {ok, {renamed, DeletedFileName}};
        Else -> Else
    end.

deleted_filename(Original) ->
    {{Y,Mon,D}, {H,Min,S}} = calendar:universal_time(),
    Suffix = lists:flatten(
        io_lib:format(".~w~2.10.0B~2.10.0B."
            ++ "~2.10.0B~2.10.0B~2.10.0B.deleted"
            ++ filename:extension(Original), [Y,Mon,D,H,Min,S])),
    filename:rootname(Original) ++ Suffix.


get_configured_engines() ->
    ConfigEntries = config:get("couchdb_engines"),
    Engines = lists:flatmap(fun({Extension, Module}) ->
        try
            Module = list_to_atom(Module),
            {module, _} = code:load_file(Module),
            [{Extension, Module}]
        catch _:_ ->
            []
        end
    end, ConfigEntries),
    case Engines of
        [] ->
            [{"couch", couch_bt_engine}];
        Else ->
            Else
    end.


get_engine(Server, DbName) ->
    #server{
        root_dir = RootDir,
        engines = Engines
    } = Server,
    Possible = lists:foldl(fun({Extension, Module}, Acc) ->
        Path = make_filepath(RootDir, DbName, Extension),
        case Module:exists(Path) of
            true ->
                [{Module, Path} | Acc];
            false ->
                Acc
        end
    end, [], Engines),
    case Possible of
        [] ->
            get_default_engine(Server, DbName);
        [Engine] ->
            Engine;
        _ ->
            erlang:error(engine_conflict)
    end.


get_default_engine(Server, DbName) ->
    #server{
        root_dir = RootDir,
        engines = Engines
    } = Server,
    Default = {couch_bt_engine, make_filepath(RootDir, DbName, "couch")},
    case config:get("couchdb", "default_engine") of
        Extension when is_list(Extension) ->
            case lists:keysearch(Extension, 1, Engines) of
                {Extension, Module} ->
                    {Module, make_filepath(RootDir, DbName, Extension)};
                false ->
                    Default
            end;
        _ ->
            Default
    end.


make_filepath(RootDir, DbName, Extension) ->
    filename:join([RootDir, "./" ++ DbName ++ "." ++ Extension]).
