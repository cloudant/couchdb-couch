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
-behaviour(couch_db_monitor).
-vsn(3).

-export([open/2,create/2,delete/2,get_version/0,get_version/1,get_uuid/0]).
-export([all_databases/0, all_databases/2]).
-export([init/1, handle_call/3,sup_start_link/0]).
-export([handle_cast/2,code_change/3,handle_info/2,terminate/2]).
-export([dev_start/0,is_admin/2,has_admins/0,get_stats/0]).

% config_listener api
-export([handle_config_change/5, handle_config_terminate/3]).

% couch_db_monitor api
-export([close/2, tab_name/1]).

-include_lib("couch/include/couch_db.hrl").

-define(MAX_DBS_OPEN, 5000).
-define(RELISTEN_DELAY, 5000).
-define(DEFAULT_RETRIES, 1000).

-record(server,{
    root_dir = [],
    monitor_state,
    start_time=""
    }).

-record(entry, {
    db,
    monitor,
    status,
    req_type,
    waiters = [],
    sys_db
}).

dev_start() ->
    couch:stop(),
    up_to_date = make:all([load, debug_info]),
    couch:start().

get_version() ->
    ?COUCHDB_VERSION. %% Defined in rebar.config.script
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
    {ok, #server{start_time=Time,monitor_state=Mon}} =
            gen_server:call(couch_server, get_server),
    [{start_time, ?l2b(Time)}, {dbs_open, couch_db_monitor:num_open(Mon)}].

sup_start_link() ->
    gen_server:start_link({local, couch_server}, couch_server, [], []).


open(DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
    case couch_db_monitor:incref(?MODULE, DbName) of
    ok ->
        {ok, #entry{
            db = Db,
            monitor = Monitor,
            status = active
        }} = couch_db_monitor:lookup(?MODULE, name, DbName),
        ok = couch_db_monitor:notify(Monitor),
        {ok, Db#db{
            user_ctx = Ctx,
            fd_monitor = {self(), Monitor, couch_db:is_system_db(Db)}
        }};
    _ ->
        Timeout = couch_util:get_value(timeout, Options, infinity),
        Create = couch_util:get_value(create_if_missing, Options, false),
        OpenOpts = [{timestamp, os:timestamp()} | Options],
        case gen_server:call(couch_server, {open, DbName, OpenOpts}, Timeout) of
        {ok, #db{} = Db} ->
            {ok, Db#db{user_ctx = Ctx}};
        {not_found, no_db_file} when Create ->
            couch_log:warning("creating missing database: ~s", [DbName]),
            couch_server:create(DbName, Options);
        Error ->
            Error
        end
    end.

create(DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    case gen_server:call(couch_server, {create, DbName, Options}, infinity) of
    {ok, #db{} = Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, Db#db{user_ctx = Ctx}};
    Error ->
        Error
    end.

delete(DbName, Options) ->
    gen_server:call(couch_server, {delete, DbName, Options}, infinity).

maybe_add_sys_db_callbacks(DbName, Options) when is_binary(DbName) ->
    maybe_add_sys_db_callbacks(?b2l(DbName), Options);
maybe_add_sys_db_callbacks(DbName, Options) ->
    DbsDbName = config:get("mem3", "shards_db", "_dbs"),
    NodesDbName = config:get("mem3", "nodes_db", "_nodes"),

    IsReplicatorDb = path_ends_with(DbName, "_replicator"),
    UsersDbSuffix = config:get("couchdb", "users_db_suffix", "_users"),
    IsUsersDb = path_ends_with(DbName, "_users")
        orelse path_ends_with(DbName, UsersDbSuffix),
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

path_ends_with(Path, Suffix) when is_binary(Suffix) ->
    Suffix =:= couch_db:dbname_suffix(Path);
path_ends_with(Path, Suffix) when is_list(Suffix) ->
    path_ends_with(Path, ?l2b(Suffix)).

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

get_full_filename(Server, DbName) ->
    filename:join([Server#server.root_dir, "./" ++ DbName ++ ".couch"]).

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
    MaxDbsOpen = list_to_integer(
            config:get("couchdb", "max_dbs_open", integer_to_list(?MAX_DBS_OPEN))),
    ok = config:listen_for_changes(?MODULE, nil),
    ok = couch_file:init_delete_dir(RootDir),
    hash_admin_passwords(),
    MonState = couch_db_monitor:new(?MODULE, MaxDbsOpen),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                monitor_state=MonState,
                start_time=couch_util:rfc1123_date()}}.

terminate(Reason, Srv) ->
    Msg = "couch_server terminating with ~p, state ~2048p",
    couch_log:error(Msg, [Reason, Srv]),
    ets:foldl(fun({_DbName, #entry{db = Db}}, _) ->
        couch_util:shutdown_sync(Db#db.main_pid)
    end, nil, tab_name(name)),
    ok.

handle_config_change("couchdb", "database_dir", _, _, _) ->
    exit(whereis(couch_server), config_change),
    remove_handler;
handle_config_change("couchdb", "max_dbs_open", Max, _, _) when is_list(Max) ->
    {ok, gen_server:call(couch_server,{set_max_dbs_open,list_to_integer(Max)})};
handle_config_change("couchdb", "max_dbs_open", _, _, _) ->
    {ok, gen_server:call(couch_server,{set_max_dbs_open,?MAX_DBS_OPEN})};
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

handle_config_terminate(_, stop, _) ->
    ok;
handle_config_terminate(_Server, _Reason, _State) ->
    erlang:send_after(?RELISTEN_DELAY, whereis(?MODULE), restart_config_listener).


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
        false ->
            #server{monitor_state=MonState} = Server,
            case couch_db_monitor:maybe_close_idle(MonState) of
                {ok, NewMonState} ->
                    {ok, Server#server{monitor_state=NewMonState}};
                Error ->
                    Error
            end;
        true ->
            {ok, Server}
    end.


% couch_db_monitor behaviour implementation
tab_name(name) -> couch_db;
tab_name(pid) -> couch_db_by_pid;
tab_name(counters) -> couch_db_counters;
tab_name(idle) -> couch_db_idle.

close(_DbName, #entry{db=#db{compactor_pid = Pid}}) when Pid /= nil ->
    false;
close(_DbName, #entry{db=#db{waiting_delayed_commit = WDC}}) when WDC /= nil ->
    false;
close(DbName, #entry{db=Db, monitor=Monitor, sys_db=IsSysDb}) ->
    case ets:select_delete(tab_name(counters), [{{DbName, 0}, [], [true]}]) of
        1 ->
            exit(Monitor, kill),
            put(last_db_closed, DbName),
            gen_server:cast(Db#db.fd, close),
            exit(Db#db.main_pid, kill),
            true = ets:delete(tab_name(name), DbName),
            true = ets:delete(tab_name(pid), Db#db.main_pid),
            {true, IsSysDb};
        0 ->
            false
    end.

open_async(Server, From, DbName, Options) ->
    Parent = self(),
    T0 = os:timestamp(),
    Opener = spawn_link(fun() ->
        Result = case check_dbname(Server, DbName) of
            ok ->
                DbNameList = binary_to_list(DbName),
                Filepath = get_full_filename(Server, DbNameList),
                Res = couch_db:start_link(DbName, Filepath, Options),
                case {Res, lists:member(create, Options)} of
                    {{ok, _Db}, true} ->
                        couch_event:notify(DbName, created);
                    _ ->
                        ok
                end,
                Res;
            Error ->
                Error
        end,
        gen_server:call(Parent, {open_result, T0, DbName, Result}, infinity),
        unlink(Parent)
    end),
    ReqType = case lists:member(create, Options) of
        true -> create;
        false -> open
    end,
    IsSysDb = lists:member(sys_db, Options),
    Entry = #entry{
        db = #db{
            name = DbName,
            main_pid = Opener,
            header = undefined,
            options = Options
        },
        monitor = couch_db_monitor:spawn_link(?MODULE, DbName, IsSysDb),
        status = inactive,
        req_type = ReqType,
        waiters = [From],
        sys_db = IsSysDb
    },
    couch_db_monitor:insert(?MODULE, name, DbName, Entry),
    couch_db_monitor:insert(?MODULE, pid, Opener, DbName),
    NewMon = couch_db_monitor:opened(Server#server.monitor_state, lists:member(sys_db, Options)),
    Server#server{monitor_state=NewMon}.
    %verify_size(NewServer).
    %TotalOpen = NewServer#server.dbs_open + NewServer#server.sys_dbs_open,
    %case ets:info(couch_db_monitor:tab_name(name, db), size) of
    %    TotalOpen ->
    %        ok;
    %    Size ->
    %        exit({dbs_open_mismatch, Size, TotalOpen, get(last_db_closed)})
    %end,
    %NewServer.

handle_call(open_dbs_count, _From, Server) ->
    {reply, couch_db_monitor:num_open(Server#server.monitor_state), Server};
handle_call({set_max_dbs_open, Max}, _From, Server) ->
    Mon = couch_db_monitor:set_max_open(Server#server.monitor_state, Max),
    {reply, ok, Server#server{monitor_state=Mon}};
handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call({open_result, T0, DbName, {ok, Db}}, {FromPid, _Tag}, Server) ->
    link(Db#db.main_pid),
    ok = couch_db_monitor:delete(Server#server.monitor_state, pid, FromPid),
    OpenTime = timer:now_diff(os:timestamp(), T0) / 1000,
    couch_stats:update_histogram([couchdb, db_open_time], OpenTime),
    {ok, #entry{
        monitor = Monitor,
        req_type = ReqType,
        waiters = Waiters,
        status = inactive
    } = Entry} = couch_db_monitor:lookup(?MODULE, name, DbName),
    ok = couch_db_monitor:set_pid(Monitor, Db#db.main_pid),
    % Cancel the creation request if it exists.
    case ReqType of
        {create, DbName, _Options, CrFrom} ->
            gen_server:reply(CrFrom, file_exists);
        _ ->
            ok
    end,
    NewEntry = Entry#entry{
        db = Db,
        status = active,
        req_type = undefined,
        waiters = undefined
    },
    couch_db_monitor:insert(?MODULE, name, DbName, NewEntry),
    couch_db_monitor:insert(?MODULE, pid, Db#db.main_pid, DbName),
    couch_db_monitor:insert(?MODULE, counters, DbName, 0),

    lists:foreach(fun(From) ->
        {Client, _} = From,
        ok = couch_db_monitor:incref(?MODULE, DbName),
        ok = couch_db_monitor:notify(Monitor, Client),
        ClientMon = {Client, Monitor, couch_db:is_system_db(Db)},
        gen_server:reply(From, {ok, Db#db{fd_monitor = ClientMon}})
    end, Waiters),

    {reply, ok, Server};
handle_call({open_result, T0, DbName, {error, eexist}}, From, Server) ->
    handle_call({open_result, T0, DbName, file_exists}, From, Server);
handle_call({open_result, _T0, DbName, Error}, {FromPid, _Tag}, Server) ->
    {ok, #entry{status = inactive} = Entry} = couch_db_monitor:lookup(?MODULE, name, DbName),
    #entry{
        db = Db,
        monitor = Monitor,
        req_type = ReqType,
        waiters = Waiters
    } = Entry,
    [gen_server:reply(From, Error) || From <- Waiters],
    ok = couch_db_monitor:close(Monitor),
    couch_log:info("open_result error ~p for ~s", [Error, DbName]),
    ok = couch_db_monitor:delete(?MODULE, DbName, FromPid),
    NewServer = case ReqType of
        {create, DbName, Options, CrFrom} ->
            open_async(Server, CrFrom, DbName, Options);
        _ ->
            Server
    end,
    NewMonState = couch_db_monitor:opened(NewServer#server.monitor_state, lists:member(sys_db, Db#db.options)),
    {reply, ok, NewServer#server{monitor_state=NewMonState}};
handle_call({open, DbName, Options}, From, Server) ->
    case should_drop(Options) of
    true ->
        {reply, timedout, Server};
    false ->
        case couch_db_monitor:lookup(?MODULE, name, DbName) of
        not_found ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                {noreply, open_async(Server2, From, DbName, Options)};
            CloseError ->
                {reply, CloseError, Server}
            end;
        {ok, #entry{status = inactive} = Entry} ->
            #entry{
                waiters = Waiters
            } = Entry,
            NewEntry = Entry#entry{
                waiters = [From | Waiters]
            },
            couch_db_monitor:insert(?MODULE, name, DbName, NewEntry),
            if length(Waiters) =< 10 -> ok; true ->
                Fmt = "~b clients waiting to open db ~s",
                couch_log:info(Fmt, [length(Waiters) + 1, DbName])
            end,
            {noreply, Server};
        {ok, #entry{status = active} = Entry} ->
            #entry{
                db = Db,
                monitor = Monitor
            } = Entry,
            ok = couch_db_monitor:incref(?MODULE, DbName),
            {Client, _} = From,
            ok = couch_db_monitor:notify(Monitor, Client),
            ClientMon = {Client, Monitor, couch_db:is_system_db(Db)},
            {reply, {ok, Db#db{fd_monitor = ClientMon}}, Server}
        end
    end;
handle_call({create, DbName, Options}, From, Server) ->
    case couch_db_monitor:lookup(?MODULE, name, DbName) of
    not_found ->
        case make_room(Server, Options) of
        {ok, Server2} ->
            {noreply, open_async(Server2, From, DbName, [create | Options])};
        CloseError ->
            {reply, CloseError, Server}
        end;
    {ok, #entry{status = inactive, req_type = open} = Entry} ->
        % We're trying to create a database while someone is in
        % the middle of trying to open it. We allow one creator
        % to wait while we figure out if it'll succeed.
        CrOptions = [create | Options],
        NewEntry = Entry#entry{
            req_type = {create, DbName, CrOptions, From}
        },
        couch_db_monitor:insert(?MODULE, name, DbName, NewEntry),
        {noreply, Server};
    {ok, #entry{}} ->
        {reply, file_exists, Server}
    end;
handle_call({delete, DbName, Options}, _From, Server) ->
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        FullFilepath = get_full_filename(Server, DbNameList),
        Server2 =
        case couch_db_monitor:lookup(?MODULE, name, DbName) of
        not_found -> Server;
        {ok, #entry{waiters = Waiters} = Entry} ->
            #entry{
                db = Db,
                waiters = Waiters
            } = Entry,
            couch_db_monitor:delete(?MODULE, DbName, Db#db.main_pid),
            exit(Db#db.main_pid, kill),
            if not is_list(Waiters) -> ok; true ->
                [gen_server:reply(F, not_found) || F <- Waiters]
            end,
            NewMonState = couch_db_monitor:closed(Server#server.monitor_state, lists:member(sys_db, Db#db.options)),
            Server#server{monitor_state=NewMonState}
        end,

        %% Delete any leftover compaction files. If we don't do this a
        %% subsequent request for this DB will try to open them to use
        %% as a recovery.
        lists:foreach(fun(Ext) ->
            couch_file:delete(Server#server.root_dir, FullFilepath ++ Ext)
        end, [".compact", ".compact.data", ".compact.meta"]),
        couch_file:delete(Server#server.root_dir, FullFilepath ++ ".compact"),

        couch_db_plugin:on_delete(DbName, Options),

        DelOpt = [{context, delete} | Options],
        case couch_file:delete(Server#server.root_dir, FullFilepath, DelOpt) of
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
handle_call({db_updated, #db{}=Db}, _From, Server) ->
    #db{name = DbName, instance_start_time = StartTime} = Db,
    try couch_db_monitor:lookup(?MODULE, name, DbName) of
        {ok, #entry{status = active} = Entry} ->
            #entry{
                db = CurrDb
            } = Entry,
            if StartTime /= CurrDb#db.instance_start_time -> ok; true ->
                couch_db_monitor:insert(?MODULE, name, DbName, Entry#entry{db=Db})
            end;
        _ ->
            ok
    catch _:_ ->
        ok
    end,
    {reply, ok, Server}.

handle_cast(Msg, Server) ->
    {stop, {unknown_cast_message, Msg}, Server}.

code_change(_OldVsn, #server{}=State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _Pid, config_change}, Server) ->
    {stop, config_change, Server};
handle_info({'EXIT', Pid, Reason}, Server) ->
    case couch_db_monitor:lookup(?MODULE, pid, Pid) of
    {ok, {Pid, DbName}} ->
        {ok, #entry{db = Db, waiters = Waiters}} = couch_db_monitor:lookup(?MODULE, name, DbName),
        if Reason /= snappy_nif_not_loaded -> ok; true ->
            Msg = io_lib:format("To open the database `~s`, Apache CouchDB "
                "must be built with Erlang OTP R13B04 or higher.", [DbName]),
            couch_log:error(Msg, [])
        end,
        couch_log:info("db ~s died with reason ~p", [DbName, Reason]),
        if not is_list(Waiters) -> ok; true ->
            [gen_server:reply(From, Reason) || From <- Waiters]
        end,
        couch_db_monitor:delete(?MODULE, DbName, Pid),
        NewMonState = couch_db_monitor:closed(Server#server.monitor_state, lists:member(sys_db, Db#db.options)),
        {noreply, Server#server{monitor_state=NewMonState}};
    not_found ->
        {noreply, Server}
    end;
handle_info(restart_config_listener, State) ->
    ok = config:listen_for_changes(?MODULE, nil),
    {noreply, State};
handle_info(Info, Server) ->
    {stop, {unknown_message, Info}, Server}.


should_drop(Options) ->
    Timeout = couch_util:get_value(timeout, Options, infinity),
    Timestamp = couch_util:get_value(timestamp, Options, {0, 0, 0}),
    % Timeout is in millisecond, now_diff returns micro so we
    % have to devide by 1000
    Diff = timer:now_diff(os:timestamp(), Timestamp) / 1000,
    Timeout /= infinity andalso Diff > (Timeout * 1.25).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup() ->
    ok = meck:new(config, [passthrough]),
    ok = meck:expect(config, get, fun config_get/3),
    ok.

teardown(_) ->
    (catch meck:unload(config)).

config_get("couchdb", "users_db_suffix", _) -> "users_db";
config_get(_, _, _) -> undefined.

maybe_add_sys_db_callbacks_pass_test_() ->
    SysDbCases = [
        "shards/00000000-3fffffff/foo/users_db.1415960794.couch",
        "shards/00000000-3fffffff/foo/users_db.1415960794",
        "shards/00000000-3fffffff/foo/users_db",
        "shards/00000000-3fffffff/users_db.1415960794.couch",
        "shards/00000000-3fffffff/users_db.1415960794",
        "shards/00000000-3fffffff/users_db",

        "shards/00000000-3fffffff/_users.1415960794.couch",
        "shards/00000000-3fffffff/_users.1415960794",
        "shards/00000000-3fffffff/_users",

        "foo/users_db.couch",
        "foo/users_db",
        "users_db.couch",
        "users_db",
        "foo/_users.couch",
        "foo/_users",
        "_users.couch",
        "_users",

        "shards/00000000-3fffffff/foo/_replicator.1415960794.couch",
        "shards/00000000-3fffffff/foo/_replicator.1415960794",
        "shards/00000000-3fffffff/_replicator",
        "foo/_replicator.couch",
        "foo/_replicator",
        "_replicator.couch",
        "_replicator"
    ],

    NonSysDbCases = [
        "shards/00000000-3fffffff/foo/mydb.1415960794.couch",
        "shards/00000000-3fffffff/foo/mydb.1415960794",
        "shards/00000000-3fffffff/mydb",
        "foo/mydb.couch",
        "foo/mydb",
        "mydb.couch",
        "mydb"
    ],
    {
        foreach, fun setup/0, fun teardown/1,
        [
            [should_add_sys_db_callbacks(C) || C <- SysDbCases]
            ++
            [should_add_sys_db_callbacks(?l2b(C)) || C <- SysDbCases]
            ++
            [should_not_add_sys_db_callbacks(C) || C <- NonSysDbCases]
            ++
            [should_not_add_sys_db_callbacks(?l2b(C)) || C <- NonSysDbCases]
        ]
    }.

should_add_sys_db_callbacks(DbName) ->
    {test_name(DbName), ?_test(begin
        Options = maybe_add_sys_db_callbacks(DbName, [other_options]),
        ?assert(lists:member(sys_db, Options)),
        ok
    end)}.
should_not_add_sys_db_callbacks(DbName) ->
    {test_name(DbName), ?_test(begin
        Options = maybe_add_sys_db_callbacks(DbName, [other_options]),
        ?assertNot(lists:member(sys_db, Options)),
        ok
    end)}.

test_name(DbName) ->
    lists:flatten(io_lib:format("~p", [DbName])).


-endif.
