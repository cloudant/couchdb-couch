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
-vsn(3).

-export([open/2,create/2,delete/2,get_version/0,get_version/1,get_uuid/0]).
-export([all_databases/0, all_databases/2]).
-export([init/1, handle_call/3,sup_start_link/0]).
-export([handle_cast/2,code_change/3,handle_info/2,terminate/2]).
-export([dev_start/0,is_admin/2,has_admins/0,get_stats/0]).

% config_listener api
-export([handle_config_change/5, handle_config_terminate/3]).

-include_lib("couch/include/couch_db.hrl").

-define(DBS, couch_dbs).
-define(PIDS, couch_dbs_pid_to_name).
-define(MONITORS, couch_db_monitors).
-define(IDLE, couch_dbs_idle).

-define(MAX_DBS_OPEN, 100).
-define(RELISTEN_DELAY, 5000).

-record(server,{
    root_dir = [],
    max_dbs_open=?MAX_DBS_OPEN,
    dbs_open=0,
    start_time=""
    }).

-record(mon, {
    name,
    pid
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
    {ok, #server{start_time=Time,dbs_open=Open}} =
            gen_server:call(couch_server, get_server),
    [{start_time, ?l2b(Time)}, {dbs_open, Open}].

sup_start_link() ->
    gen_server:start_link({local, couch_server}, couch_server, [], []).


open(DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
    case ets:lookup(?DBS, DbName) of
    [#db{fd=Fd, fd_monitor=Lock} = Db] when Lock =/= locked ->
        gen_server:cast(?MODULE, {incref, DbName, self()}),
        {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
    _ ->
        Timeout = couch_util:get_value(timeout, Options, infinity),
        Create = couch_util:get_value(create_if_missing, Options, false),
        case gen_server:call(couch_server, {open, DbName, Options}, Timeout) of
        {ok, #db{fd=Fd} = Db} ->
            {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
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
    {ok, #db{fd=Fd} = Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
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
    ets:new(?DBS, [set, protected, named_table, {keypos, #db.name}]),
    ets:new(?PIDS, [set, protected, named_table]),
    ets:new(?MONITORS, [set, protected, named_table, {keypos, #mon.name}]),
    ets:new(?IDLE, [set, protected, named_table, {keypos, #mon.name}]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                max_dbs_open=MaxDbsOpen,
                start_time=couch_util:rfc1123_date()}}.

terminate(Reason, Srv) ->
    Args = [Reason, Srv],
    couch_log:error("couch_server terminating with ~r, state ~2048p", Args),
    ets:foldl(fun(#db{main_pid=Pid}, _) -> couch_util:shutdown_sync(Pid) end,
        nil, ?DBS),
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
        false -> maybe_close_idle_db(Server);
        true -> {ok, Server}
    end.


maybe_close_idle_db(Server) ->
    #server{
        dbs_open = NumOpen,
        max_dbs_open = MaxOpen
    } = Server,
    case NumOpen < MaxOpen of
        true ->
            {ok, Server};
        false ->
            try
                close_idle_db(),
                {ok, db_closed(Server, [])}
            catch error:all_dbs_active ->
                {error, all_dbs_active}
            end
    end.


close_idle_db() ->
    {_, Stack} = process_info(self(), current_stacktrace),
    case ets:first(?IDLE) of
        DbName when is_binary(DbName) ->
            case close_idle_db(DbName) of
                closed ->
                    ok;
                not_closed ->
                    close_idle_db()
            end;
        '$end_of_table' ->
            %dump_monitors(),
            erlang:error(all_dbs_active)
    end.


close_idle_db(DbName) ->
    true = ets:update_element(?DBS, DbName, {#db.fd_monitor, locked}),
    [#db{main_pid = Pid} = Db] = ets:lookup(?DBS, DbName),
    case couch_db:is_idle(Db) of
        true ->
            [#mon{pid = MonPid}] = ets:lookup(?MONITORS, DbName),
            gen_server:cast(MonPid, stop),
            exit(Pid, kill),
            true = ets:delete(?DBS, DbName),
            true = ets:delete(?PIDS, Pid),
            true = ets:delete(?MONITORS, DbName),
            true = ets:delete(?IDLE, DbName),
            closed;
        false ->
            % Someone must've just opened this database and
            % we have an incref message for it in our
            % message queue.
            true = ets:delete(?IDLE, DbName),
            not_closed
    end.


%% dump_monitors() ->
%%     MaxOpen = config:get("couchdb", "max_dbs_open", "default"),
%%     CurrOpen = ets:info(?DBS, size),
%%     io:format(standard_error, "Max: ~p, Curr: ~p~n", [MaxOpen, CurrOpen]),
%%     io:format(standard_error, "Idle: ~p~n", [ets:tab2list(?IDLE)]),
%%     lists:foreach(fun(#db{name = DbName} = Db) ->
%%         [#mon{pid = Pid}] = ets:lookup(?MONITORS, DbName),
%%         Idle = couch_db:is_idle(Db),
%%         Status = gen_server:call(Pid, status),
%%         io:format(standard_error, "Db: ~s, Idle: ~p, Monitor: ~p~n", [DbName, Idle, Status])
%%     end, ets:tab2list(?DBS)).


open_async(Server, From, DbName, Filepath, Options) ->
    Parent = self(),
    T0 = os:timestamp(),
    Opener = spawn_link(fun() ->
        Res = couch_db:start_link(DbName, Filepath, Options),
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
    Client = element(1, From),
    IsSysDb = lists:member(sys_db, Options),
    {ok, MonPid} = couch_server_monitor:start_link(DbName, Client, IsSysDb),
    Monitor = #mon{name = DbName, pid = MonPid},
    % icky hack of field values
    % - compactor_pid used to store clients
    % - fd used for opening request info
    % - id_tree used for monitor
    true = ets:insert(?DBS, #db{
        name = DbName,
        fd = ReqType,
        main_pid = Opener,
        compactor_pid = [From],
        fd_monitor = locked,
        id_tree = Monitor,
        options = Options
    }),
    true = ets:insert(?PIDS, {Opener, DbName}),
    true = ets:insert(?MONITORS, Monitor),
    db_opened(Server, Options).

handle_call(open_dbs_count, _From, Server) ->
    {reply, Server#server.dbs_open, Server};
handle_call({set_max_dbs_open, Max}, _From, Server) ->
    {reply, ok, Server#server{max_dbs_open=Max}};
handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call({open_result, T0, DbName, {ok, Db}}, {FromPid, _Tag}, Server) ->
    link(Db#db.main_pid),
    true = ets:delete(?PIDS, FromPid),
    OpenTime = timer:now_diff(os:timestamp(), T0) / 1000,
    couch_stats:update_histogram([couchdb, db_open_time], OpenTime),
    % icky hack of field values
    % - compactor_pid used to store clients
    % - fd used to possibly store a creation request
    % - id_tree used to store monitor
    [#db{
        fd = ReqType,
        compactor_pid = Froms,
        id_tree = #mon{}
    }] = ets:lookup(?DBS, DbName),
    [gen_server:reply(From, {ok, Db}) || From <- Froms],
    % Cancel the creation request if it exists.
    case ReqType of
        {create, DbName, _Filepath, _Options, CrFrom} ->
            gen_server:reply(CrFrom, file_exists);
        _ ->
            ok
    end,
    true = ets:insert(?DBS, Db),
    true = ets:insert(?PIDS, {Db#db.main_pid, DbName}),
    {reply, ok, Server};
handle_call({open_result, T0, DbName, {error, eexist}}, From, Server) ->
    handle_call({open_result, T0, DbName, file_exists}, From, Server);
handle_call({open_result, _T0, DbName, Error}, {FromPid, _Tag}, Server) ->
    % icky hack of field values
    % - compactor_pid used to store clients
    % - fd used to possibly store a creation request
    % - id_tree used to store monitor
    [#db{
        fd = ReqType,
        compactor_pid = Froms,
        id_tree = #mon{pid = MonPid},
        options = Options
    }] = ets:lookup(?DBS, DbName),
    [gen_server:reply(From, Error) || From <- Froms],
    gen_server:cast(MonPid, stop),
    couch_log:info("open_result error ~p for ~s", [Error, DbName]),
    true = ets:delete(?DBS, DbName),
    true = ets:delete(?PIDS, FromPid),
    true = ets:delete(?MONITORS, DbName),
    false = ets:member(?IDLE, DbName),
    NewServer = case ReqType of
        {create, DbName, Filepath, Options, CrFrom} ->
            open_async(Server, CrFrom, DbName, Filepath, Options);
        _ ->
            Server
    end,
    {reply, ok, db_closed(NewServer, Options)};
handle_call({open, DbName, Options}, From, Server) ->
    case ets:lookup(?DBS, DbName) of
    [] ->
        DbNameList = binary_to_list(DbName),
        case check_dbname(Server, DbNameList) of
        ok ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                Filepath = get_full_filename(Server, DbNameList),
                {noreply, open_async(Server2, From, DbName, Filepath, Options)};
            CloseError ->
                {reply, CloseError, Server}
            end;
        Error ->
            {reply, Error, Server}
        end;
    [#db{compactor_pid = Froms, id_tree = #mon{pid = MonPid}} = Db] ->
        % icky hack of field values
        % - compactor_pid used to store clients
        % - id_tree used to store monitor
        true = ets:insert(?DBS, Db#db{compactor_pid = [From|Froms]}),
        gen_server:cast(MonPid, {incref, element(1, From)}),
        if length(Froms) =< 10 -> ok; true ->
            Fmt = "~b clients waiting to open db ~s",
            couch_log:info(Fmt, [length(Froms), DbName])
        end,
        {noreply, Server};
    [#db{} = Db] ->
        [#mon{pid = MonPid}] = ets:lookup(?MONITORS, DbName),
        gen_server:cast(MonPid, {incref, element(1, From)}),
        {reply, {ok, Db}, Server}
    end;
handle_call({create, DbName, Options}, From, Server) ->
    DbNameList = binary_to_list(DbName),
    Filepath = get_full_filename(Server, DbNameList),
    case check_dbname(Server, DbNameList) of
    ok ->
        case ets:lookup(?DBS, DbName) of
        [] ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                {noreply, open_async(Server2, From, DbName, Filepath,
                        [create | Options])};
            CloseError ->
                {reply, CloseError, Server}
            end;
        [#db{fd=open}=Db] ->
            % We're trying to create a database while someone is in
            % the middle of trying to open it. We allow one creator
            % to wait while we figure out if it'll succeed.
            % icky hack of field values - fd used to store create request
            CrOptions = [create | Options],
            NewDb = Db#db{fd={create, DbName, Filepath, CrOptions, From}},
            true = ets:insert(?DBS, NewDb),
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
        FullFilepath = get_full_filename(Server, DbNameList),
        Server2 =
        case ets:lookup(?DBS, DbName) of
        [] -> Server;
        [#db{main_pid=Pid, compactor_pid=Froms} = Db] when is_list(Froms) ->
            #mon{pid = MonPid} = Db#db.id_tree,
            % icky hack of field values
            % - compactor_pid used to store clients
            % - id_tree used to store monitor
            true = ets:delete(?DBS, DbName),
            true = ets:delete(?PIDS, Pid),
            true = ets:delete(?MONITORS, DbName),
            true = ets:delete(?IDLE, DbName),
            exit(Pid, kill),
            gen_server:cast(MonPid, stop),
            [gen_server:reply(F, not_found) || F <- Froms],
            db_closed(Server, Db#db.options);
        [#db{main_pid=Pid} = Db] ->
            [#mon{pid = MonPid}] = ets:lookup(?MONITORS, DbName),
            true = ets:delete(?DBS, DbName),
            true = ets:delete(?PIDS, Pid),
            true = ets:delete(?MONITORS, DbName),
            true = ets:delete(?IDLE, DbName),
            exit(Pid, kill),
            gen_server:cast(MonPid, stop),
            db_closed(Server, Db#db.options)
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
    case (catch ets:lookup_element(?DBS, DbName, #db.instance_start_time)) of
        StartTime ->
            true = ets:insert(?DBS, Db);
        _ ->
            ok
    end,
    {reply, ok, Server}.


handle_cast({incref, DbName, Client}, Server) ->
    case ets:lookup(?MONITORS, DbName) of
        [#mon{pid = Pid}] ->
            true = ets:delete(?IDLE, DbName),
            gen_server:cast(Pid, {incref, Client});
        [] ->
            ok
    end,
    {noreply, Server};
handle_cast({decref, DbName, Client}, Server) ->
    case ets:lookup(?MONITORS, DbName) of
        [#mon{pid = Pid} = Mon] ->
            case gen_server:call(Pid, {decref, Client}) of
                true ->
                    ets:insert(?IDLE, Mon);
                false ->
                    ok
            end;
        [] ->
            ok
    end,
    {noreply, Server};
handle_cast({idle, DbName, MonPid}, Server) ->
    case is_process_alive(MonPid) of
        true ->
            case gen_server:call(MonPid, is_idle) of
                0 ->
                    ets:insert(?IDLE, #mon{name = DbName, pid = MonPid});
                _ ->
                    % A client requested this database while the
                    % idle message was in flight so ignore.
                    ok
            end;
        false ->
            % We already closed this monitor because
            % it was idle. Ignore this notification
            ok
    end,
    {noreply, Server};
handle_cast(Msg, Server) ->
    {stop, {unknown_cast_message, Msg}, Server}.

code_change(_OldVsn, #server{}=State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _Pid, config_change}, Server) ->
    {stop, config_change, Server};
handle_info({'EXIT', Pid, Reason}, Server) ->
    case ets:lookup(?PIDS, Pid) of
    [{Pid, DbName}] ->
        [#db{compactor_pid=Froms}=Db] = ets:lookup(?DBS, DbName),
        if Reason /= snappy_nif_not_loaded -> ok; true ->
            Msg = io_lib:format("To open the database `~s`, Apache CouchDB "
                "must be built with Erlang OTP R13B04 or higher.", [DbName]),
            couch_log:error(Msg, [])
        end,
        couch_log:info("db ~s died with reason ~p", [DbName, Reason]),
        % icky hack of field values - compactor_pid used to store clients
        if is_list(Froms) ->
            [gen_server:reply(From, Reason) || From <- Froms],
            #mon{pid = MonPid} = Db#db.id_tree,
            gen_server:cast(MonPid, stop);
        true ->
            [#mon{pid = MonPid}] = ets:lookup(?MONITORS, DbName),
            gen_server:cast(MonPid, close)
        end,
        true = ets:delete(?DBS, DbName),
        true = ets:delete(?PIDS, Pid),
        true = ets:delete(?MONITORS, DbName),
        true = ets:delete(?IDLE, DbName),
        {noreply, db_closed(Server, Db#db.options)};
    [] ->
        {noreply, Server}
    end;
handle_info(restart_config_listener, State) ->
    ok = config:listen_for_changes(?MODULE, nil),
    {noreply, State};
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
