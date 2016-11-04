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
-export([close_lru/0]).

% config_listener api
-export([handle_config_change/5, handle_config_terminate/3]).

-include_lib("couch/include/couch_db.hrl").

-define(DBS, couch_dbs).
-define(MONITORS, couch_dbs_monitors).
-define(PIDS, couch_dbs_pid_to_name).
-define(IDLE, couch_dbs_idle).

-define(MAX_DBS_OPEN, 100).
-define(RELISTEN_DELAY, 5000).

-record(server,{
    root_dir = [],
    max_dbs_open=?MAX_DBS_OPEN,
    dbs_open=0,
    start_time=""
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


open(DbName, Options) ->
    Timeout = couch_util:get_value(timeout, Options, infinity),
    case ets:lookup(?DBS, DbName) of
        [{DbName, Monitor}] ->
            case couch_db_monitor:open_db(Monitor, Options) of
                noproc ->
                    gen_server:call(?MODULE, {open, DbName, Options}, Timeout);
                Else ->
                    Else
            end;
        [] ->
            gen_server:call(?MODULE, {open, DbName, Options}, Timeout)
    end.


create(DbName, Options) ->
    gen_server:call(couch_server, {create, DbName, Options}, infinity).

delete(DbName, Options) ->
    gen_server:call(couch_server, {delete, DbName, Options}, infinity).

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
    ets:new(?DBS, [set, protected, named_table]),
    ets:new(?PIDS, [set, protected, named_table]),
    ets:new(?IDLE, [set, public, named_table]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                max_dbs_open=MaxDbsOpen,
                start_time=couch_util:rfc1123_date()}}.

terminate(Reason, Srv) ->
    couch_log:error("couch_server terminating with ~p, state ~2048p",
                    [Reason,
                     Srv#server{lru = redacted}]),
    ets:foldl(fun(#db{main_pid=Pid}, _) -> couch_util:shutdown_sync(Pid) end,
        nil, ?DBS),
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


maybe_close_idle_db(#server{dbs_open = Open, max_dbs_open = Max} = Server)
        when Open < Max ->
    {ok, Server};
maybe_close_idle_db(Server) ->
    try
        close_idle_db(),
        {ok, db_closed(Server, [])}
    catch error:all_dbs_active ->
        {error, all_dbs_active}
    end.


close_idle_db() ->
    ets:safe_fixtable(?IDLE, true),
    try
        close_idle_db(ets:first(?IDLE))
    after
        ets:safe_fixtable(?IDLE, false)
    end.


close_idle_db('$end_of_table') ->
    erlang:error(all_dbs_active);

close_idle_db(DbName) when is_binary(DbName) ->
    [{DbName, MonitorPid}] = ets:lookup(?DBS, DbName),
    case couch_db_monitor:close(MonitorPid) of
        ok ->
            true = ets:delete(?DBS, DbName),
            true = ets:delete(?PIDS, MonitorPid),
            true = ets:delete(?IDLE, DbName);
        not_idle ->
            close_idle_db(ets:next(?IDLE, DbName))
    end.


handle_call(open_dbs_count, _From, Server) ->
    {reply, Server#server.dbs_open, Server};
handle_call({set_update_lru_on_read, UpdateOnRead}, _From, Server) ->
    {reply, ok, Server#server{update_lru_on_read=UpdateOnRead}};
handle_call({set_max_dbs_open, Max}, _From, Server) ->
    {reply, ok, Server#server{max_dbs_open=Max}};
handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call({Action, DbName, Options} = Req, From, Server) ->
        when Action == open; Action == create ->
    case ets:lookup(?DBS, DbName) of
        [{DbName, Monitor}] ->
            case couch_db_monitor:forward(Monitor, From, {Action, Options}) of
                ok ->
                    {noreply, Server};
                noproc ->
                    create_monitor(Action, DbName, Options, From, Server)
            end;
        [] ->
            create_monitor(Action, DbName, Options, From, Server)

    end;
handle_call({delete, DbName, Options}, _From, Server) ->
    #server{
        root_dir = RootDir
    } = Server,
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
        ok ->
            FullFilepath = get_full_filename(Server, DbNameList),
            Server2 = case ets:lookup(?DBS, DbName) of
                [] ->
                    Server;
                [{DbName, MonitorPid}] ->
                    ok = couch_db_monitor:close(MonitorPid),
                    true = ets:delete(?DBS, DbName),
                    true = ets:delete(?PIDS, MonitorPid),
                    true = ets:delete(?IDLE, DbName),
                    db_closed(Server)
            end;

            %% Delete any leftover compaction files. If we don't do this a
            %% subsequent request for this DB will try to open them to use
            %% as a recovery.
            lists:foreach(fun(Ext) ->
                couch_file:delete(RootDir, FullFilepath ++ Ext)
            end, [".compact", ".compact.data", ".compact.meta"]),

            couch_db_plugin:on_delete(DbName, Options),

            DelOpt = [{context, delete} | Options],
            case couch_file:delete(RootDir, FullFilepath, DelOpt) of
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
    end.

handle_cast(Msg, Server) ->
    {stop, {unknown_cast_message, Msg}, Server}.

code_change(_OldVsn, #server{}=State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _Pid, config_change}, Server) ->
    {stop, config_change, Server};
handle_info({'EXIT', Pid, Reason}, Server) ->
    case ets:lookup(?PIDS, Pid) of
        [{Pid, DbName}] ->
            [{DbName, Pid, Options}] = ets:lookup(?DBS, DbName),
            true = ets:delete(?DBS, DbName),
            true = ets:delete(?PIDS, Pid),
            true = ets:delete(?IDLE, DbName),
            {noreply, db_closed(Server, Options)};
        [] ->
            Msg = "Monitor ~p failed to exit cleanly: ~r",
            couch_log:error(Msg, [Pid, Reason])
    end;
handle_info(restart_config_listener, State) ->
    ok = config:listen_for_changes(?MODULE, nil),
    {noreply, State};
handle_info(Info, Server) ->
    {stop, {unknown_message, Info}, Server}.


create_monitor(Action, DbName, Options, Action, Server) ->
    #server{
        root_dir = RootDir
    } = Server,
    case make_room(Server, Options) of
        {ok, Server2} ->
            {ok, Monitor} = couch_db_monitor:start_link(RootDir, DbName),
            true = ets:insert_new(?DBS, {DbName, Monitor}),
            true = ets:insert_new(?PIDS, {Monitor, DbName}),
            ok = couch_db_monitor:forward(Monitor, From, {Action, Options}),
            {noreply, Server2};
        Error ->
            {reply, Error, Server}
    end.


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
