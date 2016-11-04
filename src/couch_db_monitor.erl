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

-module(couch_db_monitor).
-behavior(gen_server).
-vsn(1).


-export([
    start_link/1,
    close/1,
    forward/3,
    open_db/1,
    close_db/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    async_init/3
]).


-record(st, {
    root_dir,
    dbname,
    status,
    db,
    create,
    opener,
    waiters
}).


-define(IDLE, couch_dbs_idle).
-define(CLOSE_TIMEOUT, 5000).


start_link(RootDir, DbName) ->
    gen_server:start_link(?MODULE, {RootDir, DbName}, []).


close(Monitor) ->
    gen_server:call(Monitor, close).


forward(Monitor, From, Msg) ->
    case (catch gen_server:call(Monitor, {forward, From, Msg})) of
        {'EXIT', {noproc, _}} ->
            noproc;
        Else ->
            Else
    end.


open_db(Monitor, Options) ->
    case (catch gen_server:call(Monitor, open_db)) of
        {'EXIT', {noproc, _}} ->
            noproc;
        Else ->
            Else
    end.


close_db(Monitor) ->
    ok = gen_server:call(Monitor, close_db).


init({RootDir, DbName}) ->
    process_flag(trap_exit, true),
    {ok, #st{
        dbname = DbName,
        status = new,
        db = undefined,
        create = undefined,
        opener = undefined,
        waiters = []
    }}.


terminate(_Reason, _St) ->
    ok.


handle_call(close, _From, #st{status = normal} = St) ->
    #st{
        db = Db
    } = St,
    case process_info(self(), monitors) of
        {monitors, []} ->
            exit(Db#db.main_pid, kill),
            NewSt = St#st{
                status = closing,
                db = undefined
            },
            {reply, ok, NewSt, ?CLOSE_TIMEOUT};
        {monitors, _} ->
            {reply, not_idle, St}
    end;

handle_call({forward, Client, {Action, Opts}}, From, #st{status = new} = St) ->
    % Don't make couch_server wait
    gen_server:reply(From, ok),
    add_monitor(Client),
    #st{
        root_dir = RootDir,
        dbname = DbName,
        waiters = Waiters
    } = St,
    Args = [self(), os:timestamp(), RootDir, DbName, Opts]
    Opener = spawn_link(?MODULE, async_init, Args),
    NewSt = St#st{
        status = Action,
        opener = Opener,
        waiters = [Client | Waiters]
    },
    {noreply, NewSt};

handle_call({forward, Client, {open, Opts}}, From, St) ->
    % Don't make couch_server wait
    gen_server:reply(From, ok),
    add_monitor(Client),
    NewSt = St#st{
        waiters = [Client | St#st.waiters]
    },
    {noreply, NewSt};

handle_call({forward, Client, {create, Opts}}, From, #st{status = open} = St) ->
    % Don't make couch_server wait
    gen_server:reply(From, ok),
    add_monitor(Client),
    NewSt = case St#st.create of
        undefined ->
            % We're trying to create a database while someone is in
            % the middle of trying to open it. We allow one creator
            % to wait while we figure out if then open will succeed.
            CreateOpts = [create | Opts],
            St#st{
                create = {Client, [create | Opts]}
            };
        _ ->
            % Someone else is already trying to create the db which
            % means we fail this one with file_exists
            gen_server:reply(Client, file_exists),
            St
    end,
    {noreply, NewSt};

handle_call(open_db, From, #st{status = normal} = St) ->
    add_monitor(From),
    {reply, {ok, St#st.db}, St};

handle_call(open_db, From, #st{status = Status} = St)
        when Status == open; Status == create ->
    add_monitor(From),
    {noreply, St#st{
        waiters = [From | St#st.waiters]
    }};

handle_call(open_db, From, #st{status = closing} = St) ->
    {reply, noproc, St, ?CLOSE_TIMEOUT};

handle_call(close_db, From, #st{status = normal} = St) ->
    remove_monitor(From),
    {reply, ok, St};

handle_call({open_result, T0, {ok, Db}}, _From, #st{status = Status} = St)
        when Status == open; Status == create ->
    link(Db#db.main_pid),
    OpenTime = timer:now_diff(os:timestamp(), T0) / 1000,
    couch_stats:update_histogram([couchdb, db_open_time], OpenTime),

    lists:foreach(fun(W) ->
        gen_server:reply(W, {ok, Db})
    end, St#st.waiters),

    % Cancel the creation request if it exists.
    case St#st.create of
        {Client, _CreateOpts} ->
            gen_server:reply(Client, file_exists);
        undefined ->
            ok
    end,

    {reply, ok, St#st{
        status = normal,
        db = Db,
        opener = undefined,
        waiters = undefined
    }};

handle_call({open_result, T0, {error, eexist}}, From, St) ->
    handle_call({open_result, T0, file_exists}, From, St);

handle_call({open_result, _T0, Error}, {Pid, _}, #st{opener = Pid} = St) ->
    #st{
        root_dir = RootDir
    } = St

    lists:foreach(fun(W) ->
        gen_server:reply(W, Error)
    end, St#st.waiters),

    couch_log:info("Error opening database ~s: ~r", [DbName, Error]),

    NewSt = case St#st.create of
        {Client, CreateOpts} ->
            Args = [self(), os:timestamp(), RootDir, DbName, CreateOpts]
            Opener = spawn_link(?MODULE, async_init, Args),
            St#st{
                status = create,
                create = undefined,
                opener = Opener,
                waiters = [Client]
            };
        undefined ->
            St#st{
                status = closing,
                db = Error,
                waiters = undefined
            }
    end,
    Timeout = case NewSt#st.status of
        create -> infinity;
        closing -> ?CLOSE_TIMEOUT
    end,
    {reply, ok, NewSt, Timeout}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', _, process, Pid, _Reason}, St) ->
    case process_info(self(), monitors) of
        {monitors, []} ->
            % Just became idle
            true = ets:delete(?IDLE, DbName);
        {monitors, [_ | _]} ->
            % Still have other clients
            ok
    end,
    {noreply, St#st{waiters = Waiters}};

handle_info(timeout, #st{status = closing} = St) ->
    {stop, normal, St};

handle_info({'EXIT', Pid, Reason}, #st{opener = Pid} = St) ->
    #st{
        dbname = DbName,
        waiters = Waiters
    } = St,

    lists:foreach(fun(W) ->
        gen_server:reply(W, Reason)
    end, St#st.waiters),

    Msg = "Failed to open db ~s with reason ~r",
    couch_log:error(Msg, [DbName, Reason]),

    {noreply, St#st{
        status = closing,
        opener = undefined,
        waiters = undefined
    }, ?CLOSE_TIMEOUT};

handle_info({'EXIT', Pid, Reason}, #st{status = normal} = St) ->
    #st{
        dbname = DbName,
        db = Db
    } = St,
    case Pid == Db#db.main_pid of
        true ->
            Msg = "db ~s closed with reason ~r",
            couch_log:error(Msg, [DbName, Reason]),
            {noreply, St#st{
                status = closing,
                db = undefined
            }, ?CLOSE_TIMEOUT};
        false ->
            Msg = "db monitor had linked process ~p die with reason ~r",
            couch_log:error(Msg, [Pid, Reason]),
            {stop, {error, Reason}, St}
    end;

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


async_init(Parent, T0, RootDir, DbName, Options0) ->
    Options = maybe_add_sys_db_callbacks(DbName, Options0),
    Res = couch_db:start_link(DbName, Filepath, Options),
    case {Res, lists:member(create, Options)} of
        {{ok, _Db}, true} ->
            couch_event:notify(DbName, created);
        _ ->
            ok
    end,
    gen_server:call(Parent, {open_result, T0, DbName, Res}, infinity),
    unlink(Parent).


add_monitor({Pid, _}) when is_pid(Pid) ->
    erlang:monitor(process, Pid),
    case process_info(self(), monitors) of
        {monitors, [_]} ->
            % Just became not idle
            true = ets:delete(?IDLE, DbName);
        {monitors, [_ | _]} ->
            % Was already not idle
            ok
    end.


remove_monitor({Pid, _}) when is_pid(Pid) ->
    erlang:demonitor(Pid, [flush]),
    case process_info(self(), monitors) of
        {monitors, []} ->
            % Just became idle
            true = ets:insert(?IDLE, {DbName});
        {monitors, [_ | _]} ->
            % Still have clients
            ok
    end.


maybe_add_sys_db_callbacks(DbName, Options) when is_binary(DbName) ->
    maybe_add_sys_db_callbacks(?b2l(DbName), Options);

maybe_add_sys_db_callbacks(DbName, Options) ->
    DbsDbName = config:get("mem3", "shards_db", "_dbs"),
    NodesDbName = config:get("mem3", "nodes_db", "_nodes"),

    IsReplicatorDb = path_ends_with(DbName, "_replicator"),
    UsersDbSuffix = config:get("couchdb", "users_db_suffix", "_users"),
    IsUsersDb = path_ends_with(DbName, "_users")
        orelse path_ends_with(DbName, UsersDbSuffix),

    case DbName of
        DbsDbName ->
    	    [sys_db | Options];
    	NodesDbName ->
    	    [sys_db | Options];
    	_ when IsReplicatorDb ->
    	    [
    	        {
    	            before_doc_update,
    	            fun couch_replicator_manager:before_doc_update/2},
    	        {
    	            after_doc_read,
    	            fun couch_replicator_manager:after_doc_read/2
    	        },
    	        sys_db
    	    ] ++ Options;
    	_ when IsUsersDb ->
    	    [
    	        {before_doc_update, fun couch_users_db:before_doc_update/2},
    	        {after_doc_read, fun couch_users_db:after_doc_read/2},
    	        sys_db
    	    ] ++ Options;
    	_ ->
    	    Options
    end.
