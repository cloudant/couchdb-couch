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

-module(couch_server_monitor).
-vsn(1).


-export([
    create/1,
    refresh/2,
    cancel/1
]).

-export([
    init/1,
    handle_msg/1
]).


-include("couch_db.hrl").


-record(monitor, {
    ref,
    pid,
    client
}).

-record(st, {
    dbname,
    instance_start_time,
    client,
    is_sys_db
}).


-define(COUCH_SERVER, couch_server).
-define(COUNTERS, couch_dbs_counters).
-define(CANCEL_TIMEOUT, 5000).


create(#db{name = DbName, fd = Fd, instance_start_time = IST} = Db)
        when is_binary(DbName), is_pid(Fd), is_binary(IST) ->
    St = #st{
        dbname = DbName,
        instance_start_time = IST,
        client = self(),
        is_sys_db = couch_db:is_system_db(Db)
    },
    #monitor{
        ref = erlang:monitor(process, Fd),
        pid = spawn(?MODULE, init, [St]),
        client = self()
    }.


refresh(#monitor{} = Monitor, Fd) ->
    case Monitor#monitor.client == self() of
        true ->
            ok;
        false ->
            erlang:exit({bad_monitor, client_mismatch})
    end,
    #monitor{
        ref = OldRef
    } = Monitor,
    erlang:demonitor(OldRef, [flush]),
    NewRef = erlang:monitor(process, Fd),
    Monitor#monitor{
        ref = NewRef
    }.


cancel(#monitor{} = Monitor) ->
    case Monitor#monitor.client == self() of
        true ->
            do_cancel(Monitor);
        false ->
            % A db record was shared across
            % processes so we'll ignore to
            % cover fabric:get_security's bug
            ignored
    end.


do_cancel(Monitor) ->
    #monitor{
        ref = Ref,
        pid = Pid
    } = Monitor,
    erlang:demonitor(Ref, [flush]),
    MRef = erlang:monitor(process, Pid),
    Pid ! {cancel, self()},
    receive
        {'DOWN', _, _, Pid, _} ->
            ok
    after ?CANCEL_TIMEOUT ->
        erlang:demonitor(MRef, [flush]),
        erlang:error({timeout, couch_server_monitor_cancel})
    end.


init(St) ->
    erlang:monitor(process, St#st.client),
    Resp = case (catch ets:update_counter(?COUNTERS, key(St), {2, 1})) of
        1 -> gen_server:cast(?COUCH_SERVER, {not_idle, key(St)});
        N when N > 1 -> ok;
        _ -> exit
    end,
    if Resp == exit -> ok; true ->
        erlang:hibernate(?MODULE, handle_msg, [St])
    end.


terminate(St) ->
    #st{is_sys_db = IsSysDb} = St,
    case (catch ets:update_counter(?COUNTERS, key(St), {2, -1})) of
        0 when not IsSysDb ->
            gen_server:cast(?COUCH_SERVER, {idle, key(St)});
        _ ->
            ok
    end.


handle_msg(St) ->
    #st{client = Pid} = St,
    receive
        {'DOWN', _, _, Pid, _} ->
            terminate(St);
        {cancel, Pid} ->
            terminate(St);
        Other ->
            Msg = {bad_monitor_message, St#st.dbname, Other},
            gen_server:cast(?COUCH_SERVER, Msg)
    end.


key(St) ->
    {St#st.dbname, St#st.instance_start_time}.
