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
    start/2,
    update/1,
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
    client
}).


-define(COUCH_SERVER, couch_server).
-define(COUNTERS, couch_dbs_counters).
-define(CANCEL_TIMEOUT, 5000).


create(#db{name = DbName, fd = Fd, instance_start_time = IST}) when
        is_binary(DbName),
        is_pid(Fd),
        is_binary(IST) ->
    St = #st{
        dbname = DbName,
        instance_start_time = IST,
        client = self()
    },
    #monitor{
        ref = erlang:monitor(process, Fd),
        pid = spawn(?MODULE, init, [St]),
        client = self()
    }.


% start/2 is a special case when couch_server creates the monitor
% for a client. The client must then call update/1 on the monitor
% it receives in its response from couch_server.
start(#db{name = DbName, fd = Fd, instance_start_time = IST}, Client) when
        is_binary(DbName),
        is_pid(Fd),
        is_binary(IST),
        is_pid(Client) ->
    St = #st{
        dbname = DbName,
        instance_start_time = IST,
        client = Client
    },
    #monitor{
        ref = Fd,
        pid = spawn(?MODULE, init, [St]),
        client = Client
    }.


update(#monitor{ref = Fd} = Monitor) when is_pid(Fd) ->
    Monitor#monitor{
        ref = erlang:monitor(process, Fd)
    }.


refresh(#monitor{ref = Ref} = Monitor, Fd) when is_reference(Ref) ->
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


cancel(#monitor{ref = Ref} = Monitor) when is_reference(Ref) ->
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
    ets:update_counter(?COUNTERS, key(St), {2, 1}),
    erlang:hibernate(?MODULE, handle_msg, [St]).


terminate(St) ->
    % We may have been delayed until after the databsae
    % was closed so ignore any errors here.
    catch ets:update_counter(?COUNTERS, key(St), {2, -1}).


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
