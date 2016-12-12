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


-export([
    spawn_link/2,
    close/1,
    set_db_pid/2,

    notify/1,
    notify/2,
    cancel/2
]).

-export([
    init/2
]).


-record(st, {
    dbname,
    is_sys_db,
    db_ref,
    client_refs,
    closing
}).


-define(COUNTERS, couch_dbs_counters).
-define(IDLE, couch_dbs_idle).


spawn_link(DbName, IsSysDb) ->
    erlang:spawn_link(?MODULE, init, [DbName, IsSysDb]).


close(Monitor) ->
    Monitor ! exit,
    ok.


set_db_pid(Monitor, DbPid) ->
    Monitor ! {set_db_pid, DbPid},
    ok.


notify(Monitor) ->
    notify(Monitor, self()).


notify(Monitor, Client) when is_pid(Client) ->
    Monitor ! {notify, Client},
    ok;

notify(Monitor, {Client, _}) when is_pid(Client) ->
    notify(Monitor, Client).


cancel(DbName, {Client, Monitor, IsSysDb})
        when Client == self(), is_pid(Monitor) ->
    Monitor ! {cancel, self()},
    case (catch ets:update_counter(?COUNTERS, DbName, -1)) of
        0 when not IsSysDb ->
            true = ets:insert(?IDLE, {DbName}),
            ok;
        _ ->
            ok
    end;

% This happens if a #db{} record is shared across processes
% like fabric does with fabric_util:get_db/2
cancel(_DbName, {Client, Monitor, _}) when is_pid(Client), is_pid(Monitor) ->
    ok.


init(DbName, IsSysDb) ->
    {ok, CRefs} = khash:new(),
    loop(#st{
        dbname = DbName,
        is_sys_db = IsSysDb,
        db_ref = undefined,
        client_refs = CRefs,
        closing = false
    }).



handle_info(exit, St) ->
    {stop, normal, St};

handle_info({set_db_pid, Pid}, #st{db_ref = undefined} = St) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, St#st{db_ref = Ref}};

handle_info({set_db_pid, Pid}, #st{db_ref = Ref} = St) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]),
    handle_info({set_db_pid, Pid}, St#st{db_ref = undefined});

handle_info({notify, Client}, St) when is_pid(Client) ->
    case khash:get(St#st.client_refs, Client) of
        {Ref, Count} when is_reference(Ref), is_integer(Count), Count > 0 ->
            khash:put(St#st.client_refs, Client, {Ref, Count + 1});
        undefined ->
            Ref = erlang:monitor(process, Client),
            case khash:size(St#st.client_refs) of
                0 ->
                    % Our first monitor after being idle
                    khash:put(St#st.client_refs, Client, {Ref, 1}),
                    true = ets:delete(?IDLE, St#st.dbname);
                N when is_integer(N), N > 0 ->
                    % Still not idle
                    khash:put(St#st.client_refs, Client, {Ref, 1}),
                    ok
            end
    end,
    {noreply, St};

handle_info({cancel, Client}, St) when is_pid(Client) ->
    case khash:get(St#st.client_refs, Client) of
        {Ref, 1} when is_reference(Ref) ->
            erlang:demonitor(Ref, [flush]),
            khash:del(St#st.client_refs, Client),
            maybe_set_idle(St);
        {Ref, Count} when is_reference(Ref), is_integer(Count), Count > 1 ->
            khash:put(St#st.client_refs, Client, {Ref, Count - 1})
    end,
    {noreply, St};

handle_info({'DOWN', Ref, process, _, _}, #st{db_ref = Ref} = St) ->
    {stop, normal, St};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    case khash:get(St#st.client_refs, Pid) of
        {Ref, N} when is_reference(Ref), is_integer(N), N > 0 ->
            ets:update_counter(?COUNTERS, St#st.dbname, -N),
            khash:del(St#st.client_refs, Pid),
            maybe_set_idle(St);
        undefined ->
            % Ignore unknown processes
            ok
    end,
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


maybe_set_idle(St) ->
    case khash:size(St#st.client_refs) of
        0 when St#st.is_sys_db ->
            % System dbs don't go idle so they're
            % never a candidate to get closed
            ok;
        0 ->
            % We're now idle
            true = ets:insert(?IDLE, {St#st.dbname});
        N when is_integer(N), N > 0 ->
            % We have other clients
            ok
    end.


loop(St) ->
    receive
        Other ->
            do_handle_info(Other, St)
    end.


do_handle_info(Msg, St) ->
    try handle_info(Msg, St) of
        {noreply, NewSt} ->
            loop(NewSt);
        {stop, Reason, _NewSt} ->
            exit(Reason)
    catch T:R ->
        exit({T, R})
    end.
