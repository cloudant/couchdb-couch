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
    start_link/2,
    exit/1,
    set_db_pid/2,
    is_idle/1,

    incref/1,
    incref/2,
    decref/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-record(st, {
    dbname,
    is_sys_db,
    db_ref,
    client_refs
}).


-define(IDLE, couch_dbs_idle).


start_link(DbName, IsSysDb) ->
    gen_server:start_link(?MODULE, {DbName, IsSysDb}, []).


exit(Monitor) ->
    gen_server:cast(Monitor, exit).


set_db_pid(Monitor, DbPid) ->
    gen_server:cast(Monitor, {set_db_pid, DbPid}).


is_idle(Monitor) ->
    gen_server:call(Monitor, is_idle).


incref(Monitor) ->
    incref(Monitor, self()).


incref(Monitor, Client) when is_pid(Client) ->
    case (catch gen_server:call(Monitor, {incref, Client})) of
        {'EXIT', {noproc, _}} ->
            retry;
        Else ->
            Else
    end;

incref(Monitor, {Client, _}) when is_pid(Client) ->
    incref(Monitor, Client).


decref(Monitor) ->
    gen_server:call(Monitor, decref).


init({DbName, IsSysDb}) ->
    {ok, CRefs} = khash:new(),
    {ok, #st{
        dbname = DbName,
        is_sys_db = IsSysDb,
        db_ref = undefined,
        client_refs = CRefs
    }}.


terminate(_Reason, _St) ->
    ok.


handle_call({incref, Client}, _From, St) ->
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
    {reply, ok, St};

handle_call(decref, {Pid, _}, St) ->
    case khash:get(St#st.client_refs, Pid) of
        {Ref, 1} when is_reference(Ref) ->
            erlang:demonitor(Ref, [flush]),
            khash:del(St#st.client_refs, Pid),
            maybe_set_idle(St);
        {Ref, Count} when is_reference(Ref), is_integer(Count), Count > 1 ->
            khash:put(St#st.client_refs, Pid, {Ref, Count - 1});
        undefined ->
            % Ignore for now, most likely this is from
            % fabric:get_security/1 which shares a db record
            % between processes
            ok
    end,
    {reply, ok, St};

handle_call(is_idle, _From, St) ->
    Reply = case khash:size(St#st.client_refs) of
        0 -> true;
        _ -> false
    end,
    {reply, Reply, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(exit, St) ->
    {stop, normal, St};

handle_cast({set_db_pid, Pid}, #st{db_ref = undefined} = St) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, St#st{db_ref = Ref}};

handle_cast({set_db_pid, Pid}, #st{db_ref = Ref} = St) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]),
    handle_cast({set_db_pid, Pid}, St#st{db_ref = undefined});

handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _, _}, #st{db_ref = Ref} = St) ->
    {stop, normal, St};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    khash:del(St#st.client_refs, Pid),
    maybe_set_idle(St),
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_Vsn, St, _Extra) ->
    {ok, St}.


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