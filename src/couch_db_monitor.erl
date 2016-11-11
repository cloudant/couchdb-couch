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
    close_if_idle/1,
    close_if_idle/2,
    set_db_pid/2,
    is_idle/1,

    incref/1,
    incref/2,
    incref/3,
    decref/1
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


-define(IDLE, couch_dbs_idle).


spawn_link(DbName, IsSysDb) ->
    erlang:spawn_link(?MODULE, init, [DbName, IsSysDb]).


close(Monitor) ->
    Monitor ! exit,
    ok.


close_if_idle(Monitor) ->
    call(Monitor, close_if_idle, []).


close_if_idle(Monitor, Options) ->
    call(Monitor, close_if_idle, Options).


set_db_pid(Monitor, DbPid) ->
    Monitor ! {set_db_pid, DbPid},
    ok.


is_idle(Monitor) ->
    call(Monitor, is_idle).


incref(Monitor) ->
    incref(Monitor, self(), []).


incref(Monitor, Client) ->
    incref(Monitor, Client, []).


incref(Monitor, Client, Options) when is_pid(Client) ->
    case call(Monitor, {incref, Client}, Options) of
        {error, noproc} ->
            retry;
        Else ->
            Else
    end;

incref(Monitor, {Client, _}, Options) when is_pid(Client) ->
    incref(Monitor, Client, Options).


decref(Monitor) ->
    ok = call(Monitor, decref).


init(DbName, IsSysDb) ->
    {ok, CRefs} = khash:new(),
    loop(#st{
        dbname = DbName,
        is_sys_db = IsSysDb,
        db_ref = undefined,
        client_refs = CRefs,
        closing = false
    }).


handle_call({incref, _}, _From, #st{closing = true} = St) ->
    {reply, retry, St};

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

handle_call(close_if_idle, _From, St) ->
    case khash:size(St#st.client_refs) of
        0 ->
            {reply, closing, St#st{closing = true}};
        _ ->
            {reply, not_idle, St}
    end;

handle_call(is_idle, _From, St) ->
    Reply = case khash:size(St#st.client_refs) of
        0 -> true;
        _ -> false
    end,
    {reply, Reply, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_info(exit, St) ->
    {stop, normal, St};

handle_info({set_db_pid, Pid}, #st{db_ref = undefined} = St) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, St#st{db_ref = Ref}};

handle_info({set_db_pid, Pid}, #st{db_ref = Ref} = St) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]),
    handle_info({set_db_pid, Pid}, St#st{db_ref = undefined});

handle_info({'DOWN', Ref, process, _, _}, #st{db_ref = Ref} = St) ->
    {stop, normal, St};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    khash:del(St#st.client_refs, Pid),
    maybe_set_idle(St),
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


loop(#st{closing = false} = St) ->
    receive
        {call, From, Cmd} ->
            do_handle_call(Cmd, From, St);
        Other ->
            do_handle_info(Other, St)
    end;

loop(#st{closing = true} = St) ->
    receive
        {call, From, Cmd} ->
            do_handle_call(Cmd, From, St);
        Other ->
            do_handle_info(Other, St)
    after 0 ->
        exit(normal)
    end.


do_handle_call(Cmd, {Pid, Ref} = From, St) ->
    try handle_call(Cmd, From, St) of
        {reply, Msg, NewSt} ->
            Pid ! {Ref, Msg},
            loop(NewSt);
        {stop, Reason, Msg, _NewSt} ->
            Pid ! {Ref, Msg},
            exit(Reason)
    catch T:R ->
        exit({T, R})
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


call(Pid, Cmd) when is_pid(Pid) ->
    call(Pid, Cmd, []).


call(Pid, Cmd, Options) ->
    case lists:member(unmonitored, Options) of
        true ->
            Ref = erlang:make_ref(),
            Pid ! {call, {self(), Ref}, Cmd},
            receive
                {Ref, Resp} ->
                    Resp
            after 5000 ->
                erlang:error({couch_db_monitor, {timeout, Pid, Cmd}})
            end;
        false ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {call, {self(), Ref}, Cmd},
            receive
                {Ref, Resp} ->
                    Resp;
                {'DOWN', Ref, process, Pid, _Reason} ->
                    {error, noproc}
            end
    end.
