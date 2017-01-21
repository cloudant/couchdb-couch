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
    spawn_link/3,
    close/1,
    set_pid/2,

    notify/1,
    notify/2,
    cancel/3
]).

-export([
    init/3
]).

-export([behaviour_info/1]).
-export([
    opened/2,
    closed/2,
    new/2,
    delete/3,
    insert/4,
    lookup/3,
    maybe_close_idle/1,
    incref/2,
    num_open/1,
    set_max_open/2
]).

-record(mon_state, {
    mod,
    max_open,
    open=0,
    sys_open=0
}).


-record(st, {
    mod,
    name,
    type,
    is_sys,
    ref,
    client_refs,
    closing
}).


spawn_link(Mod, Name, IsSys) ->
    erlang:spawn_link(?MODULE, init, [Mod, Name, IsSys]).


close(Monitor) ->
    Monitor ! exit,
    ok.


set_pid(Monitor, Pid) ->
    Monitor ! {set_pid, Pid},
    ok.


notify(Monitor) ->
    notify(Monitor, self()).


notify(Monitor, Client) when is_pid(Client) ->
    Monitor ! {notify, Client},
    ok;

notify(Monitor, {Client, _}) when is_pid(Client) ->
    notify(Monitor, Client).


cancel(Mod, Name, {Client, Monitor, IsSys})
        when Client == self(), is_pid(Monitor) ->
    Monitor ! {cancel, self()},
    case (catch ets:update_counter(Mod:tab_name(counters), Name, -1)) of
        0 when not IsSys ->
            true = insert(Mod, idle, Name, ok),
            ok;
        _ ->
            ok
    end;

% This happens if a #db{} record is shared across processes
% like fabric does with fabric_util:get_db/2
cancel(_Mod, _Name, {Client, Monitor, _})
        when is_pid(Client), is_pid(Monitor) ->
    ok.


init(Mod, Name, IsSys) ->
    {ok, CRefs} = khash:new(),
    loop(#st{
        name = Name,
        mod = Mod,
        is_sys = IsSys,
        ref = undefined,
        client_refs = CRefs,
        closing = false
    }).



handle_info(exit, St) ->
    {stop, normal, St};

handle_info({set_pid, Pid}, #st{ref = undefined} = St) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, St#st{ref = Ref}};

handle_info({set_pid, Pid}, #st{ref = Ref} = St) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]),
    handle_info({set_pid, Pid}, St#st{ref = undefined});

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
                    delete(St#st.mod, idle, St#st.name);
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

handle_info({'DOWN', Ref, process, _, _}, #st{ref = Ref} = St) ->
    {stop, normal, St};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    #st{name=Name, mod=Mod} = St,
    case khash:get(St#st.client_refs, Pid) of
        {Ref, N} when is_reference(Ref), is_integer(N), N > 0 ->
            ets:update_counter(Mod:tab_name(counters), Name, -N),
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
        0 when St#st.is_sys ->
            % System dbs don't go idle so they're
            % never a candidate to get closed
            ok;
        0 ->
            % We're now idle
            insert(St#st.mod, idle, St#st.name, {});
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


behaviour_info(callbacks) ->
    [{close,2}, {tab_name,1}];


behaviour_info(_) ->
    undefined.


opened(State, IsSysOwned) ->
    case IsSysOwned of
        true -> State#mon_state{sys_open=State#mon_state.sys_open + 1};
        false -> State#mon_state{open=State#mon_state.open + 1}
    end.


closed(State, IsSysOwned) ->
    case IsSysOwned of
        true -> State#mon_state{sys_open=State#mon_state.sys_open - 1};
        false -> State#mon_state{open=State#mon_state.open - 1}
    end.


-spec new(atom(), pos_integer()) -> #mon_state{}.
new(Mod, MaxOpen) ->
    ets:new(Mod:tab_name(name), [public, set, named_table]),
    ets:new(Mod:tab_name(pid), [public, set, named_table]),
    ets:new(Mod:tab_name(counters), [set, public, named_table]),
    ets:new(Mod:tab_name(idle), [set, public, named_table]),
    #mon_state{mod=Mod, max_open=MaxOpen}.


delete(State, Name, Pid) when is_record(State, mon_state) ->
    #mon_state{mod=Mod} = State,
    delete(Mod, Name, Pid);

delete(Mod, Name, Pid) when is_atom(Mod) ->
    true = ets:delete(Mod:tab_name(counters), Name),
    true = ets:delete(Mod:tab_name(name), Name),
    true = ets:delete(Mod:tab_name(pid), Pid),
    true = ets:delete(Mod:tab_name(idle), Name),
    ok.


insert(State, Tab, Key, Value) when is_record(State, mon_state) ->
    insert(State#mon_state.mod, Tab, Key, Value);

insert(Mod, Tab, Key, Value) when is_atom(Mod) ->
    ets:insert(Mod:tab_name(Tab), {Key, Value}).


lookup(State, Type, Key) when is_record(State, mon_state) ->
    lookup(State#mon_state.mod, Type, Key);

lookup(Mod, Type, Key) ->
    case ets:lookup(Mod:tab_name(Type), Key) of
        [{_Key, Value}] -> {ok, Value};
        [] -> not_found
    end.


maybe_close_idle(#mon_state{open=Open, max_open=Max}=State) when Open < Max ->
    {ok, State};

maybe_close_idle(State) ->
    try
        close_idle(State),
        {ok, State}
    catch error:all_active ->
        {error, all_active}
    end.

set_max_open(State, Max) ->
    #mon_state{max_open=OldMax} = State,
    lists:foldl(fun(_, StateAcc) ->
        {ok, NewStateAcc} = maybe_close_idle(StateAcc),
        NewStateAcc
    end, State#mon_state{max_open=Max}, lists:seq(1, max(0, OldMax-Max))).


close_idle(State) ->
    #mon_state{mod=Mod} = State,
    ets:safe_fixtable(Mod:tab_name(idle), true),
    try
        close_idle(State, ets:first(Mod:tab_name(idle)))
    after
        ets:safe_fixtable(Mod:tab_name(idle), false)
    end.


close_idle(_State, '$end_of_table') ->
    erlang:error(all_active);

close_idle(State, Name) when is_binary(Name) ->
    #mon_state{mod=Mod} = State,
    case lookup(Mod, name, Name) of
        {ok, Value} ->
            true = ets:delete(Mod:tab_name(idle), Name),
            case Mod:close(Name, Value) of
                {true, SysOwned} ->
                    closed(State, SysOwned);
                false ->
                    close_idle(State, ets:next(Mod:tab_name(idle), Name))
            end;
        not_found ->
            true = ets:delete(Mod:tab_name(idle), Name),
            close_idle(State, ets:next(Mod:tab_name(idle), Name))
    end.


% incref is the only state-updating operation which can be performed
% concurrently
incref(State, Name) when is_record(State, mon_state) ->
    incref(State#mon_state.mod, Name);

incref(Mod, Name) when is_atom(Mod) ->
    case (catch ets:update_counter(Mod:tab_name(counters), Name, 1)) of
        1 ->
            ets:delete(Mod:tab_name(idle), Name),
            ok;
        N when is_integer(N), N > 0 ->
            ok;
        N when is_integer(N) ->
            {invalid_refcount, N};
        {'EXIT', {badarg, _}} ->
            missing_counter
    end.


num_open(State) ->
    State#mon_state.open.
