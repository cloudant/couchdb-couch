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

-module(couch_monitor_server).


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


behaviour_info(callbacks) ->
    [{close,2}, {tab_name,1}];


behaviour_info(_) ->
    undefined.


-spec new(atom(), pos_integer()) -> #mon_state{}.
new(Mod, MaxOpen) ->
    ets:new(Mod:tab_name(name), [public, set, named_table]),
    ets:new(Mod:tab_name(pid), [public, set, named_table]),
    ets:new(Mod:tab_name(counters), [set, public, named_table]),
    ets:new(Mod:tab_name(idle), [set, public, named_table]),
    #mon_state{mod=Mod, max_open=MaxOpen}.


-spec opened(#mon_state{}, boolean()) -> #mon_state{}.
opened(State, IsSysOwned) ->
    case IsSysOwned of
        true -> State#mon_state{sys_open=State#mon_state.sys_open + 1};
        false -> State#mon_state{open=State#mon_state.open + 1}
    end.


-spec closed(#mon_state{}, boolean()) -> #mon_state{}.
closed(State, IsSysOwned) ->
    case IsSysOwned of
        true -> State#mon_state{sys_open=State#mon_state.sys_open - 1};
        false -> State#mon_state{open=State#mon_state.open - 1}
    end.


-spec delete(#mon_state{}, term(), pid()) -> ok.
delete(State, Name, Pid) when is_record(State, mon_state) ->
    #mon_state{mod=Mod} = State,
    delete(Mod, Name, Pid);

delete(Mod, Name, Pid) when is_atom(Mod) ->
    true = ets:delete(Mod:tab_name(counters), Name),
    true = ets:delete(Mod:tab_name(name), Name),
    true = ets:delete(Mod:tab_name(pid), Pid),
    true = ets:delete(Mod:tab_name(idle), Name),
    ok.


-spec insert(#mon_state{} | atom(), atom(), term(), term()) -> ok.
insert(State, Tab, Key, Value) when is_record(State, mon_state) ->
    insert(State#mon_state.mod, Tab, Key, Value);

insert(Mod, Tab, Key, Value) when is_atom(Mod) ->
    ets:insert(Mod:tab_name(Tab), {Key, Value}).


-spec lookup(#mon_state{} | atom(), atom(), term()) -> ok.
lookup(State, Type, Key) when is_record(State, mon_state) ->
    lookup(State#mon_state.mod, Type, Key);

lookup(Mod, Type, Key) ->
    case ets:lookup(Mod:tab_name(Type), Key) of
        [{_Key, Value}] -> {ok, Value};
        [] -> not_found
    end.


-spec maybe_close_idle(#mon_state{}) -> {ok, #mon_state{}} | {error, all_active}.
maybe_close_idle(#mon_state{open=Open, max_open=Max}=State) when Open < Max ->
    {ok, State};

maybe_close_idle(State) ->
    try
        {ok, close_idle(State)}
    catch error:all_active ->
        {error, all_active}
    end.


-spec close_idle(#mon_state{}) -> #mon_state{}.
close_idle(State) ->
    #mon_state{mod=Mod} = State,
    ets:safe_fixtable(Mod:tab_name(idle), true),
    try
        close_idle(State, ets:first(Mod:tab_name(idle)))
    after
        ets:safe_fixtable(Mod:tab_name(idle), false)
    end.


-spec close_idle(#mon_state{}, term()) -> #mon_state{}.
close_idle(_State, '$end_of_table') ->
    erlang:error(all_active);

close_idle(State, Name) ->
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
-spec incref(#mon_state{} | atom(), atom()) ->
    ok | {invalid_refcount, integer()} | missing_counter.
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


-spec num_open(#mon_state{}) -> non_neg_integer().
num_open(State) ->
    State#mon_state.open.


-spec set_max_open(#mon_state{}, pos_integer()) -> #mon_state{}.
set_max_open(State, Max) ->
    #mon_state{max_open=OldMax} = State,
    lists:foldl(fun(_, StateAcc) ->
        {ok, NewStateAcc} = maybe_close_idle(StateAcc),
        NewStateAcc
    end, State#mon_state{max_open=Max}, lists:seq(1, max(0, OldMax-Max))).
                                                                                
