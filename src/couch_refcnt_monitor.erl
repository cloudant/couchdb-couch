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

-module(couch_refcnt_monitor).
-behavior(gen_server).


-export([
    spawn_link/3,
    close/1,

    set_refcnt/2,
    set_mon_pid/2
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
    name,
    update_idle,
    table,
    refcnt,
    mon_ref
}).


spawn_link(Name, UpdateIdle, IdleTable) ->
    proc_lib:spawn_link(?MODULE, init, [{Name, UpdateIdle, IdleTable}]).


close(Monitor) ->
    gen_server:cast(Monitor, close).


set_refcnt(Monitor, RefCnt) ->
    gen_server:cast(Monitor, {set_refcnt, RefCnt}).


set_mon_pid(Monitor, Pid) ->
    gen_server:cast(Monitor, {set_mon_pid, Pid}).


init({Name, UpdateIdle, IdleTable}) ->
    St = #st{
        name = Name,
        update_idle = UpdateIdle,
        table = IdleTable
    },
    gen_server:enter_loop(?MODULE, [], St).


terminate(_Reason, St) ->
    true = ets:delete(St#st.table, St#st.name),
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(close, St) ->
    {stop, normal, St};

handle_cast({set_refcnt, RefCnt}, #st{refcnt = undefined} = St) ->
    {noreply, St#st{refcnt = RefCnt}};

handle_cast({set_mon_pid, Pid}, #st{mon_ref = Ref} = St) ->
    if not is_reference(Ref) -> ok; true ->
        erlang:demonitor(Ref, [flush])
    end,
    NewRef = erlang:monitor(process, Pid),
    {noreply, St#st{mon_ref = NewRef}}.


handle_info({refcnt, _}, #st{update_idle = true} = St) ->
    #st{
        name = Name,
        refcnt = RefCnt,
        table = Table
    } = St,
    case couch_refcnt:get_count(RefCnt) of
        {ok, 1} ->
            true = ets:insert(Table, {Name});
        {ok, _} ->
            true = ets:delete(Table, Name)
    end,
    {noreply, St};

handle_info({refcnt, _}, St) ->
    {noreply, St};

handle_info({'DOWN', Ref, _, _, _}, #st{mon_ref = Ref} = St) ->
    {stop, normal, St}.


code_change(_Vsn, St, _Extra) ->
    {ok, St}.
