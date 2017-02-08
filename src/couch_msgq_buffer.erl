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

-module(couch_msgq_buffer).
-compile(native).

-export([
    start_link/2
]).

-export([
    init/3
]).

-export([
    system_continue/3,
    system_terminate/4,
    system_get_state/1,
    system_replace_state/2,
    write_debug/3
]).


-define(BACKLOG, 5000).
-define(WRITE_DEBUG, fun ?MODULE:write_debug/3).


-record(st, {
    name,
    target,
    ts
}).


start_link(Name, Target) ->
    proc_lib:start_link(?MODULE, init, [self(), Name, Target]).


init(Parent, Name, Target) ->
    process_flag(priority, high),
    try register(Name, self()) of
        true ->
            proc_lib:init_ack(Parent, {ok, self()}),
            Debug = sys:debug_options([]),
            {_, Second, Micro} = os:timestamp(),
            St = #st{
                name = Name,
                target = Target,
                ts = Second * 1000 + Micro div 1000
            },
            loop(Parent, Debug, St)
    catch error:_ ->
        proc_lib:init_ack(Parent, {error, {already_started, whereis(Name)}})
    end.


loop(Parent, Debug, St) ->
    #st{
        name = Name,
        target = Target,
        ts = TimeStamp
    } = St,

    {_, NewSecond, NewMicro} = os:timestamp(),
    NewTimeStamp = NewSecond * 1000 + NewMicro div 1000,

    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, Name, Debug, St);
        Msg ->
            TargetPid = whereis(Target),
            TargetPid ! Msg,
            NewSt = case NewTimeStamp - TimeStamp > 10 of
                true ->
                    case process_info(TargetPid, message_queue_len) of
                        {_, N} when N < ?BACKLOG ->
                            ok;
                        {_, N} when N >= ?BACKLOG ->
                            waitfor(TargetPid)
                    end,
                    St#st{ts = NewTimeStamp};
                false ->
                    St
            end,

            loop(Parent, Debug, NewSt)
    end.


system_continue(Parent, Debug, St) ->
    loop(Parent, Debug, St).


system_terminate(Reason, _Parent, _Debug, _St) ->
    exit(Reason).


system_get_state(St) ->
    {ok, St}.


system_replace_state(StateFun, St) ->
    NewSt = StateFun(St),
    {ok, NewSt, NewSt}.


write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).


waitfor(Pid) ->
    timer:sleep(2),
    case process_info(Pid, message_queue_len) of
        {_, N} when N < ?BACKLOG ->
            ok;
        {_, N} when N >= ?BACKLOG ->
            waitfor(Pid)
    end.
