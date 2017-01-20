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


-define(DEFAULT_RATE, 5000).
-define(RATE_ADJ, 100).
-define(WRITE_DEBUG, fun ?MODULE:write_debug/3).


-record(st, {
    name,
    target,
    second,
    count,
    rate
}).


start_link(Name, Target) ->
    proc_lib:start_link(?MODULE, init, [self(), Name, Target]).


init(Parent, Name, Target) ->
    process_flag(priority, high),
    try register(Name, self()) of
        true ->
            proc_lib:init_ack(Parent, {ok, self()}),
            Debug = sys:debug_options([]),
            {_, Second, _} = os:timestamp(),
            St = #st{
                name = Name,
                target = Target,
                second = Second,
                count = 0,
                rate = ?DEFAULT_RATE
            },
            loop(Parent, Debug, St)
    catch error:_ ->
        proc_lib:init_ack(Parent, {error, {already_started, whereis(Name)}})
    end.


loop(Parent, Debug, St) ->
    #st{
        name = Name,
        target = Target,
        second = Second,
        count = Count,
        rate = Rate
    } = St,

    {_, NewSecond, MicroSeconds} = os:timestamp(),

    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, Name, Debug, St);

        Msg ->
            TargetPid = whereis(Target),

            %%InLoc = {in, loop, Msg},
            %%Debug2 = sys:handle_debug(Debug, ?WRITE_DEBUG, Name, InLoc),

            NewCount = 1 + (if NewSecond == Second -> Count; true -> 0 end),
            NewRate = if NewCount < Rate -> Rate; true ->
                SleepMilli = 1000 - (MicroSeconds div 1000),
                timer:sleep(SleepMilli),

                case process_info(TargetPid, message_queue_len) of
                    {_, 0} -> Rate + ?RATE_ADJ;
                    {_, N} -> Rate - erlang:min(?RATE_ADJ, N)
                end
            end,

            put(status, {NewCount, NewRate}),

            TargetPid ! Msg,

            %%OutLoc = {out, loop, Msg},
            %%Debug3 = sys:handle_debug(Debug2, ?WRITE_DEBUG, Name, OutLoc),

            NewSt = St#st{
                second = NewSecond,
                count = NewCount,
                rate = NewRate
            },
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
