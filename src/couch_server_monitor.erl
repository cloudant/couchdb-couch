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
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/3
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
    ref_count,
    is_sys_db
}).


-define(COUCH_SERVER, couch_server).


start_link(DbName, Client, IsSysDb) ->
    gen_server:start_link(?MODULE, {DbName, Client, IsSysDb}, []).


init({DbName, Client, IsSysDb}) ->
    erlang:monitor(process, Pid),
    {ok, #st{
        name = DbName,
        ref_count = 1,
        is_sys_db = IsSysDb
    }}.


terminate(_Reason, _St) ->
    ok.


handle_call(get_ref_count, _From, St) ->
    {reply, St#st.ref_count, St};

handle_call(Msg, _From, St) ->
    {reply, {bad_call, Msg}, St}.


handle_cast({incref, Client}, St) ->
    erlang:monitor(process, Client),
    {noreply, St#st{
        ref_count = St#st.ref_count + 1
    }};

handle_cast(stop, St) ->
    {stop, normal, St};

handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info({'DOWN', _Ref, process, _Pid, _Reason}, St) ->
    NewSt = St#st{
        ref_count = St#st.ref_count - 1
    },
    HasClients = NewSt#st.ref_count > 0,
    if HasClients or NewSt#st.is_sys_db -> ok; true ->
        gen_server:cast(?COUCH_SERVER, {idle, NewSt#st.name, self()})
    end,
    {noreply, NewSt};

handle_info(_Msg, St) ->
    {noreply, St}.


code_change(_Vsn, St, _Extra) ->
    {ok, St}.
