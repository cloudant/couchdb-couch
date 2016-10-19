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
    refs,
    is_sys_db
}).


-define(COUCH_SERVER, couch_server).


start_link(DbName, Client, IsSysDb) ->
    gen_server:start_link(?MODULE, {DbName, Client, IsSysDb}, []).


init({DbName, Client, IsSysDb}) ->
    Ref = erlang:monitor(process, Client),
    % Values are lists of refs because a single client
    % can open a db more than once.
    {ok, Refs} = khash:from_list([{Client, [Ref]}]),
    {ok, #st{
        name = DbName,
        refs = Refs,
        is_sys_db = IsSysDb
    }}.


terminate(_Reason, _St) ->
    ok.


handle_call(is_idle, _From, St) ->
    Reply = case khash:size(St#st.refs) of
        0 -> true;
        _ -> false
    end,
    {reply, Reply, St};

handle_call(Msg, _From, St) ->
    {reply, {bad_call, Msg}, St}.


handle_cast({incref, Client}, St) ->
    Ref = erlang:monitor(process, Client),
    ExistingRefs = khash:get(St#st.refs, Client, []),
    khash:put(St#st.refs, Client, [Ref | ExistingRefs]),
    {noreply, St};

handle_cast({decref, Client}, St) ->
    {value, [Ref | RestRefs]} = khash:lookup(St#st.refs, Client),
    erlang:demonitor(Ref, [flush]),
    maybe_remove_client(St, Client, RestRefs),
    {noreply, St};

handle_cast(stop, St) ->
    {stop, normal, St};

handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info({'DOWN', Ref, process, Pid, _Reason}, St) ->
    remove_ref(St, Pid, Ref),
    {noreply, St};

handle_info(_Msg, St) ->
    {noreply, St}.


code_change(_Vsn, St, _Extra) ->
    {ok, St}.


remove_ref(St, Client, Ref) ->
    {value, ExistingRefs} = khash:get(St#st.refs, Client),
    RestRefs = ExistingRefs -- [Ref],
    true = RestRefs /= ExistingRefs,
    maybe_remove_client(St, Client, RestRefs).


maybe_remove_client(St, Client, []) ->
    khash:del(St#st.refs, Client),
    maybe_send_idle(St);

maybe_remove_client(St, Client, Refs) ->
    khash:put(St#st.refs, Client, Refs).


maybe_send_idle(St) ->
    HasClients = khash:size(St#st.refs) > 0,
    if HasClients or St#st.is_sys_db -> ok; true ->
        gen_server:cast(?COUCH_SERVER, {idle, St#st.name, self()})
    end.
