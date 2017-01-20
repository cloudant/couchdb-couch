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

-module(couch_lru).
-export([new/0, insert/2, update/2, close/1]).

-include_lib("couch/include/couch_db.hrl").

new() ->
    {ok, KHash} = khash:new(),
    KHash.

insert(DbName, KHash) ->
    ok = khash:put(KHash, DbName, nil),
    KHash.

update(_DbName, KHash) ->
    KHash.

close(KHash) ->
    {ok, Iter} = khash:iter(KHash),
    close_int(get_next(Iter), Iter, KHash).

%% internals

close_int(end_of_table, _Iter, _KHash) ->
    erlang:error(all_dbs_active);
close_int(DbName, Iter, KHash) ->
    case ets:update_element(couch_dbs, DbName, {#db.fd_monitor, locked}) of
    true ->
        [#db{main_pid = Pid} = Db] = ets:lookup(couch_dbs, DbName),
        case couch_db:is_idle(Db) of true ->
            true = ets:delete(couch_dbs, DbName),
            true = ets:delete(couch_dbs_pid_to_name, Pid),
            exit(Pid, kill),
            ok = khash:del(KHash, DbName),
            KHash;
        false ->
            true = ets:update_element(couch_dbs, DbName, {#db.fd_monitor, nil}),
            couch_stats:increment_counter([couchdb, couch_server, lru_skip]),
            close_int(get_next(Iter), Iter, KHash)
        end;
    false ->
        ok = khash:del(KHash, DbName),
        {ok, NewIter} = khash:iter(KHash),
        close_int(get_next(NewIter), NewIter, KHash)
    end.


get_next(Iter) ->
    case khash:iter_next(Iter) of
        {DbName, _} ->
            DbName;
        end_of_table ->
            end_of_table
    end.