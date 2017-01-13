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

-module(couch_db_tests).

-include_lib("couch/include/couch_eunit.hrl").

-define(TIMEOUT, 120).


setup() ->
    Ctx = test_util:start_couch(),
    config:set("log", "include_sasl", "false", false),
    Ctx.


create_delete_db_test_()->
    {
        "Database create/delete tests",
        {
            setup,
            fun setup/0, fun test_util:stop_couch/1,
            fun(_) ->
                [should_create_db(),
                 should_delete_db(),
                 should_create_multiple_dbs(),
                 should_delete_multiple_dbs(),
                 should_create_delete_database_continuously()]
            end
        }
    }.

open_db_test_()->
    {
        "Database open tests",
        {
            setup,
            fun setup/0,
            fun test_util:stop_couch/1,
            [
                fun should_create_db_if_missing/0,
                fun should_have_db_return_to_idle/0
            ]
        }
    }.


should_create_db() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, []),
    ok = couch_db:close(Db),
    {ok, AllDbs} = couch_server:all_databases(),
    ?_assert(lists:member(DbName, AllDbs)).

should_delete_db() ->
    DbName = ?tempdb(),
    couch_db:create(DbName, []),
    couch_server:delete(DbName, []),
    {ok, AllDbs} = couch_server:all_databases(),
    ?_assertNot(lists:member(DbName, AllDbs)).

should_create_multiple_dbs() ->
    gen_server:call(couch_server, {set_max_dbs_open, 3}),

    DbNames = [?tempdb() || _ <- lists:seq(1, 6)],
    lists:foreach(fun(DbName) ->
        {ok, Db} = couch_db:create(DbName, []),
        ok = couch_db:close(Db)
    end, DbNames),

    {ok, AllDbs} = couch_server:all_databases(),
    NumCreated = lists:foldl(fun(DbName, Acc) ->
        ?assert(lists:member(DbName, AllDbs)),
        Acc+1
    end, 0, DbNames),

    ?_assertEqual(NumCreated, 6).

should_delete_multiple_dbs() ->
    DbNames = [?tempdb() || _ <- lists:seq(1, 6)],
    lists:foreach(fun(DbName) ->
        {ok, Db} = couch_db:create(DbName, []),
        ok = couch_db:close(Db)
    end, DbNames),

    lists:foreach(fun(DbName) ->
        ok = couch_server:delete(DbName, [])
    end, DbNames),

    {ok, AllDbs} = couch_server:all_databases(),
    NumDeleted = lists:foldl(fun(DbName, Acc) ->
        ?assertNot(lists:member(DbName, AllDbs)),
        Acc + 1
    end, 0, DbNames),

    ?_assertEqual(NumDeleted, 6).

should_create_delete_database_continuously() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, []),
    couch_db:close(Db),
    [{timeout, ?TIMEOUT, {integer_to_list(N) ++ " times",
                           ?_assert(loop(DbName, N))}}
     || N <- [10, 100]].

should_create_db_if_missing() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:open(DbName, [{create_if_missing, true}]),
    ok = couch_db:close(Db),
    {ok, AllDbs} = couch_server:all_databases(),
    ?assert(lists:member(DbName, AllDbs)).

should_have_db_return_to_idle() ->
    DbName = ?tempdb(),
    {ok, Db0} = couch_db:create(DbName, []),
    ?assertEqual([], ets:lookup(couch_dbs_idle, DbName)),
    couch_db:close(Db0),
    timer:sleep(100),
    ?assertEqual([{DbName}], ets:lookup(couch_dbs_idle, DbName)),
    lists:foreach(fun(_) ->
        {ok, Db1} = couch_db:open_int(DbName, []),
        couch_db:close(Db1),
        {_, Ref1} = spawn_monitor(fun() ->
            {ok, _Db2} = couch_db:open_int(DbName, [])
        end),
        receive {'DOWN', Ref1, _, _, _} -> ok end,
        {_, Ref2} = spawn_monitor(fun() ->
            {ok, Db2} = couch_db:open_int(DbName, []),
            couch_db:close(Db2)
        end),
        receive {'DOWN', Ref2, _, _, _} -> ok end,
        {_, Ref3} = spawn_monitor(fun() ->
            {ok, Db3} = couch_db:open_int(DbName, []),
            {ok, _Db4} = couch_db:reopen(Db3)
        end),
        receive {'DOWN', Ref3, _, _, _} -> ok end,
        {ok, Db5} = couch_db:open_int(DbName, []),
        {_, Ref4} = spawn_monitor(fun() ->
            couch_db:close(Db5)
        end),
        receive {'DOWN', Ref4, _, _, _} -> ok end,
        couch_db:close(Db5)
    end, lists:seq(1, 10)),
    timer:sleep(500),
    ?assertEqual([{DbName}], ets:lookup(couch_dbs_idle, DbName)).

loop(_, 0) ->
    true;
loop(DbName, N) ->
    ok = cycle(DbName),
    loop(DbName, N - 1).

cycle(DbName) ->
    ok = couch_server:delete(DbName, []),
    {ok, Db} = couch_db:create(DbName, []),
    couch_db:close(Db),
    ok.
