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

-module(couch_lru_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


setup() ->
    ok = meck:new(couch_db, [passthrough]),
    ets:new(couch_dbs, [set, public, named_table, {keypos, #db.name}]),
    ets:new(couch_dbs_pid_to_name, [set, public, named_table]),
    couch_lru:new().

teardown(_) ->
    ets:delete(couch_dbs),
    ets:delete(couch_dbs_pid_to_name),
    (catch meck:unload(couch_db)).


new_test_() ->
    {setup,
        fun() -> couch_lru:new() end,
        fun(Lru) ->
            ?_assertMatch({_, _}, Lru)
        end
    }.

insert_test_() ->
    {setup,
        fun() -> couch_lru:new() end,
        fun(Lru) ->
            Key = <<"test">>,
            {Tree, Dict} = couch_lru:insert(Key, Lru),
            [
                ?_assertEqual(1, dict:size(Dict)),
                ?_assert(dict:is_key(Key, Dict)),
                ?_assertEqual(1, gb_trees:size(Tree)),
                ?_assert(gb_trees:is_defined(dict:fetch(Key, Dict), Tree))
            ]
        end
    }.

insert_same_test_() ->
    {setup,
        fun() -> couch_lru:new() end,
        fun(Lru) ->
            Key = <<"test">>,
            {Tree0, Dict0} = couch_lru:insert(Key, Lru),
            TS0 = dict:fetch(Key, Dict0),
            {Tree, Dict} = couch_lru:insert(Key, {Tree0, Dict0}),
            TS = dict:fetch(Key, Dict),
            [
                ?_assertEqual(1, dict:size(Dict)),
                ?_assert(dict:is_key(Key, Dict)),
                ?_assertEqual(2, gb_trees:size(Tree)),
                ?_assert(gb_trees:is_defined(TS0, Tree)),
                ?_assert(gb_trees:is_defined(TS, Tree))
            ]
        end
    }.

update_test_() ->
    {setup,
        fun() -> couch_lru:new() end,
        fun(Lru) ->
            Key = <<"test">>,
            {Tree0, Dict0} = couch_lru:insert(Key, Lru),
            TS0 = dict:fetch(Key, Dict0),
            {Tree, Dict} = couch_lru:update(Key, {Tree0, Dict0}),
            TS = dict:fetch(Key, Dict),
            [
                ?_assertEqual(1, dict:size(Dict)),
                ?_assert(dict:is_key(Key, Dict)),
                ?_assertEqual(1, gb_trees:size(Tree)),
                ?_assert(gb_trees:is_defined(TS, Tree)),
                ?_assert(TS > TS0)
            ]
        end
    }.

update_missing_test_() ->
    {setup,
        fun() -> couch_lru:new() end,
        fun(Lru) ->
            Key = <<"test">>,
            {Tree0, Dict0} = couch_lru:insert(Key, Lru),
            TS0 = dict:fetch(Key, Dict0),
            {Tree, Dict} = couch_lru:update(<<"missing">>, {Tree0, Dict0}),
            [
                ?_assertEqual(1, dict:size(Dict)),
                ?_assert(dict:is_key(Key, Dict)),
                ?_assertEqual(1, gb_trees:size(Tree)),
                ?_assert(gb_trees:is_defined(TS0, Tree)),
                ?_assertEqual(Tree0, Tree),
                ?_assertEqual(Dict0, Dict)
            ]
        end
    }.

close_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        fun(Lru) ->
            Key = <<"test2">>,
            ok = meck:expect(couch_db, is_idle, 1, true),
            {ok, Lru1} = add_record(Lru, <<"test1">>, c:pid(0, 1001, 0)),
            {ok, Lru2} = add_record(Lru1, <<"test2">>, c:pid(0, 2001, 0)),
            {Tree, Dict} = couch_lru:close(Lru2),
            [
                ?_assertEqual(1, dict:size(Dict)),
                ?_assert(dict:is_key(Key, Dict)),
                ?_assertEqual(1, gb_trees:size(Tree)),
                ?_assert(gb_trees:is_defined(dict:fetch(Key, Dict), Tree))
            ]
        end
    }.

close_all_active_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        fun(Lru) ->
            ok = meck:expect(couch_db, is_idle, 1, false),
            {ok, Lru1} = add_record(Lru, <<"test1">>, c:pid(0, 1001, 0)),
            {ok, Lru2} = add_record(Lru1, <<"test2">>, c:pid(0, 2001, 0)),
            ?_assertError(all_dbs_active, couch_lru:close(Lru2))
        end
    }.


add_record(Lru, Key, Pid) ->
    true = ets:insert(couch_dbs, #db{name = Key, main_pid = Pid}),
    true = ets:insert(couch_dbs_pid_to_name, {Pid, Key}),
    {ok, couch_lru:insert(Key, Lru)}.
