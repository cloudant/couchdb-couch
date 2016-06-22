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

-module(test_engine_get_set_props).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").


cet_default_props() ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St} = Engine:init(DbPath, [
            create,
            {default_security_object, dso}
        ]),

    Node = node(),

    ?assertEqual(0, Engine:get(St, doc_count)),
    ?assertEqual(0, Engine:get(St, del_doc_count)),
    ?assertEqual(true, is_list(Engine:get(St, size_info))),
    ?assertEqual(true, is_integer(Engine:get(St, disk_version))),
    ?assertEqual(0, Engine:get(St, update_seq)),
    ?assertEqual(0, Engine:get(St, purge_seq)),
    ?assertEqual(true, is_integer(Engine:get(St, purged_docs_limit))),
    ?assertEqual(true, Engine:get(St, purged_docs_limit) > 0),
    ?assertEqual(dso, Engine:get(St, security)),
    ?assertEqual(1000, Engine:get(St, revs_limit)),
    ?assertMatch(<<_:32/binary>>, Engine:get(St, uuid)),
    ?assertEqual([{Node, 0}], Engine:get(St, epochs)),
    ?assertEqual(0, Engine:get(St, compacted_seq)).


cet_set_security() ->
    check_prop_set(security, dso, [{<<"readers">>, []}]).


cet_set_revs_limit() ->
    check_prop_set(revs_limit, 1000, 50).


cet_set_epochs() ->
    TestValue = [
        {'other_node@127.0.0.1', 0},
        {node(), 0}
    ],
    % This looks weird. I need to move the epochs
    % logic to a central place so this isn't the case here.
    % The weirdness is because epochs are modified when
    % we use couch_bt_engine_header.
    CommittedValue = [
        {node(), 1},
        {'other_node@127.0.0.1', 0}
    ],
    check_prop_set(epochs, [{node(), 0}], TestValue, CommittedValue).


cet_set_compact_seq() ->
    check_prop_set(compacted_seq, 0, 12).


check_prop_set(Name, Default, Value) ->
    check_prop_set(Name, Default, Value, Value).


check_prop_set(Name, Default, Value, CommittedValue) ->
    Engine = test_engine_util:get_engine(),
    DbPath = test_engine_util:dbpath(),

    {ok, St0} = Engine:init(DbPath, [
            create,
            {default_security_object, dso}
        ]),
    ?assertEqual(Default, Engine:get(St0, Name)),

    {ok, St1} = Engine:set(St0, Name, Value),
    ?assertEqual(Value, Engine:get(St1, Name)),

    {ok, St2} = Engine:commit_data(St1),
    Engine:terminate(normal, St2),

    {ok, St3} = Engine:init(DbPath, []),
    ?assertEqual(CommittedValue, Engine:get(St3, Name)).
