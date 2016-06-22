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

-module(test_engine_purge_docs).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


cet_purge_simple() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, PIdRevs2} = Engine:fold_purged_docs(St2, 0, fun fold_fun/3, [], []),

    ?assertEqual(1, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(1, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], PIdRevs2),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),
    {ok, PIdRevs3} = Engine:fold_purged_docs(St3, 0, fun fold_fun/3, [], []),

    ?assertEqual(0, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(2, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], PIdRevs3).


cet_purge_conflicts() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {conflict, {<<"foo">>, [{<<"vsn">>, 2}]}}
    ],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, PIdRevs2} = Engine:fold_purged_docs(St2, 0, fun fold_fun/3, [], []),

    ?assertEqual(1, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], PIdRevs2),

    [FDI1] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev1 = test_engine_util:prev_rev(FDI1),
    Rev1 = PrevRev1#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev1}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),
    {ok, PIdRevs3} = Engine:fold_purged_docs(St3, 0, fun fold_fun/3, [], []),

    ?assertEqual(1, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(3, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev1]}], PIdRevs3),

    [FDI2] = Engine:open_docs(St3, [<<"foo">>]),
    PrevRev2 = test_engine_util:prev_rev(FDI2),
    Rev2 = PrevRev2#rev_info.rev,

    Actions3 = [
        {purge, {<<"foo">>, Rev2}}
    ],
    {ok, St4} = test_engine_util:apply_actions(Engine, St3, Actions3),
    {ok, PIdRevs4} = Engine:fold_purged_docs(St4, 0, fun fold_fun/3, [], []),

    ?assertEqual(0, Engine:get(St4, doc_count)),
    ?assertEqual(0, Engine:get(St4, del_doc_count)),
    ?assertEqual(4, Engine:get(St4, update_seq)),
    ?assertEqual(2, Engine:get(St4, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev1]}, {<<"foo">>, [Rev2]}],
            lists:reverse(PIdRevs4)).


cet_add_delete_purge() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {delete, {<<"foo">>, [{<<"vsn">>, 2}]}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, PIdRevs2} = Engine:fold_purged_docs(St2, 0, fun fold_fun/3, [], []),


    ?assertEqual(0, Engine:get(St2, doc_count)),
    ?assertEqual(1, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], PIdRevs2),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),
    {ok, PIdRevs3} = Engine:fold_purged_docs(St3, 0, fun fold_fun/3, [], []),


    ?assertEqual(0, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(3, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], PIdRevs3).


cet_add_two_purge_one() ->
    {ok, Engine, St1} = test_engine_util:init_engine(),

    Actions1 = [
        {create, {<<"foo">>, [{<<"vsn">>, 1}]}},
        {create, {<<"bar">>, []}}
    ],

    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions1),
    {ok, PIdRevs2} = Engine:fold_purged_docs(St2, 0, fun fold_fun/3, [], []),

    ?assertEqual(2, Engine:get(St2, doc_count)),
    ?assertEqual(0, Engine:get(St2, del_doc_count)),
    ?assertEqual(2, Engine:get(St2, update_seq)),
    ?assertEqual(0, Engine:get(St2, purge_seq)),
    ?assertEqual([], PIdRevs2),

    [FDI] = Engine:open_docs(St2, [<<"foo">>]),
    PrevRev = test_engine_util:prev_rev(FDI),
    Rev = PrevRev#rev_info.rev,

    Actions2 = [
        {purge, {<<"foo">>, Rev}}
    ],
    {ok, St3} = test_engine_util:apply_actions(Engine, St2, Actions2),
    {ok, PIdRevs3} = Engine:fold_purged_docs(St3, 0, fun fold_fun/3, [], []),

    ?assertEqual(1, Engine:get(St3, doc_count)),
    ?assertEqual(0, Engine:get(St3, del_doc_count)),
    ?assertEqual(3, Engine:get(St3, update_seq)),
    ?assertEqual(1, Engine:get(St3, purge_seq)),
    ?assertEqual([{<<"foo">>, [Rev]}], PIdRevs3).



fold_fun(_PurgeSeq, {Id, Revs}, Acc) ->
    {ok, [{Id, Revs} | Acc]}.
