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

-module(couch_db_purge_docs_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


setup() ->
    DbName = ?tempdb(),
    {ok, _Db} = create_db(DbName),
    DbName.

teardown(DbName) ->
    delete_db(DbName),
    ok.

couch_db_purge_docs_test_() ->
    {
        "Couch_db purge_docs",
        {
            setup,
            fun test_util:start_couch/0, fun test_util:stop_couch/1,
            [couch_db_purge_docs()]
        }
    }.


couch_db_purge_docs() ->
    {
       foreach,
            fun setup/0, fun teardown/1,
            [
                fun purge_simple/1,
                fun add_delete_purge/1,
                fun add_two_purge_one/1,
                fun purge_id_not_exist/1,
                fun purge_non_leaf_rev/1,
                fun purge_conflicts/1
            ]
    }.



purge_simple(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            {ok, Rev} = save_doc(Db, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 1}]}),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(1, couch_db_engine:get(Db2, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db2, del_doc_count)),
            ?assertEqual(1, couch_db_engine:get(Db2, update_seq)),
            ?assertEqual(0, couch_db_engine:get(Db2, purge_seq)),
            ?assertEqual(nil, couch_db_engine:get(Db2, purge_tree_state)),

            IdsRevs = [{<<"foo">>, [Rev]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db2, IdsRevs),
            ?assertEqual([Rev], PRevs),
            ?assertEqual(1, PurgeSeq),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, PIdsRevs} = couch_db:fold_purged_docs(Db3, 0, fun fold_fun/3, [], []),
            ?assertEqual(0, couch_db_engine:get(Db3, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db3, del_doc_count)),
            ?assertEqual(2, couch_db_engine:get(Db3, update_seq)),
            ?assertEqual(1, couch_db_engine:get(Db3, purge_seq)),
            ?assertEqual(IdsRevs, PIdsRevs)
        end).


add_delete_purge(DbName) ->
    ?_test(
        begin
            {ok, Db0} = couch_db:open_int(DbName, []),
            {ok, Rev} = save_doc(Db0, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 1}]}),
            couch_db:ensure_full_commit(Db0),
            {ok, Db1} = couch_db:reopen(Db0),

            {ok, Rev2} = save_doc(Db1, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 2},
                {<<"_rev">>, couch_doc:rev_to_str(Rev)}, {<<"_deleted">>, true}]}),
            couch_db:ensure_full_commit(Db1),

            {ok, Db2} = couch_db:reopen(Db1),
            {ok, PIdsRevs1} = couch_db:fold_purged_docs(Db2, 0, fun fold_fun/3, [], []),
            ?assertEqual(0, couch_db_engine:get(Db2, doc_count)),
            ?assertEqual(1, couch_db_engine:get(Db2, del_doc_count)),
            ?assertEqual(2, couch_db_engine:get(Db2, update_seq)),
            ?assertEqual(0, couch_db_engine:get(Db2, purge_seq)),
            ?assertEqual([], PIdsRevs1),

            IdsRevs = [{<<"foo">>, [Rev2]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db2, IdsRevs),
            ?assertEqual([Rev2], PRevs),
            ?assertEqual(1, PurgeSeq),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, PIdsRevs2} = couch_db:fold_purged_docs(Db3, 0, fun fold_fun/3, [], []),
            ?assertEqual(0, couch_db_engine:get(Db3, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db3, del_doc_count)),
            ?assertEqual(3, couch_db_engine:get(Db3, update_seq)),
            ?assertEqual(1, couch_db_engine:get(Db3, purge_seq)),
            ?assertEqual(IdsRevs, PIdsRevs2)
        end).


add_two_purge_one(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            {ok, Rev} = save_doc(Db, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 1}]}),
            {ok, _Rev2} = save_doc(Db, {[{<<"_id">>, <<"bar">>}]}),
            couch_db:ensure_full_commit(Db),

            {ok, Db2} = couch_db:reopen(Db),
            ?assertEqual(2, couch_db_engine:get(Db2, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db2, del_doc_count)),
            ?assertEqual(2, couch_db_engine:get(Db2, update_seq)),
            ?assertEqual(0, couch_db_engine:get(Db2, purge_seq)),

            IdsRevs = [{<<"foo">>, [Rev]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db2, IdsRevs),
            ?assertEqual([Rev], PRevs),
            ?assertEqual(1, PurgeSeq),

            {ok, Db3} = couch_db:reopen(Db2),
            {ok, PIdsRevs} = couch_db:fold_purged_docs(Db3, 0, fun fold_fun/3, [], []),
            ?assertEqual(1, couch_db_engine:get(Db3, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db3, del_doc_count)),
            ?assertEqual(3, couch_db_engine:get(Db3, update_seq)),
            ?assertEqual(1, couch_db_engine:get(Db3, purge_seq)),
            ?assertEqual(IdsRevs, PIdsRevs)
        end).


purge_id_not_exist(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            IdsRevs = [{<<"foo">>, [{0, <<0>>}]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db, IdsRevs),

            ?assertEqual([], PRevs),
            ?assertEqual(0, PurgeSeq),

            {ok, Db2} = couch_db:reopen(Db),
            {ok, PIdsRevs} = couch_db:fold_purged_docs(Db2, 0, fun fold_fun/3, [], []),
            ?assertEqual(0, couch_db_engine:get(Db2, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db2, del_doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db2, update_seq)),
            ?assertEqual(0, couch_db_engine:get(Db2, purge_seq)),
            ?assertEqual([], PIdsRevs)
        end).


purge_non_leaf_rev(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            {ok, Rev} = save_doc(Db, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 1}]}),
            couch_db:ensure_full_commit(Db),
            {ok, Db2} = couch_db:reopen(Db),

            {ok, _Rev2} = save_doc(Db2, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, 2},
                {<<"_rev">>, couch_doc:rev_to_str(Rev)}]}),
            couch_db:ensure_full_commit(Db2),
            {ok, Db3} = couch_db:reopen(Db2),

            IdsRevs = [{<<"foo">>, [Rev]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db3, IdsRevs),
            ?assertEqual([], PRevs),
            ?assertEqual(0, PurgeSeq),

            {ok, Db4} = couch_db:reopen(Db3),
            {ok, PIdsRevs} = couch_db:fold_purged_docs(Db4, 0, fun fold_fun/3, [], []),
            ?assertEqual(1, couch_db_engine:get(Db4, doc_count)),
            ?assertEqual(2, couch_db_engine:get(Db4, update_seq)),
            ?assertEqual(0, couch_db_engine:get(Db4, purge_seq)),
            ?assertEqual([], PIdsRevs)
        end).


purge_conflicts(DbName) ->
    ?_test(
        begin
            {ok, Db} = couch_db:open_int(DbName, []),
            {ok, Rev} = save_doc(Db, {[{<<"_id">>, <<"foo">>}, {<<"vsn">>, <<"v1.1">>}]}),
            couch_db:ensure_full_commit(Db),
            {ok, Db2} = couch_db:reopen(Db),

            % create a conflict
            DocConflict = #doc{
                id = <<"foo">>,
                revs = {1, [couch_crypto:hash(md5, <<"v1.2">>)]},
                body = {[ {<<"vsn">>,  <<"v1.2">>}]}
            },
            {ok, _} = couch_db:update_doc(Db2, DocConflict, [], replicated_changes),
            couch_db:ensure_full_commit(Db2),
            {ok, Db3} = couch_db:reopen(Db2),

            IdsRevs = [{<<"foo">>, [Rev]}],
            {ok, {PurgeSeq, [{ok, PRevs}]}} = couch_db:purge_docs(Db3, IdsRevs),
            ?assertEqual([Rev], PRevs),
            ?assertEqual(1, PurgeSeq),

            {ok, Db4} = couch_db:reopen(Db3),
            {ok, PIdsRevs} = couch_db:fold_purged_docs(Db4, 0, fun fold_fun/3, [], []),
            % still has one doc
            ?assertEqual(1, couch_db_engine:get(Db4, doc_count)),
            ?assertEqual(0, couch_db_engine:get(Db4, del_doc_count)),
            ?assertEqual(3, couch_db_engine:get(Db4, update_seq)),
            ?assertEqual(1, couch_db_engine:get(Db4, purge_seq)),
            ?assertEqual(IdsRevs, PIdsRevs)
        end).


create_db(DbName) ->
    couch_db:create(DbName, [?ADMIN_CTX, overwrite]).

delete_db(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]).

save_doc(Db, Json) ->
    Doc = couch_doc:from_json_obj(Json),
    couch_db:update_doc(Db, Doc, []).

fold_fun(_PurgeSeq, {Id, Revs}, Acc) ->
    {ok, [{Id, Revs} | Acc]}.



