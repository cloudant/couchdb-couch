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

-module(couch_db_mpr_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(USER, "couch_db_admin").
-define(PASS, "pass").
-define(AUTH, {basic_auth, {?USER, ?PASS}}).
-define(CONTENT_JSON, {"Content-Type", "application/json"}).
-define(CONTENT_MULTI_RELATED, {"Content-Type",
    "multipart/related;boundary=\"bound\""}).


setup() ->
    ok = config:set("admins", ?USER, ?PASS, _Persist=false),
    TmpDb = ?tempdb(),
    Addr = config:get("httpd", "bind_address", "127.0.0.1"),
    Port = mochiweb_socket_server:get(couch_httpd, port),
    Url = lists:concat(["http://", Addr, ":", Port, "/", ?b2l(TmpDb)]),
    create_db(Url),
    Url.

teardown(Url) ->
    delete_db(Url),
    ok = config:delete("admins", ?USER, _Persist=false).

create_db(Url) ->
    {ok, Status, _, _} = test_request:put(Url, [?CONTENT_JSON, ?AUTH], "{}"),
    ?assert(Status =:= 201 orelse Status =:= 202).

delete_db(Url) ->
    {ok, 200, _, _} = test_request:delete(Url, [?AUTH]).

create_doc(Url, Id, Body, Type) ->
    test_request:put(Url ++ "/" ++ Id, [Type, ?AUTH], Body).

delete_doc(Url, Id, Rev) ->
    test_request:delete(Url ++ "/" ++ Id ++ "?rev=" ++ ?b2l(Rev)).

couch_db_mpr_test_() ->
    {
        "multi-part attachment tests",
        {
            setup,
            fun test_util:start_couch/0, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun delete_then_create_mpr/1
                ]
            }
        }
    }.

delete_then_create_mpr(Url) ->
    Id1 = "testdoc1",
    Id2 = "testdoc2",

    {ok, _, _, Resp1} = create_doc(Url, Id1, "{\"foo\": \"bar\"}",
        ?CONTENT_JSON),
    {Json1} = ?JSON_DECODE(Resp1),

    Rev1 = couch_util:get_value(<<"rev">>, Json1, undefined),
    {ok, _, _, _} = delete_doc(Url, Id1, Rev1),

    Body = lists:concat(["{\"body\":\"This is that tests deletes and ",
        "recreates with attachments.\","]),
    DocBeg = "--bound\r\nContent-Type: application/json\r\n\r\n",
    DocRest =  lists:concat(["\"_attachments\":{\"foo.txt\":{\"follows\":true,",
        "\"content_type\":\"text/plain\",\"length\":21},\"bar.txt\":",
        "{\"follows\":true,\"content_type\":\"text/plain\",",
        "\"length\":20}}}\r\n--bound\r\n\r\nthis is 21 chars long",
        "\r\n--bound\r\n\r\nthis is 20 chars lon\r\n--bound--epilogue"]),
    AttachDoc = lists:concat([DocBeg, Body, DocRest]),

    {ok, _, _, Resp2} = create_doc(Url, Id1, AttachDoc,
        ?CONTENT_MULTI_RELATED),
    {Json2} = ?JSON_DECODE(Resp2),
    AttachRev = couch_util:get_value(<<"rev">>, Json2, undefined),

    delete_db(Url),
    create_db(Url),

    {ok, _, _, Resp3} = create_doc(Url, Id2, "{\"foo\": \"bar\"}",
        ?CONTENT_JSON),
    {Json3} = ?JSON_DECODE(Resp3),
    Rev3 = couch_util:get_value(<<"rev">>, Json3, undefined),
    {ok, _, _, _} = delete_doc(Url, Id2, Rev3),

    {ok, _, _, Resp4} = create_doc(Url, Id2, AttachDoc,
        ?CONTENT_MULTI_RELATED),
    {Json4} = ?JSON_DECODE(Resp4),
    AttachRev2 = couch_util:get_value(<<"rev">>, Json4, undefined),

    ?_assertEqual(AttachRev, AttachRev2).
