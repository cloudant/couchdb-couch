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

-module(couch_http_stack).
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_httpd/include/couch_httpd.hrl").

-define(HANDLER_NAME_IN_MODULE_POS, 6).

-record(couch_http_stack, {name, protocol, port, bind_address}).

-export([
    start_link/1,
    stop/0,
    stop/1,
    new/2
]).

-export([
    name/1,
    protocol/1,
    port/1,
    bind_address/1,
    server_options/1,
    socket_options/1
]).

-export([
    authenticate/2,
    authorize/2
]).

-export([
    url_handler/2,
    db_handler/2,
    design_handler/2
]).

start_link(http) ->
    start_link(new(backdoor_http, http));
start_link(https) ->
    start_link(new(backdoor_https, https));
start_link(#couch_http_stack{} = Stack) ->
    % ensure uuid is set so that concurrent replications
    % get the same value.
    couch_server:get_uuid(),
    set_handlers(),
    set_auth_handlers(),
    couch_httpd_config_listener:subscribe(Stack, [
        {"httpd", "bind_address"},
        {"httpd", "port"},
        {"httpd", "backlog"},
        {"httpd", "server_options"}
    ]),
    couch_httpd:start_link(Stack).

stop() ->
    ok = couch_httpd_handler:stop(backdoor_http),
    ok = couch_httpd_handler:stop(backdoor_https),
    ok.

stop(#couch_http_stack{name = Name}) ->
    couch_httpd_handler:stop(Name).

new(Name, Protocol) ->
    #couch_http_stack{
        name = Name,
        protocol = Protocol,
        port = config:get("httpd", "port", "5984"),
        bind_address = bind_address()
    }.

name(#couch_http_stack{name = Name}) -> Name.

protocol(#couch_http_stack{protocol = Protocol}) -> Protocol.

port(#couch_http_stack{port = Port}) -> Port.

bind_address(#couch_http_stack{bind_address = Address}) -> Address.


server_options(#couch_http_stack{}) ->
    ServerOptsCfg = config:get("httpd", "server_options", "[]"),
    {ok, ServerOpts} = couch_util:parse_term(ServerOptsCfg),
    ServerOpts.

socket_options(#couch_http_stack{}) ->
    case config:get("httpd", "socket_options") of
        undefined ->
            undefined;
        SocketOptsCfg ->
            {ok, SocketOpts} = couch_util:parse_term(SocketOptsCfg),
            SocketOpts
    end.

authenticate(Req, _Stack) ->
    {ok, AuthenticationFuns} = application:get_env(couch, auth_handlers),
    couch_httpd_handler:authenticate_request(Req, couch_auth_cache, AuthenticationFuns).

authorize(Req, _Stack) -> Req.


url_handler(HandlerKey, #couch_http_stack{}) ->
    couch_http_handlers_plugin:url_handler(HandlerKey).

db_handler(HandlerKey, #couch_http_stack{}) ->
    couch_http_handlers_plugin:db_handler(HandlerKey).

design_handler(HandlerKey, #couch_http_stack{}) ->
    couch_http_handlers_plugin:design_handler(HandlerKey).

bind_address() ->
    case config:get("httpd", "bind_address", "any") of
        "any" -> any;
        Else -> Else
    end.


set_handlers() ->
    UrlHandlersList = lists:map(
        fun({UrlKey, SpecStr}) ->
            {?l2b(UrlKey), couch_httpd_util:fun_from_spec(SpecStr, 1)}
        end, config:get("httpd_global_handlers")),

    DbUrlHandlersList = lists:map(
        fun({UrlKey, SpecStr}) ->
            {?l2b(UrlKey), couch_httpd_util:fun_from_spec(SpecStr, 2)}
        end, config:get("httpd_db_handlers")),

    DesignUrlHandlersList = lists:map(
        fun({UrlKey, SpecStr}) ->
            {?l2b(UrlKey), couch_httpd_util:fun_from_spec(SpecStr, 3)}
        end, config:get("httpd_design_handlers")),

    ok = application:set_env(couch, url_handlers, dict:from_list(UrlHandlersList)),
    ok = application:set_env(couch, db_handlers, dict:from_list(DbUrlHandlersList)),
    ok = application:set_env(couch, design_handlers, dict:from_list(DesignUrlHandlersList)),
    ok.

set_auth_handlers() ->
    AuthenticationSrcs = make_fun_spec_strs(
        config:get("httpd", "authentication_handlers", "")),
    AuthHandlers = lists:map(
        fun(A) ->
            {auth_handler_name(A), couch_httpd_util:fun_from_spec(A, 1)}
        end, AuthenticationSrcs),
    AuthenticationFuns = AuthHandlers ++ [
        {<<"local">>, fun couch_httpd_auth:party_mode_handler/1} %% should be last
    ],
    ok = application:set_env(couch, auth_handlers, AuthenticationFuns).

auth_handler_name(SpecStr) ->
    lists:nth(?HANDLER_NAME_IN_MODULE_POS, re:split(SpecStr, "[\\W_]", [])).

% SpecStr is "{my_module, my_fun}, {my_module2, my_fun2}"
make_fun_spec_strs(SpecStr) ->
    re:split(SpecStr, "(?<=})\\s*,\\s*(?={)", [{return, list}]).
