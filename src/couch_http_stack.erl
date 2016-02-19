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
    authenticate/1,
    authorize/1
]).

-export([
    default_url_handler/1,
    default_db_handler/1,
    default_design_handler/1
]).

start_link(http) ->
    start_link(new(backdoor_http, http));
start_link(https) ->
    start_link(new(backdoor_https, https));
start_link(#couch_http_stack{} = Stack) ->
    % ensure uuid is set so that concurrent replications
    % get the same value.
    couch_server:get_uuid(),

    set_auth_handlers(),
    couch_httpd:start_link(Stack).

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

authenticate(Req) ->
    {ok, AuthenticationFuns} = application:get_env(couch, auth_handlers),
    couch_httpd_handler:authenticate_request(Req, couch_auth_cache, AuthenticationFuns).

authorize(Req) -> Req.

default_url_handler(#couch_http_stack{}) -> fun couch_httpd_db:handle_request/1.
default_db_handler(#couch_http_stack{}) -> fun couch_httpd_db:db_req/2.
default_design_handler(#couch_http_stack{}) -> fun couch_httpd_db:bad_action_req/3.


bind_address() ->
    case config:get("httpd", "bind_address", "any") of
        "any" -> any;
        Else -> Else
    end.


set_auth_handlers() ->
    AuthenticationSrcs = make_fun_spec_strs(
        config:get("httpd", "authentication_handlers", "")),
    AuthHandlers = lists:map(
        fun(A) -> {auth_handler_name(A), make_arity_1_fun(A)} end, AuthenticationSrcs),
    AuthenticationFuns = AuthHandlers ++ [
        {<<"local">>, fun couch_httpd_auth:party_mode_handler/1} %% should be last
    ],
    ok = application:set_env(couch, auth_handlers, AuthenticationFuns).

auth_handler_name(SpecStr) ->
    lists:nth(?HANDLER_NAME_IN_MODULE_POS, re:split(SpecStr, "[\\W_]", [])).

% SpecStr is "{my_module, my_fun}, {my_module2, my_fun2}"
make_fun_spec_strs(SpecStr) ->
    re:split(SpecStr, "(?<=})\\s*,\\s*(?={)", [{return, list}]).

% SpecStr is a string like "{my_module, my_fun}"
%  or "{my_module, my_fun, <<"my_arg">>}"
make_arity_1_fun(SpecStr) ->
    case couch_util:parse_term(SpecStr) of
    {ok, {Mod, Fun, SpecArg}} ->
        fun(Arg) -> Mod:Fun(Arg, SpecArg) end;
    {ok, {Mod, Fun}} ->
        fun(Arg) -> Mod:Fun(Arg) end
    end.
