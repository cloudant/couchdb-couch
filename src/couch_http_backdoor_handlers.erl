% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_http_backdoor_handlers).

-export([url_handler/1, db_handler/1, design_handler/1]).

url_handler(<<>>) ->
    url_handler(<<"/">>);
url_handler(HandlerKey) ->
    {ok, Handlers} = application:get_env(couch, url_handlers),
    couch_util:dict_find(HandlerKey, Handlers, no_match).


db_handler(HandlerKey) ->
    {ok, Handlers} = application:get_env(couch, db_handlers),
    couch_util:dict_find(HandlerKey, Handlers, no_match).

design_handler(HandlerKey) ->
    {ok, Handlers} = application:get_env(couch, design_handlers),
    couch_util:dict_find(HandlerKey, Handlers, no_match).
