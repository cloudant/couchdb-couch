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

-module(couch_http_handlers_plugin).

-export([
    url_handler/1,
    db_handler/1,
    design_handler/1
]).

-define(SERVICE_ID, http_backdoor_handlers).


-include_lib("couch/include/couch_db.hrl").

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

url_handler(HandlerKey) ->
    Default = fun couch_httpd_db:handle_request/1,
    couch_httpd_handlers:select(?SERVICE_ID, url_handler, HandlerKey, Default).

db_handler(HandlerKey) ->
    Default = fun couch_httpd_db:db_req/2,
    couch_httpd_handlers:select(?SERVICE_ID, db_handler, HandlerKey, Default).


design_handler(HandlerKey) ->
    Default = fun couch_httpd_db:bad_action_req/3,
    couch_httpd_handlers:select(?SERVICE_ID, design_handler, HandlerKey, Default).
