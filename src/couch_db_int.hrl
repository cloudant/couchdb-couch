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


-record(db, {
    name,
    filepath,

    engine = {couch_bt_engine, undefined},

    main_pid = nil,
    compactor_pid = nil,

    committed_update_seq,

    instance_start_time, % number of microsecs since jan 1 1970 as a binary string

    user_ctx = #user_ctx{},
    security = [],
    validate_doc_funs = undefined,

    before_doc_update = nil, % nil | fun(Doc, Db) -> NewDoc
    after_doc_read = nil,    % nil | fun(Doc, Db) -> NewDoc

    waiting_delayed_commit = nil,

    options = [],
    compression
}).

