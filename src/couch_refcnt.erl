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

-module(couch_refcnt).
-on_load(init/0).


-export([
    create/0,
    create/1,
    incref/1,
    decref/1,
    close_if/2,
    get_ref/1,
    get_count/1
]).


-define(NOT_LOADED, not_loaded(?LINE)).


-type refcnt() :: term().

-spec create() -> {ok, refcnt()}.
create() ->
    ?NOT_LOADED.


-spec create(pid()) -> {ok, refcnt()}.
create(_Dst) ->
    ?NOT_LOADED.


-spec incref(refcnt()) -> {ok, refcnt()} | closed.
incref(_RefCnt) ->
    ?NOT_LOADED.


-spec decref(refcnt()) -> ok.
decref(_RefCnt) ->
    ?NOT_LOADED.


-spec close_if(refcnt(), pos_integer()) -> ok | closed | no_match.
close_if(_RefCnt, _Expect) ->
    ?NOT_LOADED.


-spec get_ref(refcnt()) -> {ok, reference()}.
get_ref(_RefCnt) ->
    ?NOT_LOADED.


-spec get_count(refcnt()) -> {ok, pos_integer()}.
get_count(_RefCnt) ->
    ?NOT_LOADED.


init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end,
    erlang:load_nif(filename:join(PrivDir, "couch_refcnt"), 0).


not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
