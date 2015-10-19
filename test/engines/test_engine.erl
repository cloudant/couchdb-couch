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

-module(test_engine).

-export([start/0]).


start() ->
    monitor_parent(),
    {EngineApp, EngineMod} = start_apps(),
    io:format(standard_error, "~nTesting: ~s/~s~n~n", [EngineApp, EngineMod]),
    try
        run(EngineApp, EngineMod)
    after
        init:stop()
    end.


run(EngineApp, EngineMod) ->
    application:set_env(couch, test_engine, {EngineApp, EngineMod}),
    {ok, [[TestDir]]} = init:get_argument(engine_tests),
    {ok, CoverSt} = test_engine_cover:init(EngineApp, TestDir),
    Glob = filename:join(TestDir, "*.beam"),
    Files = filelib:wildcard(Glob),
    Tests = lists:map(fun(FileName) ->
        BaseName = filename:basename(FileName, ".beam"),
        list_to_atom(BaseName)
    end, lists:sort(Files)),
    case eunit:test(Tests, [verbose]) of
        ok ->
            test_engine_cover:finish(CoverSt);
        error ->
            init:stop(2)
    end.


monitor_parent() ->
    {ok, [[PPid]]} = init:get_argument(parent_pid),
    spawn(fun() -> monitor_parent(PPid) end).


monitor_parent(PPid) ->
    timer:sleep(1000),
    case os:cmd("kill -0 " ++ PPid) of
        "" ->
            monitor_parent(PPid);
        _Else ->
            % Assume _Else is a no such process error
            init:stop()
    end.


start_apps() ->
    {ok, [[EngineAppStr]]} = init:get_argument(engine_app),
    {ok, [[EngineModStr]]} = init:get_argument(engine_mod),
    EngineApp = list_to_atom(EngineAppStr),
    EngineMod = list_to_atom(EngineModStr),
    application:ensure_all_started(couch),
    if EngineApp == couch -> ok; true ->
        application:ensure_all_started(EngineApp)
    end,
    {EngineApp, EngineMod}.
