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

-module(couch_refcnt_tests).

-include_lib("eunit/include/eunit.hrl").


-define(TIMEOUT, 1000).


basic_test() ->
    {ok, RefCnt} = couch_refcnt:create(),
    {ok, 1} = couch_refcnt:get_count(RefCnt),
    ok = couch_refcnt:close_if(RefCnt, 1).


simple_incref_test() ->
    Total = 50,
    {ok, RefCnt} = couch_refcnt:create(),
    (fun() ->
        lists:map(fun(I) ->
            {ok, New} = couch_refcnt:incref(RefCnt),
            ?assertEqual({ok, I + 1}, couch_refcnt:get_count(New)),
            ?assertEqual({ok, I + 1}, couch_refcnt:get_count(RefCnt)),
            New
        end, lists:seq(1, Total))
    end)(),
    erlang:garbage_collect(),
    {ok, 1} = couch_refcnt:get_count(RefCnt).


chained_incref_test() ->
    Total = 50,
    {ok, RefCnt} = couch_refcnt:create(),
    (fun() ->
        lists:foldl(fun(I, [Prev | Rest]) ->
            {ok, New} = couch_refcnt:incref(Prev),
            ?assertEqual({ok, I + 1}, couch_refcnt:get_count(New)),
            lists:foreach(fun(R) ->
                ?assertEqual({ok, I + 1}, couch_refcnt:get_count(R))
            end, [Prev | Rest]),
            [New, Prev | Rest]
        end, [RefCnt], lists:seq(1, Total))
    end)(),
    erlang:garbage_collect(),
    {ok, 1} = couch_refcnt:get_count(RefCnt).


decref_test() ->
    {ok, RefCnt} = couch_refcnt:create(),
    ok = couch_refcnt:decref(RefCnt),
    try couch_refcnt:get_count(RefCnt) of
        _ -> erlang:error(should_throw)
    catch error:badarg ->
        ok
    end.


decref_copy_test() ->
    Self = self(),
    {ok, RefCnt} = couch_refcnt:create(),
    Pid = spawn(fun() ->
        {ok, 1} = couch_refcnt:get_count(RefCnt),
        Self ! inited,
        receive ok -> ok end,
        try couch_refcnt:get_count(RefCnt) of
            N -> Self ! N
        catch error:badarg ->
            Self ! badarg
        end
    end),
    receive
        inited -> ok
    after ?TIMEOUT ->
        erlang:error(timeout)
    end,
    ok = couch_refcnt:decref(RefCnt),
    Pid ! ok,
    Msg = receive
        M -> M
    after ?TIMEOUT ->
        erlang:error(timeout)
    end,
    ?assertEqual(badarg, Msg).


decref_isolation_test() ->
    Self = self(),
    {ok, RefCnt1} = couch_refcnt:create(),
    Pid = spawn(fun() ->
        {ok, RefCnt2} = couch_refcnt:incref(RefCnt1),
        {ok, 2} = couch_refcnt:get_count(RefCnt2),
        Self ! inited,
        receive ok -> ok end,
        try couch_refcnt:get_count(RefCnt2) of
            N -> Self ! N
        catch error:badarg ->
            Self ! badarg
        end
    end),
    receive
        inited -> ok
    after ?TIMEOUT ->
        erlang:error(timeout)
    end,
    ok = couch_refcnt:decref(RefCnt1),
    Pid ! ok,
    Msg = receive
        M -> M
    after ?TIMEOUT ->
        erlang:error(timeout)
    end,
    ?assertEqual({ok, 1}, Msg).


decref_invalidation_test() ->
    {ok, RefCnt} = couch_refcnt:create(),
    ok = couch_refcnt:decref(RefCnt),
    AssertBadarg = fun(Fun) ->
        try Fun(RefCnt) of
            _Any -> erlang:error(decref_failed)
        catch error:badarg ->
            ok
        end
    end,
    Funs = [
        fun couch_refcnt:incref/1,
        fun couch_refcnt:decref/1,
        fun(RC) -> couch_refcnt:close_if(RC, 1) end,
        fun couch_refcnt:get_ref/1,
        fun couch_refcnt:get_count/1
    ],
    lists:foreach(fun(F) ->
        AssertBadarg(F)
    end, Funs).


close_test() ->
    {ok, RefCnt1} = couch_refcnt:create(),
    ?assertEqual(no_match, couch_refcnt:close_if(RefCnt1, 2)),
    {ok, RefCnt2} = couch_refcnt:incref(RefCnt1),
    ?assertEqual(ok, couch_refcnt:close_if(RefCnt1, 2)),
    ?assertEqual(closed, couch_refcnt:close_if(RefCnt1, 1)),
    ?assertEqual(closed, couch_refcnt:close_if(RefCnt2, 2)),
    ?assertEqual(closed, couch_refcnt:incref(RefCnt1)),
    ?assertEqual(closed, couch_refcnt:incref(RefCnt2)).


shared_incref_test() ->
    Total = 50,
    {ok, RefCnt} = couch_refcnt:create(),
    Parent = self(),
    Pids = lists:map(fun(I) ->
        Pid = spawn(fun() ->
            {ok, New} = couch_refcnt:incref(RefCnt),
            ?assertEqual({ok, I + 1}, couch_refcnt:get_count(New)),
            Parent ! initialized,
            receive close -> ok end
        end),
        receive initialized -> ok end,
        ?assertEqual({ok, I + 1}, couch_refcnt:get_count(RefCnt)),
        Pid
    end, lists:seq(1, Total)),
    ?assertEqual({ok, Total + 1}, couch_refcnt:get_count(RefCnt)),
    lists:foldl(fun(Pid, I) ->
        Ref = erlang:monitor(process, Pid),
        Pid ! close,
        receive {'DOWN', Ref, _, _, _} -> ok end,
        ?assertEqual({ok, Total + 1 - I}, couch_refcnt:get_count(RefCnt)),
        I + 1
    end, 1, Pids),
    ?assertEqual({ok, 1}, couch_refcnt:get_count(RefCnt)).


shared_copy_test() ->
    Total = 50,
    {ok, RefCnt1} = couch_refcnt:create(),
    (fun() ->
        {ok, RefCnt2} = couch_refcnt:incref(RefCnt1),
        [FirstPid | _] = Pids = lists:foldl(fun(_, [AccPid | Rest]) ->
            NewPid = spawn(fun() ->
                Next = receive {set_next, Pid} -> Pid end,
                receive
                    {ref, RC} ->
                        ?assertEqual({ok, 2}, couch_refcnt:get_count(RC)),
                        Next ! {ref, RC}
                end
            end),
            NewPid ! {set_next, AccPid},
            [NewPid, AccPid | Rest]
        end, [self()], lists:seq(1, Total)),
        FirstPid ! {ref, RefCnt2},
        receive
            {ref, RefCnt2} ->
                ok
        after ?TIMEOUT ->
            erlang:exit(timeout)
        end,
        ?assertEqual({ok, 2}, couch_refcnt:get_count(RefCnt2)),
        lists:foreach(fun(Pid) ->
            if Pid == self() -> ok; true ->
                Ref = erlang:monitor(process, Pid),
                receive {'DOWN', Ref, _, _, _} -> ok end
            end
        end, Pids)
    end)(),
    erlang:garbage_collect(),
    ?assertEqual({ok, 1}, couch_refcnt:get_count(RefCnt1)).


signal_test() ->
    Ref = (fun() ->
        {ok, RefCnt} = couch_refcnt:create(self()),
        {ok, Ref} = couch_refcnt:get_ref(RefCnt),
        {ok, _} = couch_refcnt:incref(RefCnt),
        receive
            {refcnt, Ref} ->
                ok
        after ?TIMEOUT ->
            erlang:error(timeout)
        end,
        Ref
    end)(),
    erlang:garbage_collect(),
    receive
        {refcnt, Ref} ->
            ok
    after ?TIMEOUT ->
        erlang:error(timeout)
    end.
