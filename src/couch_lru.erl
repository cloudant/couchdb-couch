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

-module(couch_lru).


-export([
    new/0,
    insert/2,
    update/2,
    close/1
]).

% Debugging
-export([
    to_list/1,
    validate/1,
    debug/1
]).

-include_lib("couch/include/couch_db.hrl").


-record(lru, {
    dbs,
    nodes,
    head,
    tail
}).


-record(node, {
    dbname,
    prev,
    next
}).


new() ->
    {ok, Dbs} = khash:new(),
    {ok, Nodes} = khash:new(),
    #lru{
        dbs = Dbs,
        nodes = Nodes
    }.


insert(DbName, Lru) ->
    #lru{
        dbs = Dbs,
        nodes = Nodes,
        head = Head,
        tail = Tail
    } = Lru,
    Node = #node{
        dbname = DbName
    },
    case khash:lookup(Dbs, DbName) of
        {value, Ref} when is_reference(Ref) ->
            update(DbName, Lru);
        not_found ->
            Ref = erlang:make_ref(),
            ok = khash:put(Dbs, DbName, Ref),
            ok = khash:put(Nodes, Ref, Node),
            {NewHead, NewTail} = case {Head, Tail} of
                {undefined, undefined} ->
                    {Ref, Ref};
                {_, _} when is_reference(Head), is_reference(Tail) ->
                    set_next(Lru, Tail, Ref),
                    set_prev(Lru, Ref, Tail),
                    {Head, Ref}
            end,
            Lru#lru{
                head = NewHead,
                tail = NewTail
            }
    end.


update(DbName, Lru) ->
    #lru{
        dbs = Dbs,
        head = Head,
        tail = Tail
    } = Lru,
    case khash:get(Dbs, DbName) of
        Tail ->
            % This was already the LRU entry, ignore
            Lru;
        Ref when is_reference(Ref) ->
            % Get neighbors of EntryPid
            {Prev, Next} = get_links(Lru, Ref),

            % Remove Ref from link order
            set_next(Lru, Prev, Next),
            set_prev(Lru, Next, Prev),

            % Adjust Head if necessary
            NewHead = if
                Ref == Head -> Next;
                true -> Head
            end,

            % Move Ref to end of link order
            set_next(Lru, Tail, Ref),
            set_links(Lru, Ref, Tail, undefined),

            Lru#lru{
                head = NewHead,
                tail = Ref
            };
        undefined ->
            % We closed this database before processing the update.  Ignore
            Lru
    end.


close(Lru) ->
    #lru{
        head = Head
    } = Lru,
    close(Lru, Head).


to_list(Lru) ->
    #lru{
        head = Head
    } = Lru,
    if Head == undefined -> []; true ->
        to_list(Lru, Head)
    end.


validate(Lru) ->
    #lru{
        dbs = Dbs,
        nodes = Nodes,
        head = Head,
        tail = Tail
    } = Lru,
    try
        Size = khash:size(Dbs),
        Size = khash:size(Nodes),
        case {Head, Tail} of
            {undefined, undefined} ->
                0 = khash:size(Dbs);
            {H, T} when is_reference(H), is_reference(T) ->
                validate(Lru, H)
        end,
        true
    catch Type:Reason ->
        Stack = erlang:get_stacktrace(),
        {false, {Type, Reason, Stack}}
    end.


debug(Lru) ->
    #lru{
        dbs = Dbs,
        nodes = Nodes,
        head = Head,
        tail = Tail
    } = Lru,
    [
        {dbs, khash:to_list(Dbs)},
        {nodes, khash:to_list(Nodes)},
        {head, Head},
        {tail, Tail}
    ].


close(_, undefined) ->
    erlang:error(all_dbs_active);

close(Lru, Ref) ->
    #lru{
        nodes = Nodes
    } = Lru,
    #node{
        dbname = DbName,
        next = Next
    } = khash:get(Nodes, Ref),
    case ets:update_element(couch_dbs, DbName, {#db.fd_monitor, locked}) of
        true ->
            [#db{main_pid = Pid} = Db] = ets:lookup(couch_dbs, DbName),
            case couch_db:is_idle(Db) of
                true ->
                    true = ets:delete(couch_dbs, DbName),
                    true = ets:delete(couch_dbs_pid_to_name, Pid),
                    exit(Pid, kill),
                    remove_ref(Lru, Ref);
                false ->
                    Op = {#db.fd_monitor, nil},
                    true = ets:update_element(couch_dbs, DbName, Op),
                    couch_stats:increment_counter([
                        couchdb,
                        couch_server,
                        lru_skip
                    ]),
                    close(Lru, Next)
            end;
        false ->
            NewLru = close(Lru, Next),
            remove_ref(NewLru, Ref)
    end.


to_list(_Lru, undefined) ->
    [];

to_list(Lru, Ref) ->
    #node{
        dbname = DbName,
        next = Next
    } = khash:get(Lru#lru.nodes, Ref),
    [DbName | to_list(Lru, Next)].


validate(_Lru, undefined) ->
    ok;

validate(Lru, Ref) ->
    #lru{
        dbs = Dbs,
        nodes = Nodes,
        head = Head,
        tail = Tail
    } = Lru,
    #node{
        dbname = DbName,
        prev = Prev,
        next = Next
    } = khash:get(Nodes, Ref),
    Ref = khash:get(Dbs, DbName),
    if
        Prev == undefined ->
            Ref = Head;
        is_reference(Prev) ->
            Ref = (khash:get(Nodes, Prev))#node.next
    end,
    if
        Next == undefined ->
            Ref = Tail;
        is_reference(Next) ->
            Ref = (khash:get(Nodes, Next))#node.prev
    end,
    validate(Lru, Next).


remove_ref(Lru, Ref) ->
    #lru{
        dbs = Dbs,
        nodes = Nodes,
        head = Head,
        tail = Tail
    } = Lru,

    #node{
        dbname = DbName,
        prev = Prev,
        next = Next
    } = khash:get(Nodes, Ref),

    ok = khash:del(Dbs, DbName),
    ok = khash:del(Nodes, Ref),

    set_next(Lru, Prev, Next),
    set_prev(Lru, Next, Prev),

    NewHead = if
        Ref == Head -> Next;
        true -> Head
    end,

    NewTail = if
        Ref == Tail -> Prev;
        true -> Tail
    end,

    Lru#lru{
        head = NewHead,
        tail = NewTail
    }.


set_prev(_Lru, undefined, _) ->
    ok;

set_prev(Lru, Ref, Prev) ->
    Node = khash:get(Lru#lru.nodes, Ref),
    khash:put(Lru#lru.nodes, Ref, Node#node{
        prev = Prev
    }).


set_next(_Lru, undefined, _) ->
    ok;

set_next(Lru, Ref, Next) ->
    Node = khash:get(Lru#lru.nodes, Ref),
    khash:put(Lru#lru.nodes, Ref, Node#node{
        next = Next
    }).


set_links(Lru, Ref, Prev, Next) ->
    Node = khash:get(Lru#lru.nodes, Ref),
    khash:put(Lru#lru.nodes, Ref, Node#node{
        prev = Prev,
        next = Next
    }).


get_links(Lru, Ref) ->
    Node = khash:get(Lru#lru.nodes, Ref),
    {Node#node.prev, Node#node.next}.
