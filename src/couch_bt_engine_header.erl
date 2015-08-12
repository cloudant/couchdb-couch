-module(couch_bt_engine_header).


-export([
    new/0,
    upgrade/1,
    get/2,
    get/3,
    set/3,

    get_db_prop/2,
    get_db_prop/3,
    set_db_prop/3
]).


-define(LATEST_DISK_VERSION, {couch_bt_engine, 100}).


-record(couch_bt_engine_header, {
    disk_version = ?LATEST_DISK_VERSION,
    data = [
        {id_tree_state, nil},
        {seq_tree_state, nil},
        {local_tree_state, nil},
        {security_ptr, nil},
        {purge_ptr, nil},
        {db_props, []}
    ]
}).



new() ->
    #couch_bt_engine_header{}.


upgrade(#couch_bt_engine_header{disk_version = ?LATEST_DISK_VERSION}=H) ->
    H;
upgrade(Header0) ->
    case couch_db_header:is_header(Header0) of
        true ->
            upgrade_old_header(couch_db_header:upgrade(Header0));
        false ->
            throw({invalid_bt_enigne_header, Header0})
    end.


get(Header, Key) ->
    get(Header, Key, undefined).


get(#couch_bt_engine{data = Data}, Key, Default) ->
    case lists:keyfind(Key, 1, Data) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.


set(#couch_bt_engine_header{data = Data} = Header, Key, Value) ->
    NewData = lists:keystore(Key, 1, Data, {Key, Value}),
    Header#couch_bt_engine_header{data = NewData}.


get_db_prop(Header, Key) ->
    get_db_prop(Header, Key, undefined).


get_db_prop(Header, Key, Default) ->
    DbProps = ?MODULE:get(Header, db_props),
    case lists:keyfind(Key, 1, DbProps) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.


set_db_prop(Header, Key, Value) ->
    DbProps = ?MODULE:get(Header, db_props),
    NewProps = lists:keystore(Key, 1, DbProps, {Key, Value}),
    ?MODULE:set(Header, db_props, NewProps).


upgrade_old_header(Header) ->
    DbProps = [
        {update_seq, couch_db_header:update_seq(Header)},
        {purge_seq, couch_db_header:purge_seq(Header)},
        {revs_limit, couch_db_header:revs_limit(Header)},
        {uuid, couch_db_header:uuid(Header)},
        {epochs, couch_db_header:epochs(Header)},
        {compacted_seq, couch_db_header:compacted_seq(Header)}
    ],
    #couch_bt_engine_header{
        data = [
            {id_tree_state, couch_db_header:id_tree_state(Header)},
            {seq_tree_state, couch_db_header:seq_tree_state(Header)},
            {local_tree_state, couch_db_header:local_tree_state(Header)},
            {security_ptr, couch_db_header:security_ptr(Header)},
            {purge_ptr, couch_db_header:purged_docs(Header)},
            {db_props, DbProps}
        ]
    }.
