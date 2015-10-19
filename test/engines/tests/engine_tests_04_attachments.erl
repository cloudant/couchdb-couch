-module(engine_tests_04_attachments).
-compile(export_all).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


attachments_test_() ->
    test_engine_util:gather(?MODULE).


cet_write_attachment() ->
    {ok, Engine, DbPath, St1} = test_engine_util:init_engine(dbpath),

    AttBin = <<"Hello, World!\n">>,

    Atts = test_engine_util:prep_atts(Engine, St1, [
            {<<"ohai.txt">>, AttBin}
        ]),
    Actions = [{create, {<<"first">>, [], Atts}}],
    {ok, St2} = test_engine_util:apply_actions(Engine, St1, Actions),
    {ok, St3} = Engine:commit_data(St2),
    Engine:terminate(normal, St3),

    {ok, St4} = Engine:init(DbPath, []),
    [FDI] = Engine:open_docs(St4, [<<"first">>]),
    #rev_info{
        body_sp = Ptr
    } = test_engine_util:prev_rev(FDI),

    {ok, {_, Atts0}} = Engine:read_doc(St4, Ptr),
    Atts1 = if not is_binary(Atts0) -> Atts0; true ->
        couch_compress:decompress(Atts0)
    end,

    StreamSrc = fun(Sp) -> Engine:open_read_stream(St4, Sp) end,
    [Att] = [couch_att:from_disk_term(StreamSrc, T) || T <- Atts1],
    ReadBin = couch_att:to_binary(Att),

    ?assertEqual(AttBin, ReadBin).


    