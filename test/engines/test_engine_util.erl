-module(test_engine_util).
-compile(export_all).


gather(Module) ->
    Exports = Module:module_info(exports),
    Tests = lists:foldl(fun({Fun, Arity}, Acc) ->
        case {atom_to_list(Fun), Arity} of
            {[$c, $e, $t, $_ | _], 0} ->
                [{spawn, fun Module:Fun/Arity} | Acc];
            _ ->
                Acc
        end
    end, [], Exports),
    Tests.
    


get_engine() ->
    {ok, {_, Engine}} = application:get_env(couch, test_engine),
    Engine.


rootdir() ->
    config:get("couchdb", "database_dir", ".").


dbpath() ->
    binary_to_list(filename:join(rootdir(), couch_uuids:random())).