% Cribbed from rebar_cover_util

-module(test_engine_cover).

-export([
    init/2,
    finish/1
]).


-record(st, {
    target_dir,
    engine_app,
    test_dir,
    cover_log,
    app_beams,
    test_beams
}).


init(EngineApp, TestDir) ->
    case init:get_argument(cover_engine) of
        {ok, [["true"]]} ->
            try
                do_init(EngineApp, TestDir)
            catch E:R ->
                S = erlang:get_stacktrace(),
                io:format(standard_error, "Cover error: ~p~n~p~n", [{E,R}, S]),
                {ok, disabled}
            end;
        _ ->
            {ok, disabled}
    end.


finish(St) ->
    case St of
        #st{} ->
            do_finish(St);
        disabled ->
            ok
    end.


do_init(EngineApp, TestDir) ->
    TargetDir = filename:join([TestDir, "..", "coverage"]),
    case filelib:is_dir(TargetDir) of
        true ->
            ok;
        false ->
            filelib:ensure_dir(filename:join(TargetDir, "foo"))
    end,

    {ok, CoverPid} = case whereis(cover_server) of
        undefined ->
            cover:start();
        _ ->
            cover:stop(),
            cover:start()
    end,

    LogFile = filename:join(TargetDir, "cover.log"),
    {ok, Leader} = file:open(LogFile, [write]),
    erlang:group_leader(Leader, CoverPid),

    AppBeams = get_engine_app_beams(EngineApp),
    AppCoverBeams = cover_compile(AppBeams),
    
    TestBeams = get_test_beams(TestDir),
    TestCoverBeams = cover_compile(TestBeams),

    {ok, #st{
        target_dir = TargetDir,
        engine_app = EngineApp,
        test_dir = TestDir,
        cover_log = Leader,
        app_beams = AppCoverBeams,
        test_beams = TestCoverBeams
    }}.


do_finish(St) ->
    AppCoverage = analyze_mods(St#st.app_beams),
    TestCoverage = analyze_mods(St#st.test_beams),

    write_index(AppCoverage, TestCoverage, St#st.target_dir),
    lists:foreach(fun({Module, _, _}) ->
        BaseName = io_lib:format("~s.COVER.html", [Module]),
        FileName = filename:join(St#st.target_dir, BaseName),
        cover:analyze_to_file(Module, FileName, [html])
    end, AppCoverage ++ TestCoverage),

    file:close(St#st.cover_log).


get_engine_app_beams(EngineApp) ->
    Glob = filename:join([code:lib_dir(EngineApp), "ebin", "*.beam"]),
    get_glob_beams(Glob).


get_test_beams(TestDir) ->
    Glob = filename:join(TestDir, "*.beam"),
    get_glob_beams(Glob).


get_glob_beams(Glob) ->
    Files = filelib:wildcard(Glob),
    lists:map(fun(FileName) ->
        list_to_atom(filename:basename(FileName, ".beam"))
    end, Files).


cover_compile(Beams) ->
    lists:foldl(fun(Beam, Acc) ->
        case cover:compile_beam(Beam) of
            {ok, Module} ->
                [Module | Acc];
            {error, Desc} ->
                io:format("Cover compilation error: ~s: ~p~n", [Beam, Desc]),
                Acc
        end
    end, [], Beams).


analyze_mods(Modules) ->
    lists:flatmap(fun(Module) ->
        case cover:analyze(Module, coverage, module) of
            {ok, {Module, {Covered, NotCovered}}} ->
                [{Module, Covered, NotCovered}];
            {error, Reason} ->
                Args = [Module, Reason],
                io:format("Cover analyze failed for ~p: ~p~n", Args),
                []
        end
    end, Modules).


write_index(AppCoverage, TestCoverage, TargetDir) ->
    {ok, Index} = file:open(filename:join([TargetDir, "index.html"]), [write]),
    ok = file:write(Index,
            "<!DOCTYPE HTML>\n"
            "<html>\n"
            "  <head>\n"
            "    <meta charset=\"utf-8\" />"
            "    <title>Coverage Summary</title>\n"
            "  </head>\n"
            "  <body>\n"
        ),
    write_index_section(Index, "Source", lists:sort(AppCoverage)),
    write_index_section(Index, "Test", lists:sort(TestCoverage)),
    ok = file:write(Index,
            "  </body>\n"
            "</html>\n"
        ),
    ok = file:close(Index).


write_index_section(_Index, _SectionName, []) ->
    ok;

write_index_section(Index, Section, Coverage) ->
    %% Calculate total coverage
    SumFun = fun({_M, C, N}, {CAcc, NAcc}) -> {CAcc + C, NAcc + N} end,
    {Covered, NotCovered} = lists:foldl(SumFun, {0, 0}, Coverage),
    TotalCoverage = percentage(Covered, NotCovered),

    ok = file:write(Index, [
        io_lib:format("    <h1>~s Summary</h1>", [Section]),
        io_lib:format("    <h3>Total: ~s</h3>\n", [TotalCoverage]),
        "    <table>\n"
        "      <tr>\n"
        "        <th>Module</th>\n"
        "        <th>Coverage %</th>\n"
        "      </tr>\n"
    ]),

    Row =
    "      <tr>\n"
    "        <td><a href=\"~s.COVER.html\">~s</a></td>\n"
    "        <td>~s</td>\n"
    "      </tr>",

    lists:foreach(fun({Module, Cov, NotCov}) ->
        Percentage = percentage(Cov, NotCov),
        ok = file:write(Index, io_lib:format(Row, [Module, Module, Percentage]))
    end, Coverage),
    ok = file:write(Index, "    </table>\n").


percentage(0, 0) ->
    "not executed";
percentage(Cov, NotCov) ->
    integer_to_list(trunc((Cov / (Cov + NotCov)) * 100)) ++ "%".
