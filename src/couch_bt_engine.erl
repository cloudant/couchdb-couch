-module(couch_bt_engine).


-record(state, {
    fd,
    fd_monitor,
    fsync_options = [],
    id_tree,
    seq_tree,
    local_tree,
    security_ptr
}).


exists(FilePath) ->
    case filelib:is_file(FilePath) of
        true ->
            true;
        false ->
            filelib:is_file(FilePath ++ ".compact")
    end.


create(FilePath, Options) ->
    % delete any old compaction files that might be hanging around
    RootDir = config:get("couchdb", "database_dir", "."),
    couch_file:delete(RootDir, Filepath ++ ".compact"),
    couch_file:delete(RootDir, Filepath ++ ".compact.data"),
    couch_file:delete(RootDir, Filepath ++ ".compact.meta");
    


delete(RootDir, FilePath, Async) ->
    %% Delete any leftover compaction files. If we don't do this a
    %% subsequent request for this DB will try to open them to use
    %% as a recovery.
    lists:foreach(fun(Ext) ->
        couch_file:delete(Server#server.root_dir, FullFilepath ++ Ext)
    end, [".compact", ".compact.data", ".compact.meta"]),

    % Delete the actual database file
    couch_file:delete(RootDir, FilePath, Async).


init(...) ->
    case lists:member(create, Options) of
    true ->
        % create a new header and writes it to the file
        Header =  couch_db_header:new(),
        ok = couch_file:write_header(Fd, Header),
        % delete any old compaction files that might be hanging around
        RootDir = config:get("couchdb", "database_dir", "."),
        couch_file:delete(RootDir, Filepath ++ ".compact"),
        couch_file:delete(RootDir, Filepath ++ ".compact.data"),
        couch_file:delete(RootDir, Filepath ++ ".compact.meta");
    false ->
        case couch_file:read_header(Fd) of
        {ok, Header} ->
            ok;
        no_valid_header ->
            % create a new header and writes it to the file
            Header =  couch_db_header:new(),
            ok = couch_file:write_header(Fd, Header),
            % delete any old compaction files that might be hanging around
            file:delete(Filepath ++ ".compact"),
            file:delete(Filepath ++ ".compact.data"),
            file:delete(Filepath ++ ".compact.meta")
        end
    end,
    Db = init_db(DbName, Filepath, Fd, Header, Options),
    case lists:member(sys_db, Options) of
        false ->
            couch_stats_process_tracker:track([couchdb, open_databases]);
        true ->
            ok
    end,
    % we don't load validation funs here because the fabric query is liable to
    % race conditions.  Instead see couch_db:validate_doc_update, which loads
    % them lazily
    {ok, Db#db{main_pid = self()}}.


asdf
case open_db_file(Filepath, Options) of
{ok, Fd} ->
    {ok, UpdaterPid} = gen_server:start_link(couch_db_updater, {DbName,
        Filepath, Fd, Options}, []),
    unlink(Fd),
    gen_server:call(UpdaterPid, get_db);
Else ->
    Else
end.


open_db_file(Filepath, Options) ->
    case couch_file:open(Filepath, Options) of
    {ok, Fd} ->
        {ok, Fd};
    {error, enoent} ->
        % couldn't find file. is there a compact version? This can happen if
        % crashed during the file switch.
        case couch_file:open(Filepath ++ ".compact", [nologifmissing]) of
        {ok, Fd} ->
            couch_log:info("Found ~s~s compaction file, using as primary"
                           " storage.", [Filepath, ".compact"]),
            ok = file:rename(Filepath ++ ".compact", Filepath),
            ok = couch_file:sync(Fd),
            {ok, Fd};
        {error, enoent} ->
            {not_found, no_db_file}
        end;
    Error ->
        Error
    end.
