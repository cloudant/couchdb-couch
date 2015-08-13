

-record(st, {
    filepath,
    fd,
    fd_monitor,
    fsync_options,
    header,
    needs_commit,
    id_tree,
    seq_tree,
    local_tree,
    compression
}).
