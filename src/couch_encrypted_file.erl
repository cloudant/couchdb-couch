-module(couch_encrypted_file).
-behaviour(gen_server).
-vsn(3).

%% public API
-export([open/3, close/1, sync/1]).
-export([pread_term/2, pread_binary/2]).
-export([append_term/2, append_binary/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([format_status/2]).

-record(file, {
    fd,
    eof,
    key,
    iv = 0
}).

%% public API

open(Filepath, Key, Options) ->
    gen_server:start_link(couch_encrypted_file, {Filepath, Key, Options}, []).


close(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, close).


pread_term(Pid, Pos) when is_pid(Pid), is_integer(Pos) ->
    {ok, Bin} = pread_binary(Pid, Pos),
    {ok, binary_to_term(Bin)}.


pread_binary(Pid, Pos) when is_pid(Pid), is_integer(Pos) ->
    {ok, Key, IV, CipherText, CipherTag} =
        gen_server:call(Pid, {pread_binary, Pos}, infinity),
    PlainText = crypto:block_decrypt(aes_gcm, Key, essiv(Key, IV),
        {[], CipherText, CipherTag}),
    {ok, PlainText}.


append_term(Pid, Term) when is_pid(Pid) ->
    Bin = term_to_binary(Term, [compressed]),
    append_binary(Pid, Bin).


append_binary(Pid, PlainText) when is_pid(Pid), is_binary(PlainText) ->
    {ok, Key, IV} = gen_server:call(Pid, get_key_iv, infinity),
    {CipherText, CipherTag} = crypto:block_encrypt(aes_gcm,
        Key, essiv(Key, IV), {[], PlainText, 16}),
    Bytes = [
        <<$B, $I, $N>>, %% conspicuous marker for DR
        <<IV:32>>, %% plaintext for DR
        <<(size(CipherText)):32>>,
        CipherText,
        CipherTag
    ],
    gen_server:call(Pid, {append_binary, Bytes}, infinity).


sync(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, sync, infinity).


%% gen_server functions

init({Filepath, Key, Options}) ->
    process_flag(sensitive, true),
    OpenOptions = file_open_options(Options),
    case lists:member(create, Options) of
        true ->
            filelib:ensure_dir(Filepath),
            case file:open(Filepath, OpenOptions) of
                {ok, Fd} ->
                    ok = maybe_overwrite(Fd, Options),
                    {ok, Eof} = file:position(Fd, eof),
                    {ok, init_file(Key, #file{fd = Fd, eof = Eof})};
                {error, Reason} ->
                    {stop, Reason}
            end;
        false ->
            %% open in read mode first, so we don't create the file if it doesn't exist.
            case file:open(Filepath, [read, raw]) of
                {ok, Fd_Read} ->
                    {ok, Fd} = file:open(Filepath, OpenOptions),
                    ok = file:close(Fd_Read),
                    {ok, Eof} = file:position(Fd, eof),
                    {ok, init_file(Key, #file{fd = Fd, key = Key, eof = Eof})};
                 {error, Reason} ->
                    {stop, Reason}
            end
    end.

handle_call(get_key_iv, _From, #file{iv = IV} = File) when IV >= 4294967296 ->
    {stop, iv_overflow, File};

handle_call(get_key_iv, _From, #file{} = File0) ->
    File1 = File0#file{iv = File0#file.iv + 1},
    {reply, {ok, File0#file.key, File0#file.iv}, File1};

handle_call({pread_binary, Pos}, _From, #file{} = File) ->
    {ok, <<$B, $I, $N, IV:32, Size:32>>} =
        file:pread(File#file.fd, Pos, 11),
    {ok, <<CipherText:Size/binary, CipherTag:16/binary>>} =
        file:pread(File#file.fd, Pos + 11, Size + 16),
    {reply, {ok, File#file.key, IV, CipherText, CipherTag}, File};

handle_call({append_binary, Bin}, _From, #file{} = File0) ->
    ok = file:write(File0#file.fd, Bin),
    File1 = File0#file{eof = File0#file.eof + iolist_size(Bin)},
    {reply, {ok, File0#file.eof}, File1};

handle_call(sync, _From, #file{} = File) ->
    {reply, file:sync(File#file.fd), File};

handle_call(_Msg, _From, #file{} = File) ->
    {noreply, File}.


handle_cast(close, #file{} = File) ->
    ok = file:close(File#file.fd),
    {stop, normal, File#file{fd = undefined}};

handle_cast(_Msg, #file{} = File) ->
    {noreply, File}.


handle_info(_Msg, #file{} = File) ->
    {noreply, File}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, #file{fd = undefined}) ->
    ok;

terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).


format_status(_Opt, [_PDict, #file{} = File]) ->
    [{data, [{"State", File#file{key = redacted}}]}].


%% private functions

file_open_options(Options) ->
    case lists:member(read_only, Options) of
        true ->
            [read, raw, binary];
        false ->
            [read, raw, binary, append]
    end.


maybe_overwrite(Fd, Options) ->
    case lists:member(overwrite, Options) of
        true ->
            {ok, 0} = file:position(Fd, 0),
            ok = file:truncate(Fd),
            file:sync(Fd);
        false ->
            ok
    end.


essiv(Key, Pos) ->
    Hash = crypto:hash(sha256, Key),
    State = crypto:stream_init(aes_ctr, Hash, <<0:128>>),
    {_, Result} = crypto:stream_encrypt(State, <<Pos:128>>),
    Result.


init_file(KEK, #file{eof = 0} = File) ->
    Key = crypto:strong_rand_bytes(32),
    WrappedKey = rfc3394:wrap(KEK, Key),
    ok = file:write(File#file.fd, [
        <<$K, $E, $Y>>, %% conspicuous marker for DR
        WrappedKey]),
    ok = file:sync(File#file.fd),
    File#file{eof = 3 + size(WrappedKey),
              key = Key};


init_file(KEK, #file{} = File) ->
    {ok, <<$K, $E, $Y, WrappedKey:40/binary>>} = file:pread(File#file.fd, 0, 43),
    Key = rfc3394:unwrap(KEK, WrappedKey),
    File#file{key = Key}.


-ifdef(TEST).
-include_lib("couch/include/couch_eunit.hrl").

bin_test() ->
    Key = crypto:strong_rand_bytes(32),
    Bin = crypto:strong_rand_bytes(32),
    {ok, Fd} = couch_encrypted_file:open(?tempfile(), Key, [create, overwrite]),
    case couch_encrypted_file:append_binary(Fd, Bin) of
        {ok, Pos} ->
            ?assertMatch({ok, Bin}, couch_encrypted_file:pread_binary(Fd, Pos));
        _Else ->
            ?assert(false)
    end.

term_test() ->
    Key = crypto:strong_rand_bytes(32),
    {ok, Fd} = couch_encrypted_file:open(?tempfile(), Key, [create, overwrite]),
    case couch_encrypted_file:append_term(Fd, hello) of
        {ok, Pos} ->
            ?assertMatch({ok, hello}, couch_encrypted_file:pread_term(Fd, Pos));
        _Else ->
            ?assert(false)
    end.

-endif.
