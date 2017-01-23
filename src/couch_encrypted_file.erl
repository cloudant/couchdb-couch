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
    key,
    eof
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
    gen_server:call(Pid, {pread_binary, Pos}, infinity).


append_term(Pid, Term) when is_pid(Pid) ->
    Bin = term_to_binary(Term, [compressed]),
    append_binary(Pid, Bin).


append_binary(Pid, Bin) when is_pid(Pid), is_binary(Bin) ->
    gen_server:call(Pid, {append_binary, Bin}, infinity).


sync(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, sync, infinity).


%% gen_server functions

init({Filepath, Key, Options}) ->
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

handle_call({pread_binary, Pos}, _From, #file{} = File) ->
    case pread_binary_int(File, Pos) of
        {ok, Bin} ->
            {reply, {ok, Bin}, File};
        Else ->
            {reply, Else, File}
    end;

handle_call({append_binary, Bin}, _From, #file{} = File0) ->
    File1 = append_binary_int(File0, Bin),
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


pread_binary_int(#file{} = File, Pos) ->
    {ok, <<Size:32, ExpectedHmac:32/binary>>} = file:pread(File#file.fd, Pos  + 3, 36),
    {ok, <<CipherText/binary>>} = file:pread(File#file.fd, Pos + 39, Size),

    %% check hmac without timing effect
    ActualHmac = crypto:hmac(sha256, File#file.key, [<<Size:32>>, CipherText]),
    case crypto:exor(ExpectedHmac, ActualHmac) of
        <<0:256>> ->
            {ok, decrypt(File#file.key, Pos, CipherText)};
        _ ->
            {error, hmac_verification_failed}
    end.


append_binary_int(#file{} = File, PlainText) when is_binary(PlainText) ->
    CipherText = encrypt(File#file.key, File#file.eof, PlainText),
    Size = size(CipherText),
    Hmac = crypto:hmac(sha256, File#file.key, [<<Size:32>>, CipherText]),

    Bytes = [<<Size:32>>, Hmac, CipherText],
    ok = file:write(File#file.fd, [<<$B, $I, $N>>, Bytes]),
    File#file{eof = File#file.eof + 3 + iolist_size(Bytes)}.


encrypt(Key, Pos, PlainText) ->
    State = crypto:stream_init(aes_ctr, Key, essiv(Key, Pos)),
    {_, CipherText} = crypto:stream_encrypt(State, PlainText),
    CipherText.


decrypt(Key, Pos, CipherText) ->
    State = crypto:stream_init(aes_ctr, Key, essiv(Key, Pos)),
    {_, PlainText} = crypto:stream_decrypt(State, CipherText),
    PlainText.


essiv(Key, Pos) ->
    Hash = crypto:hash(sha256, Key),
    State = crypto:stream_init(aes_ctr, Hash, <<0:128>>),
    {_, Result} = crypto:stream_encrypt(State, <<Pos:128>>),
    Result.

init_file(KEK, #file{eof = 0} = File) ->
    Key = crypto:strong_rand_bytes(32),
    WrappedKey = rfc3394:wrap(KEK, Key),
    ok = file:write(File#file.fd, [<<$K, $E, $Y>>, WrappedKey]),
    ok = file:sync(File#file.fd),
    File#file{eof = 3 + size(WrappedKey),
              key = Key};

init_file(KEK, #file{} = File) ->
    {ok, <<WrappedKey:40/binary>>} = file:pread(File#file.fd, 3, 40),
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
