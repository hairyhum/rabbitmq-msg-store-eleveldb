-module(rabbit_leveled_store).

-define(BUCKET, <<"bucket">>).
-define(INDEX_SPECS, []).

-export([open/1, close/1, get/2, put/3, delete/2, exists/2, fold_keys/3]).

-spec open(string()) -> {ok, pid()}.
open(Dir) ->
    open(Dir, #{}).

-spec open(string(), map()) -> {ok, pid()}.
open(Path, Options) ->
    LedgerCacheSize = maps:get(cache_size, Options, 2000),
    JournalSize = maps:get(journal_size, Options, 500000000),
    SyncStrategy = maps:get(sync_strategy, Options, sync),
    leveled_bookie:book_start(Path, LedgerCacheSize, JournalSize, SyncStrategy).

close(Server) ->
    leveled_bookie:book_close(Server).

-spec get(pid(), binary()) -> {ok, binary()} | not_found.
get(Server, Key) ->
    leveled_bookie:book_get(Server, ?BUCKET, Key).

-spec put(pid(), binary(), binary()) -> ok.
put(Server, Key, Val) ->
rabbit_log:error("Leveled put ~p~n", [Key]),
    leveled_bookie:book_put(Server, ?BUCKET, Key, Val, ?INDEX_SPECS).

-spec delete(pid(), binary()) -> ok.
delete(Server, Key) ->
    leveled_bookie:book_delete(Server, ?BUCKET, Key, ?INDEX_SPECS).

-spec exists(pid(), binary()) -> boolean().
exists(Server, Key) ->
    case leveled_bookie:book_head(Server, ?BUCKET, Key) of
        not_found -> false;
        _         -> true
    end.

-spec fold_keys(pid(), fun((binary(), Acc) -> Acc), Acc) -> Acc.
fold_keys(Server, Fun, Acc) ->
    {async, F} = leveled_bookie:book_returnfolder(
        Server,
        {keylist, o, ?BUCKET, {fun(_B, E, A) -> Fun(E, A) end, Acc}}
    ),
    F().

