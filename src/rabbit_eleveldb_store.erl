-module(rabbit_eleveldb_store).

-export([open/1, close/1, exists/2, get/2, put/3, delete/2, fold_keys/3]).

-define(READ_OPTIONS, []).
-define(WRITE_OPTIONS, []).
-define(OPEN_OPTIONS, [{create_if_missing, true}]).

open(Dir) ->
    eleveldb:open(Dir, open_options()).

close(DB) ->
    eleveldb:close(DB).

exists(DB, Key) ->
    case eleveldb:get(DB, Key, ?READ_OPTIONS) of
        not_found -> false;
        {ok, _}   -> true;
        {error, _} = E -> E
    end.

get(DB, Key) ->
    eleveldb:get(DB, Key, ?READ_OPTIONS).

put(DB, Key, Val) ->
    eleveldb:put(DB, Key, Val, ?WRITE_OPTIONS).

delete(DB, Key) ->
    eleveldb:delete(DB, Key, ?WRITE_OPTIONS).

fold_keys(DB, Fun, Acc) ->
    eleveldb:fold_keys(DB, Fun, Acc, ?READ_OPTIONS).

open_options() ->
    lists:ukeymerge(1,
                    application:get_env(rabbit,
                                        eleveldb_open_options,
                                        []),
                    ?OPEN_OPTIONS).
