%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%


-module(rabbit_msg_store_ref_count_ets).

-export([start_clean/1, load/1, save/2]).
-export([find/2, add/3, delete/2]).

-define(FILE_NAME, "ref_count_ets").

ets_file(Dir) ->
    filename:join(Dir, ?FILE_NAME).

start_clean(Dir) ->
    ok = delete_file(ets_file(Dir)),
    Table = ets:new(?MODULE, [set, public]),
    {ok, Table}.

load(Dir) ->
    EtsFile = ets_file(Dir),
    case ets:file2tab(EtsFile) of
        {ok, Tab} ->
            ok = delete_file(EtsFile),
            {ok, Tab};
        {error, Err} ->
             ok = delete_file(EtsFile),
             {error, Err}
    end.

save(Tab, Dir) ->
    EtsFile = ets_file(Dir),
    ets:tab2file(Tab, EtsFile).

find(Key, Tab) ->
    case ets:lookup(Tab, Key) of
        []         -> not_found;
        [{Key, I}] -> I
    end.

add(Key, Delta, Tab) ->
    ets:update_counter(Tab, Key, Delta, {Key, 0}).

delete(Key, Tab) ->
    true = ets:delete(Tab, Key),
    ok.

delete_file(File) ->
    rabbit_file:recursive_delete([File]).