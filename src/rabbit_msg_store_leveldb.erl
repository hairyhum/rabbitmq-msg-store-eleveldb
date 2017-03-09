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

-module(rabbit_msg_store_leveldb).

-behaviour(rabbit_msg_store_behaviour).
-export([start_link/4, successfully_recovered_state/1,
         client_init/4, client_terminate/1, client_delete_and_terminate/1,
         client_ref/1, close_all_indicated/1,
         write/3, write_flow/3, read/2, contains/2, remove/2]).

-behaviour(gen_server2).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_call/4, prioritise_cast/3,
         prioritise_info/3, format_message_queue/2]).

-record(state, {
    dir,
    msg_db,
    ref_count_db,
    credit_disc_bound,
    successfully_recovered,
    clients,
    dying_clients}).
-record(client_state, {
    server,
    client_ref,
    dir,
    msg_db,
    ref_count_db,
    credit_disc_bound}).

-define(REFCOUNT_MODULE, rabbit_msg_store_ref_count_ets).
-define(CLEAN_FILENAME, "clean.dot").
-define(READ_OPTIONS, []).
-define(WRITE_OPTIONS, []).
-define(OPEN_OPTIONS, [{create_if_missing, true}]).

-type msg_ref_delta_gen(A) :: rabbit_msg_store_behaviour:msg_ref_delta_gen().
-type maybe_msg_id_fun() :: rabbit_msg_store_behaviour:maybe_msg_id_fun().
-type maybe_close_fds_fun() :: rabbit_msg_store_behaviour:maybe_close_fds_fun().
-type server() :: rabbit_msg_store_behaviour:server().
-type client_ref() :: rabbit_msg_store_behaviour:client_ref().
-type msg() :: rabbit_msg_store_behaviour:msg().


%% ====================================
%% Public API
%% ====================================
-spec start_link
        (atom(), file:filename(), [binary()] | 'undefined',
         {msg_ref_delta_gen(A), A}) -> rabbit_types:ok_pid_or_error().
start_link(Type, Dir, ClientRefs, StartupFunState) when is_atom(Type) ->
    gen_server2:start_link(?MODULE,
                           [Type, Dir, ClientRefs, StartupFunState],
                           [{timeout, infinity}]).

-spec successfully_recovered_state(server()) -> boolean().
successfully_recovered_state(Server) ->
    gen_server2:call(Server, successfully_recovered_state, infinity).

-spec client_init(server(), client_ref(), maybe_msg_id_fun(),
                        maybe_close_fds_fun()) -> client_msstate().
client_init(Server, Ref, MsgOnDiskFun, CloseFDsFun) when is_pid(Server); is_atom(Server) ->
    {Dir, MsgDb, RefCountDB} =
        gen_server2:call(
          Server,
          {new_client_state, Ref, self(), MsgOnDiskFun, CloseFDsFun},
          infinity),
    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          ?CREDIT_DISC_BOUND),
    #client_state { server             = Server,
                    client_ref         = Ref,
                    dir                = Dir,
                    msg_db             = MsgDb,
                    ref_count_db       = RefCountDB,
                    credit_disc_bound  = CreditDiscBound }.

-spec client_terminate(client_msstate()) -> 'ok'.
client_terminate(CState = #client_state { client_ref = Ref }) ->
    ok = server_call(CState, {client_terminate, Ref}).

-spec client_delete_and_terminate(client_msstate()) -> 'ok'.
client_delete_and_terminate(CState = #client_state { client_ref = Ref }) ->
    ok = server_cast(CState, {client_dying, Ref}),
    ok = server_cast(CState, {client_delete, Ref}).

-spec client_ref(client_msstate()) -> client_ref().
client_ref(#client_state { client_ref = Ref }) -> Ref.

-spec write_flow(rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.
write_flow(MsgId, Msg,
           CState = #client_state {
                       server = Server,
                       client_ref = CRef,
                       credit_disc_bound = CreditDiscBound }) ->
    %% Here we are tracking messages sent by the
    %% rabbit_amqqueue_process process via the
    %% rabbit_variable_queue. We are accessing the
    %% rabbit_amqqueue_process process dictionary.
    credit_flow:send(Server, CreditDiscBound),
    server_cast(CState, {write, CRef, MsgId, Msg, flow}).

-spec write(rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.
write(MsgId, Msg, CState = #client_state{ client_ref = CRef }) ->
    server_cast(CState, {write, CRef, MsgId, Msg, noflow}).

-spec read(rabbit_types:msg_id(), client_msstate()) ->
                     {rabbit_types:ok(msg()) | 'not_found', client_msstate()}.
read(MsgId, CState) ->
% rabbit_log:error("Read message ~p~n from client ~p~n", [MsgId, CState]),
    case positive_ref_count(MsgId, CState) of
        false -> {not_found, CState};
        true  -> {read_message(MsgId, CState), CState}
    end.

-spec contains(rabbit_types:msg_id(), client_msstate()) -> boolean().
contains(MsgId, CState) -> positive_ref_count(MsgId, CState).

-spec remove([rabbit_types:msg_id()], client_msstate()) -> 'ok'.
remove([],    _CState) -> ok;
remove(MsgIds, CState = #client_state { client_ref = CRef }) ->
    server_cast(CState, {remove, CRef, MsgIds}).

-spec close_all_indicated
        (client_msstate()) -> rabbit_types:ok(client_msstate()).
close_all_indicated(CState) -> {ok, CState}.

%% ====================================
%% gen_server2 callbacks
%% ====================================


init([Type, BaseDir, ClientRefs, StartupFunState]) ->
    process_flag(trap_exit, true),
    Dir = filename:join(BaseDir, atom_to_list(Type)),
    Name = filename:join(filename:basename(BaseDir), atom_to_list(Type)),
    RefCountModule = application:get_env(rabbitmq_msg_store_eleveldb, ref_count_module, ?REFCOUNT_MODULE),

    rabbit_log:info("Message store ~tp: using ~p to provide reference counter~n", [Name, RefCountModule]),

    filelib:ensure_dir(filename:join(Dir, "nothing")),

    DB = start_db(Dir),

    {CleanShutdown, RefCountDB} =
        recover_ref_count(RefCountModule, DB, ClientRefs, Dir, StartupFunState, Name),

    ClientRefs1 = case CleanShutdown of
        true  -> ClientRefs;
        false -> []
    end,

    Clients = dict:from_list(
                [{CRef, {undefined, undefined, undefined}} ||
                    CRef <- ClientRefs1]),

    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          ?CREDIT_DISC_BOUND),

    State = #state {
        dir                    = Dir,
        msg_db                 = DB,
        ref_count_db           = RefCountDB,
        % TODO: maybe use a set
        dying_clients          = #{},
        clients                = Clients,
        successfully_recovered = CleanShutdown,
        credit_disc_bound      = CreditDiscBound
    },

    {ok, State,
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        successfully_recovered_state                        -> 7;
        {new_client_state, _Ref, _Pid, _MODC, _CloseFDsFun} -> 7;
        _                                                   -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {client_dying, _Pid}                               -> 7;
        _                                                  -> 0
    end.

prioritise_info(_Msg, _Len, _State) -> 0.

handle_call(successfully_recovered_state, _From, State) ->
    reply(State#state.successfully_recovered, State);


handle_call({new_client_state, CRef, CPid, MsgOnDiskFun, CloseFDsFun}, _From,
            State = #state { dir          = Dir,
                             clients      = Clients,
                             msg_db       = MsgDb,
                             ref_count_db = RefCountDB}) ->
    Clients1 = dict:store(CRef, {CPid, MsgOnDiskFun, CloseFDsFun}, Clients),
    erlang:monitor(process, CPid),
    reply({Dir, MsgDb, RefCountDB},
          State #state { clients = Clients1 });

handle_call({client_terminate, CRef}, _From, State) ->
    ok = wait_for_client_operations(CRef, State),
    reply(ok, clear_client(CRef, State)).

handle_cast({client_dying, CRef},
            State = #state { dying_clients       = DyingClients }) ->
    DyingClients1 = maps:put(CRef, ok, DyingClients),
    noreply(State #state { dying_clients = DyingClients1 });

handle_cast({client_delete, CRef},
            State = #state { clients = Clients }) ->
    State1 = State #state { clients = dict:erase(CRef, Clients) },
    noreply(clear_client(CRef, State1));

handle_cast({write, CRef, MsgId, Msg, Flow},
            State = #state { clients           = Clients,
                             credit_disc_bound = CreditDiscBound }) ->
    case Flow of
        flow   -> {CPid, _, _} = dict:fetch(CRef, Clients),
                  %% We are going to process a message sent by the
                  %% rabbit_amqqueue_process. Now we are accessing the
                  %% msg_store process dictionary.
                  credit_flow:ack(CPid, CreditDiscBound);
        noflow -> ok
    end,
    %% We don't ignore any writes from dying clients, for simplicity.
    State1 = case positive_ref_count(MsgId, State) of
        true  ->
            increase_ref_count(MsgId, State),
            client_confirm(CRef, gb_sets:singleton(MsgId), written, State);
        %% If ref_count is 0 or doesn't exist - write a message to the msg store
        false ->
        % rabbit_log:error("Write message ~p~n ~p~n from server ~p~n", [MsgId, Msg, State]),
            write_message(MsgId, Msg, State),
            increase_ref_count(MsgId, State),
            client_confirm(CRef, gb_sets:singleton(MsgId), written, State)
    end,
    noreply(State1);

handle_cast({remove, CRef, MsgIds}, State) ->
    %% We remove all the message IDs we receive, so none of them gets ignored.
    lists:foreach(
        fun (MsgId) ->
            case decrease_ref_count(MsgId, State) of
                I when is_integer(I), I =< 0 ->
                    ok = delete_ref_count(MsgId, State),
                    remove_message(MsgId, State);
                _ -> ok
            end
        end,
        MsgIds),
    noreply(client_confirm(CRef, gb_sets:from_list(MsgIds), ignored, State)).

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    %% similar to what happens in
    %% rabbit_amqqueue_process:handle_ch_down but with a relation of
    %% msg_store -> rabbit_amqqueue_process instead of
    %% rabbit_amqqueue_process -> rabbit_channel.
    credit_flow:peer_down(Pid),
    noreply(State);

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

terminate(_Reason, #state { msg_db       = MsgDb,
                            ref_count_db = RefCountDB,
                            clients      = Clients,
                            dir          = Dir }) ->
    ok = stop_db(MsgDb, Dir),
    ok = stop_ref_counter(RefCountDB, Dir),
    case store_recovery_terms([{client_refs, dict:fetch_keys(Clients)},
                               {ref_count_module, ?REFCOUNT_MODULE}], Dir) of
        ok           -> ok;
        {error, RTErr} ->
            rabbit_log:error("Unable to save message store recovery terms"
                             "for directory ~p~nError: ~p~n",
                             [Dir, RTErr])
    end,
    ok.

%% ====================================
%% Internal functions
%% ====================================


recover_ref_count(RefCountModule, DB, undefined, Dir, StartupFunState, _) ->
    {false, recount_ref_counter(RefCountModule, Dir, DB, StartupFunState)};
recover_ref_count(RefCountModule, DB, ClientRefs, Dir, StartupFunState, Name) ->
    Fresh = fun (ErrorMsg, ErrorArgs) ->
                    rabbit_log:warning("Message store ~tp : " ++ ErrorMsg ++ "~n"
                                       "rebuilding reference counter from scratch~n",
                                       [Name | ErrorArgs]),
                    {false, recount_ref_counter(RefCountModule, Dir, DB, StartupFunState)}
            end,
    case read_recovery_terms(Dir) of
        {false, Error} ->
            Fresh("failed to read recovery terms: ~p", [Error]);
        {true, Terms} ->
            RecClientRefs  = proplists:get_value(client_refs, Terms, []),
            case {(lists:sort(ClientRefs) =:= lists:sort(RecClientRefs)),
                  RefCountModule == proplists:get_value(ref_count_module, Terms)} of
                {true, true}  ->
                    case load_ref_counter(RefCountModule, Dir) of
                        {ok, RefCountDB} ->
                            {true, RefCountDB};
                        {error, Error} ->
                            Fresh("failed to recover reference counter: ~p", [Error])
                    end;
                false -> Fresh("recovery terms differ from present", [])
            end
    end.

recount_ref_counter(RefCountModule, Dir, DB, {MsgRefDeltaGen, MsgRefDeltaGenInit}) ->
    {ok, RefCountDB} = start_clean_ref_counter(RefCountModule, Dir),
    ok = count_msg_refs(MsgRefDeltaGen, MsgRefDeltaGenInit, RefCountDB, DB),
    RefCountDB.

count_msg_refs(Gen, Seed, RefCountDB, DB) ->
    case Gen(Seed) of
        finished ->
            ok;
        {_MsgId, 0, Next} ->
            count_msg_refs(Gen, Next, RefCountDB, DB);
        {MsgId, Delta, Next} ->
            case message_exists(MsgId, DB) of
                true  -> modify_ref_count(MsgId, RefCountDB, Delta),
                         ok;
                false -> ok
            end,
            count_msg_refs(Gen, Next, RefCountDB, DB)
    end.

store_recovery_terms(Terms, Dir) ->
    rabbit_file:write_term_file(filename:join(Dir, ?CLEAN_FILENAME), Terms).

read_recovery_terms(Dir) ->
    Path = filename:join(Dir, ?CLEAN_FILENAME),
    case rabbit_file:read_term_file(Path) of
        {ok, Terms}    -> case file:delete(Path) of
                              ok             -> {true,  Terms};
                              {error, Error} -> {false, Error}
                          end;
        {error, Error} -> {false, Error}
    end.

positive_ref_count(MsgId, #client_state{ ref_count_db = RefCountDB }) ->
    positive_ref_count(MsgId, RefCountDB);
positive_ref_count(MsgId, #state{ ref_count_db = RefCountDB }) ->
    positive_ref_count(MsgId, RefCountDB);
positive_ref_count(MsgId, {RefCountModule, RefCountState}) ->
    case RefCountModule:find(MsgId, RefCountState) of
        I when is_integer(I), I > 0 -> true;
        _                           -> false
    end.

load_ref_counter(RefCountModule, Dir) ->
    case RefCountModule:load(Dir) of
        {ok, RefCountState} -> {ok, {RefCountModule, RefCountState}};
        {error, Err}        -> {error, Err}
    end.

start_clean_ref_counter(RefCountModule, Dir) ->
    {ok, {RefCountModule, RefCountModule:start_clean(Dir)}}.

stop_ref_counter({RefCountModule, RefCountState}, Dir) ->
    RefCountModule:save(RefCountState, Dir).

%% Client operations are synchronous.
wait_for_client_operations(_CRef, _State) -> ok.


clear_client(CRef, State = #state{ dying_clients = DyingClients }) ->
    State#state{ dying_clients = maps:remove(CRef, DyingClients) }.

increase_ref_count(MsgId, #state{ ref_count_db = RefCountDB }) ->
    modify_ref_count(MsgId, RefCountDB, 1).

decrease_ref_count(MsgId, #state{ ref_count_db = RefCountDB }) ->
    modify_ref_count(MsgId, RefCountDB, -1).

modify_ref_count(MsgId, {RefCountModule, RefCountState}, Delta) ->
    RefCountModule:add(MsgId, Delta, RefCountState).

delete_ref_count(MsgId, #state{ ref_count_db = {RefCountModule, RefCountState} }) ->
    RefCountModule:delete(MsgId, RefCountState).

read_message(MsgId, #client_state{ msg_db = MsgDb }) ->
    do_read_message(MsgId, MsgDb).

write_message(MsgId, Msg, #state{ msg_db = MsgDb }) ->
    do_write_message(MsgId, Msg, MsgDb).

remove_message(MsgId, #state{ msg_db = MsgDb }) ->
    do_remove_message(MsgId, MsgDb).

%% ====================================
%% Internal helpers
%% ====================================

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

server_call(#client_state { server = Server }, Msg) ->
    gen_server2:call(Server, Msg, infinity).

server_cast(#client_state { server = Server }, Msg) ->
    gen_server2:cast(Server, Msg).


%% ====================================
%% DB functions
%% ====================================

start_db(Dir) ->
    {ok, DbRef} = eleveldb:open(Dir, open_options()),
    DbRef.

stop_db(DB, Dir) ->
    case eleveldb:close(DB) of
        ok           -> ok;
        {error, Err} ->
            rabbit_log:error("Unable to stop message store"
                             " for directory ~p.~nError: ~p~n",
                             [filename:dirname(Dir), Err]),
            error(Err)
    end.

message_exists(MsgId, MsgDb) ->
    case eleveldb:get(MsgDb, MsgId, ?READ_OPTIONS) of
        not_found -> false;
        {ok, _}   -> true;
        {error, Err} -> error(Err)
    end.

do_read_message(MsgId, MsgDb) ->
    case eleveldb:get(MsgDb, MsgId, ?READ_OPTIONS) of
        not_found    -> not_found;
        {ok, Val}    -> {ok, binary_to_term(Val)};
        {error, Err} -> error(Err)
    end.

do_write_message(MsgId, Msg, MsgDb) ->
    Val = term_to_binary(Msg),
    ok = eleveldb:put(MsgDb, MsgId, Val, ?WRITE_OPTIONS).

do_remove_message(MsgId, MsgDb) ->
    ok = eleveldb:delete(MsgDb, MsgId, ?WRITE_OPTIONS).

open_options() ->
    lists:ukeymerge(1,
                    application:get_env(rabbit,
                                        eleveldb_open_options,
                                        []),
                    ?OPEN_OPTIONS).

client_confirm(CRef, MsgIds, ActionTaken, State) ->
    #state { clients = Clients} = State,
    case dict:fetch(CRef, Clients) of
        {_CPid, undefined, _CloseFDsFun}    ->
            State;
        {_CPid, MsgOnDiskFun, _CloseFDsFun} ->
            MsgOnDiskFun(MsgIds, ActionTaken),
            State
    end.

