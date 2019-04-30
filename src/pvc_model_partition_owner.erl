-module(pvc_model_partition_owner).
-include("pvc_model.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2,
         start_partition/1,
         stop_partition/1]).

%% Transactional API
-export([read/4,
         prepare/4,
         decide/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-export([debug_get_state/1]).
-endif.

-type vlog() :: #{key() => pvc_version_log:vlog()}.
-type vlog_last() :: #{key() => non_neg_integer()}.

-record(state, {
    partition :: partition_id(),
    vlog :: vlog(),
    vlog_last :: vlog_last(),

    mrvc :: vc(),
    last_prep :: non_neg_integer(),

    commit_queue :: pvc_commit_queue:cqueue(),
    clog :: pvc_commit_log:clog()
}).

%%%===================================================================
%%% Supervision tree
%%%===================================================================

start_link(Name, Partition) ->
    gen_server:start_link({local, Name}, ?MODULE, [Partition], []).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_partition(partition_id()) -> {ok, pid(), partition_server()} | {error, term()}.
start_partition(Partition) ->
    Name = generate_partition_name(Partition),
    Ret = supervisor:start_child(pvc_model_partition_sup, [Name, Partition]),
    case Ret of
        {ok, Pid} ->
            {ok, Pid, Name};
        Error ->
            Error
    end.

-spec stop_partition(partition_server()) -> ok.
stop_partition(PRef) ->
    gen_server:call(PRef, stop, infinity).

-spec read(partition_id(), key(), vc(), read_partitions()) -> {ok, val(), vc(), vc()} | abort().
read(Partition, Key, VCaggr, HasRead) ->
    To = generate_partition_name(Partition),
    Msg = case ordsets:is_element(Partition, HasRead) of
        true ->
            {vlog_read, Key, VCaggr};
        false ->
            {clog_read, Key, VCaggr, HasRead}
    end,
    gen_server:call(To, Msg).

-spec prepare(Partition :: partition_id(),
              TxId :: tx_id(),
              WriteSet :: ws(),
              PartitionVersion :: non_neg_integer()) -> vote().

prepare(Partition, TxId, WriteSet, PartitionVersion) ->
    To = generate_partition_name(Partition),
    gen_server:call(To, {prepare, TxId, WriteSet, PartitionVersion}).

-spec decide(Partition :: partition_id(),
             TxId :: tx_id(),
             Outcome :: outcome()) -> ok.

decide(Partition, TxId, Outcome) ->
    gen_server:cast(generate_partition_name(Partition), {decide, TxId, Outcome}).

-spec debug_get_state(partition_id()) -> #state{}.
debug_get_state(Partition) ->
    gen_server:call(generate_partition_name(Partition), get_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Partition]) ->
    CLog = pvc_commit_log:new_at(Partition),
    CommitQueue = pvc_commit_queue:new(),

    {ok, #state{partition=Partition,
                vlog=#{},
                vlog_last=#{},
                mrvc=pvc_vclock:new(),
                last_prep=0,
                commit_queue=CommitQueue,
                clog=CLog}}.

handle_call({clog_read, Key, VCaggr, HasRead}, _Sender, State=#state{partition=P, vlog=VLog}) ->
    case partition_ready(VCaggr, State) of
        false ->
            {reply, {error, try_again}, State};
        true ->
            case find_max_vc(VCaggr, HasRead, State) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {ok, MaxVC} ->
                    {Val, CommitVC} = vlog_read(P, Key, MaxVC, VLog),
                    {reply, {ok, Val, CommitVC, MaxVC}, State}
            end
    end;

handle_call({vlog_read, Key, MaxVC}, _Sender, State=#state{partition=P,vlog=VLog}) ->
    {Val, CommitVC} = vlog_read(P, Key, MaxVC, VLog),
    {reply, {ok, Val, CommitVC, MaxVC}, State};

handle_call({prepare, TxId, WriteSet, PartitionVersion}, _Sender, State=#state{partition=P,
                                                                               last_prep=LastPrep,
                                                                               vlog_last=LastCache,
                                                                               commit_queue=CQ}) ->

    Disputed = pvc_commit_queue:contains_disputed(WriteSet, CQ),
    case Disputed of
        true ->
            {reply, {error, abort_conflict}, State};
        false ->
            StaleWS = is_ws_stale(WriteSet, PartitionVersion, LastCache),
            case StaleWS of
                true ->
                    {reply, {error, abort_stale}, State};
                false ->
                    SeqNumber = LastPrep + 1,
                    NewCommitQueue = pvc_commit_queue:enqueue(TxId, WriteSet, CQ),
                    NewState = State#state{last_prep=SeqNumber, commit_queue=NewCommitQueue},
                    {reply, {ok, P, SeqNumber}, NewState}
            end
    end;

handle_call(get_state, _Sender, State) ->
    {reply, State, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(E, _From, S) ->
    io:format("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({decide, TxId, Outcome}, State) ->
    NewQueue = decide_internal(TxId, Outcome, State),
    {noreply, State#state{commit_queue=NewQueue}};

handle_cast(process_queue, State) ->
    NewState = process_queue(State),
    {noreply, NewState};

handle_cast(E, S) ->
    io:format("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(E, S) ->
    io:format("unexpected info: ~p~n", [E]),
    {noreply, S}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Generate a name for this partition owner
-spec generate_partition_name(partition_id()) -> partition_server().
generate_partition_name(Partition) ->
    BinPart = integer_to_binary(Partition),
    Name = <<<<"owner-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

-spec partition_ready(vc(), #state{}) -> boolean().
partition_ready(VCaggr, #state{partition=P,mrvc=MRVC}) ->
    check_time(P, MRVC, VCaggr) =:= ready.

-spec check_time(partition_id(), vc(), vc()) -> ready | not_ready.
check_time(Partition, MostRecentVC, VCaggr) ->
    MostRecentTime = pvc_vclock:get_time(Partition, MostRecentVC),
    AggregateTime = pvc_vclock:get_time(Partition, VCaggr),
    case MostRecentTime < AggregateTime of
        true ->
            not_ready;
        false ->
            ready
    end.

-spec find_max_vc(vc(), read_partitions(), #state{}) -> vc().
find_max_vc(VCaggr, HasRead, #state{partition=P, mrvc=MRVC, clog=CLog}) ->
    MaxVC = case ordsets:is_empty(HasRead) of
        true -> MRVC;
        _ -> pvc_commit_log:get_smaller_from_dots(ordsets:to_list(HasRead), VCaggr, CLog)
    end,
    check_valid_max_vc(P, VCaggr, MaxVC).

-spec check_valid_max_vc(partition_id(), vc(), vc()) -> {ok, vc()} | abort().
check_valid_max_vc(Partition, VCaggr, MaxVC) ->
    MaxSelectedTime = pvc_vclock:get_time(Partition, MaxVC),
    CurrentThresholdTime = pvc_vclock:get_time(Partition, VCaggr),
    ValidVersionTime = MaxSelectedTime >= CurrentThresholdTime,
    case ValidVersionTime of
        true ->
            {ok, MaxVC};
        false ->
            {error, abort_read}
    end.

-spec vlog_read(partition_id(), key(), vc(), vlog()) -> {val(), vc()}.
vlog_read(Partition, Key, MaxVC, VLog) ->
    KeyVLog = maps:get(Key, VLog, pvc_version_log:new(Partition)),
    {Val, CommitVC} = pvc_version_log:get_smaller(MaxVC, KeyVLog),
    {Val, CommitVC}.

-spec is_ws_stale(ws(), non_neg_integer(), vlog_last()) -> boolean().
is_ws_stale(WriteSet, PartitionVersion, LastCache) ->
    check_stale_ws(pvc_writeset:to_list(WriteSet), PartitionVersion, LastCache).

-spec check_stale_ws(list(), non_neg_integer(), vlog_last()) -> boolean().
check_stale_ws([], _, _) -> false;
check_stale_ws([{Key, _} | Rest], Version, Cache) ->
    CommitTime = maps:get(Key, Cache, 0),
    CommitTime > Version orelse check_stale_ws(Rest, Version, Cache).

-spec decide_internal(tx_id(), outcome(), #state{}) -> pvc_commit_queue:cqueue().
decide_internal(TxId, Outcome, #state{partition=Partition, commit_queue=CommitQueue}) ->
    case Outcome of
        abort ->
            pvc_commit_queue:remove(TxId, CommitQueue);
        {ok, CommitVC} ->
            ReadyQueue = pvc_commit_queue:ready(TxId, [], CommitVC, CommitQueue),
            ok = schedule_queue_process(Partition),
            ReadyQueue
    end.

-spec schedule_queue_process(partition_id()) -> ok.
schedule_queue_process(Partition) ->
    gen_server:cast(generate_partition_name(Partition), process_queue).


-spec process_queue(#state{}) -> #state{}.
process_queue(State=#state{partition=P}) ->
    {ReadyTx, NewQueue} = pvc_commit_queue:dequeue_ready(State#state.commit_queue),
    case ReadyTx of
        [] ->
            State#state{commit_queue=NewQueue};
        Entries ->
            lists:foldl(fun(Entry, AccState) ->
                {_TxId, WriteSet, CommitVC, _} = Entry,
                ListWS = pvc_writeset:to_list(WriteSet),

                %% Update VLog and Last Vlog Cache
                NewVLog = update_vlog(P, CommitVC, ListWS, AccState#state.vlog),

                %% Cache VLog.last(Key) \forall Key \in WS
                NewVLogLast = cache_vlog_last(P, CommitVC, ListWS, AccState#state.vlog_last),

                %% Update MRVC
                NewMRVC = pvc_vclock:max(AccState#state.mrvc, CommitVC),
                %% Update Clog
                NewCLog = pvc_commit_log:insert(NewMRVC, AccState#state.clog),
                AccState#state{vlog=NewVLog, vlog_last=NewVLogLast, mrvc=NewMRVC, clog=NewCLog}
            end, State#state{commit_queue=NewQueue}, Entries)
    end.

-spec update_vlog(partition_id(), vc(), [{key(), val()}], vlog()) -> vlog().
update_vlog(Partition, CommitVC, ListWS, VLog) ->
    %% VLog.apply(Key, Val, CommitVC) \forall Key \in WS
    lists:foldl(fun({Key, Value}, AccVlog) ->
        maps_update_with(Key, fun(KeyVLog) ->
            pvc_version_log:insert(CommitVC, Value, KeyVLog)
        end, pvc_version_log:new(Partition), AccVlog)
    end, VLog, ListWS).

-spec cache_vlog_last(partition_id(), vc(), [{key(), val()}], vlog_last()) -> vlog_last().
cache_vlog_last(Partition, CommitVC, ListWS, VLogCache) ->
    CommitTime = pvc_vclock:get_time(Partition, CommitVC),
    lists:foldl(fun({Key, _}, AccCache) ->
        maps:put(Key, CommitTime, AccCache)
    end, VLogCache, ListWS).

%% @doc Similar to maps:update_with/4, but the Initial value is also passed to Fun.
maps_update_with(Key, Fun, Init, Map) when is_function(Fun, 1), is_map(Map) ->
    Val = maps:get(Key, Map, Init),
    maps:put(Key, Fun(Val), Map).
