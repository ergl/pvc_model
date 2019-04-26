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

-record(state, {
    partition :: partition_id(),
    vlog :: ets:tab(),
    vlog_last_cache :: ets:tab(),
    %% feels wasteful to waste an entire ETS
    %% just for a single value
    mrvc :: ets:tab(),
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
    case ordsets:is_element(Partition, HasRead) of
        true ->
            %% Read from VLog directly, using VCaggr as MaxVC
            vlog_read(Partition, Key, VCaggr);
        false ->
            clog_read(Partition, Key, VCaggr, HasRead)
    end.

-spec prepare(Partition :: partition_id(),
              TxId :: tx_id(),
              WriteSet :: ws(),
              PartitionVersion :: non_neg_integer()) -> vote().

prepare(Partition, TxId, WriteSet, PartitionVersion) ->
    case is_ws_stale(Partition, WriteSet, PartitionVersion) of
        true ->
            %% Quick fail, can read from ETS
            {error, abort_stale};
        false ->
            %% Need to check with commit queue inside the partition
            try
                gen_server:call(generate_partition_name(Partition), {prepare, TxId, WriteSet})
            catch _:_Reason  ->
                {error, partition_not_started}
            end
    end.

-spec decide(Partition :: partition_id(),
             TxId :: tx_id(),
             Outcome :: outcome()) -> ok.

decide(Partition, TxId, Outcome) ->
    gen_server:cast(generate_partition_name(Partition), {decide, TxId, Outcome}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Partition]) ->
    VLog = persistent_table({vlog, Partition}),
    VLogLastCache = persistent_table({vlog_last_cache, Partition}),

    %% Init MRVC table
    MRVC = persistent_table({mrvc_cache, Partition}),
    true = ets:insert(MRVC, [{mrvc, pvc_vclock:new()}]),

    CLog = pvc_commit_log:new_at(Partition),
    CommitQueue = pvc_commit_queue:new(),
    {ok, #state{partition=Partition,
                vlog=VLog,
                vlog_last_cache=VLogLastCache,
                mrvc=MRVC,
                last_prep=0,
                commit_queue=CommitQueue,
                clog=CLog}}.

handle_call({clog, VCaggr, Dots}, _Sender, State=#state{clog=CLog}) ->
    MaxVC = pvc_commit_log:get_smaller_from_dots(Dots, VCaggr, CLog),
    {reply, {ok, MaxVC}, State};

handle_call({prepare, TxId, WriteSet}, _Sender, State=#state{partition=Partition,
                                                             commit_queue=CommitQueue,
                                                             last_prep=LastPrep}) ->

    Disputed = pvc_commit_queue:contains_disputed(WriteSet, CommitQueue),
    case Disputed of
        true ->
            {reply, {error, abort_conflict}, State};
        false ->
            SeqNumber = LastPrep + 1,
            NewCommitQueue = pvc_commit_queue:enqueue(TxId, WriteSet, CommitQueue),
            NewState = State#state{last_prep=SeqNumber, commit_queue=NewCommitQueue},
            {reply, {ok, Partition, SeqNumber}, NewState}
    end;

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

terminate(_Reason, #state{partition=Partition}) ->
    true = persistent_term:erase({vlog, Partition}),
    true = persistent_term:erase({vlog_last_cache, Partition}),
    true = persistent_term:erase({mrvc_cache, Partition}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Create an ETS table for a partition, and persist it
persistent_table({Name, Partition}=Key) ->
    TableName = get_cache_name(Partition, Name),
    ok = persistent_term:put(Key, TableName),
    open_table(TableName).

%% @doc Get an ETS name of the given partition
get_cache_name(Partition, Base) ->
    BinBase = atom_to_binary(Base, latin1),
    BinPart = integer_to_binary(Partition),
    Name = <<BinBase/binary, <<"-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

%% @doc Generate a name for this partition owner
-spec generate_partition_name(partition_id()) -> partition_server().
generate_partition_name(Partition) ->
    BinPart = integer_to_binary(Partition),
    Name = <<<<"owner-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

open_table(CacheName) ->
    open_table(CacheName, [set, protected, named_table, {read_concurrency, true}]).

open_table(CacheName, Options) ->
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            timer:sleep(100),
            try ets:delete(CacheName)
            catch _:_Reason -> ok
            end,
            open_table(CacheName, Options)
    end.

get_persistent_table(Key) ->
    case persistent_term:get(Key, undefined) of
        undefined ->
            {error, partition_not_started};
        Val ->
            {ok, Val}
    end.

-spec vlog_read(Partition :: partition_id(),
                Key :: key(),
                MaxVC :: vc()) -> {ok, val(), vc(), vc()} | abort().
vlog_read(Partition, Key, MaxVC) ->
    case get_persistent_table({vlog, Partition}) of
        {error, Reason} ->
            {error, Reason};
        {ok, VLog} ->
            case ets:lookup(VLog, Key) of
                [] ->
                    {ok, <<>>, pvc_vclock:new(), MaxVC};
                [{Key, KeyVersionLog}] ->
                    {Val, CommitVC} = pvc_version_log:get_smaller(MaxVC, KeyVersionLog),
                    {ok, Val, CommitVC, MaxVC}
              end
    end.

-spec clog_read(Partition :: partition_id(),
                Key :: key(),
                VCaggr :: vc(), read_partitions()) -> {ok, val(), vc(), vc()}
                                                    | abort().

clog_read(Partition, Key, VCaggr, HasRead) ->
    case mrvc_for(Partition) of
        {error, Reason} ->
            {error, Reason};
        {ok, MRVC} ->
            case check_time(Partition, MRVC, VCaggr) of
                not_ready ->
                    {error, try_again};
                ready ->
                    clog_read_internal(Partition, Key, VCaggr, HasRead)
            end
    end.

-spec mrvc_for(partition_id()) -> {ok, vc()} | {error, reason()}.
mrvc_for(Partition) ->
    case get_persistent_table({mrvc_cache, Partition}) of
        {error, Reason} ->
            {error, Reason};
        {ok, MRVC} ->
            {ok, ets:lookup_element(MRVC, mrvc, 2)}
    end.

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

-spec clog_read_internal(partition_id(), key(), vc(), read_partitions()) -> {ok, val(), vc(), vc()} | abort().
clog_read_internal(Partition, Key, VCaggr, HasRead) ->
    case find_maxvc(Partition, VCaggr, HasRead) of
        {error, Reason} ->
            {error, Reason};
        {ok, MaxVC} ->
            vlog_read(Partition, Key, MaxVC)
    end.

-spec find_maxvc(partition_id(), vc(), read_partitions()) -> {ok, vc()} | abort().
find_maxvc(Partition, VCaggr, HasRead) ->
    Res = case ordsets:size(HasRead) of
        0 ->
            mrvc_for(Partition);
        _ ->
            scan_clog(Partition, VCaggr, ordsets:to_list(HasRead))
    end,
    case Res of
        {error, Reason} ->
            {error, Reason};
        {ok, MaxVC} ->
            check_valid_max_vc(Partition, VCaggr, MaxVC)
    end.

-spec scan_clog(Partition :: partition_id(),
                VCaggr :: vc(),
                Dots :: [partition_id()]) -> {ok, vc()} | {error, reason()}.
scan_clog(Partition, VCaggr, Dots) ->
    try
        gen_server:call(generate_partition_name(Partition), {clog, VCaggr, Dots})
    catch _:_Reason ->
        {error, partition_not_started}
    end.

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

-spec is_ws_stale(partition_id(), ws(), non_neg_integer()) -> boolean().
is_ws_stale(Partition, WriteSet, PartitionVersion) ->
    case get_persistent_table({vlog_last_cache, Partition}) of
        {error, _} ->
            false;
        {ok, Cache} ->
            check_stale_ws(pvc_writeset:to_list(WriteSet), PartitionVersion, Cache)
    end.

check_stale_ws([], _, _) -> false;
check_stale_ws([{Key, _} | Rest], Version, Cache) ->
    StaleKey = case ets:lookup(Cache, Key) of
        [] ->
            false;
        [{Key, CommitTime}] ->
            CommitTime > Version
    end,
    case StaleKey of
        true ->
            true;
        false ->
            check_stale_ws(Rest, Version, Cache)
    end.

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
process_queue(State=#state{partition=Partition, vlog=VLog,
                           commit_queue=CommitQueue, vlog_last_cache=VLogCache}) ->

    {ReadyTx, NewQueue} = pvc_commit_queue:dequeue_ready(CommitQueue),
    case ReadyTx of
        [] ->
            State#state{commit_queue=NewQueue};
        Entries ->
            CLog = State#state.clog,
            OldMRVC = ets:lookup_element(State#state.mrvc, mrvc, 2),
            {NewCLog, NewMRVC} = lists:foldl(fun(Entry, {AccCLog, AccMRVC}) ->
                {_TxId, WriteSet, CommitVC, _} = Entry,
                ListWS = pvc_writeset:to_list(WriteSet),

                %% Update VLog
                ok = update_vlog(Partition, CommitVC, ListWS, VLog),
                %% Cache VLog.last(Key) \forall Key \in WS
                ok = cache_vlog_last(Partition, CommitVC, ListWS, VLogCache),

                %% Update MRVC and CLog
                %% We gate updating them until the end,
                %% so transactions see the updates until the end
                FoldCLog = pvc_commit_log:insert(CommitVC, AccCLog),
                FoldMRVC = pvc_vclock:max(CommitVC, AccMRVC),
                {FoldCLog, FoldMRVC}
            end, {CLog, OldMRVC}, Entries),

            %% Leave the MRVC updating until the end
            true = ets:update_element(State#state.mrvc, mrvc, {2, NewMRVC}),
            State#state{commit_queue=NewQueue, clog=NewCLog}
    end.

-spec update_vlog(partition_id(), vc(), [{key(), val()}], ets:tab()) -> ok.
update_vlog(Partition, CommitVC, ListWS, VLog) ->
    %% VLog.apply(Key, Val, CommitVC) \forall Key \in WS
    VersionLogs = [begin
        OldVLog = case ets:lookup(VLog, Key) of
            [] -> pvc_version_log:new(Partition);
            [{Key, Prev}] -> Prev
        end,
        {Key, pvc_version_log:insert(CommitVC, Value, OldVLog)}
    end || {Key, Value} <- ListWS],
    true = ets:insert(VLog, VersionLogs),
    ok.

-spec cache_vlog_last(partition_id(), vc(), [{key(), val()}], ets:tab()) -> ok.
cache_vlog_last(Partition, CommitVC, ListWS, VLogCache) ->
    CommitTime = pvc_vclock:get_time(Partition, CommitVC),
    true = ets:insert(VLogCache, [{Key, CommitTime} || {Key, _} <- ListWS]),
    ok.
