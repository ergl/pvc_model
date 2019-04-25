-module(pvc_model_partition_owner).

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

-type pref() :: atom().
-type reason() :: partition_not_started
                | abort_read
                | abort_stale
                | abort_conflict
                | try_again.

-record(state, {
    partition :: non_neg_integer(),
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

-spec start_partition(non_neg_integer()) -> {ok, pid(), pref()} | {error, term()}.
start_partition(Partition) ->
    Name = generate_partition_name(Partition),
    Ret = supervisor:start_child(pvc_model_partition_sup, [Name, Partition]),
    case Ret of
        {ok, Pid} ->
            {ok, Pid, Name};
        Error ->
            Error
    end.

-spec stop_partition(pref()) -> ok.
stop_partition(PRef) ->
    gen_server:call(PRef, stop, infinity).

-spec read(
    non_neg_integer(),
    term(),
    pvc_vclock:vc(),
    ordsets:ordset(non_neg_integer())
) -> {ok, term(), pvc_vclock:vc(), pvc_vclock:vc()} | {error, reason()}.
read(Partition, Key, VCaggr, HasRead) ->
    case ordsets:is_element(Partition, HasRead) of
        true ->
            %% Read from VLog directly, using VCaggr as MaxVC
            vlog_read(Partition, Key, VCaggr);
        false ->
            clog_read(Partition, Key, VCaggr, HasRead)
    end.

%% TODO(borja)
prepare(_Partition, _TxId, _WriteSet, _PartitionVersion) ->
    erlang:error(unimplemented).

%% TODO(borja)
decide(_Partition, _TxId, _Outcome) ->
    erlang:error(unimplemented).

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

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(E, _From, S) ->
    io:format("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

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
-spec generate_partition_name(non_neg_integer()) -> atom().
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

-spec vlog_read(Partition :: non_neg_integer(),
                Key :: term(),
                MaxVC :: pvc_vclock:vc()) -> {ok, term(), pvc_vclock:vc(), pvc_vclock:vc()}
                                           | {error, reason()}.
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

-spec clog_read(Partition :: non_neg_integer(),
                Key :: term(),
                VCaggr :: pvc_vclock:vc(), ordsets:ordset()) -> {ok, term(), pvc_vclock:vc(), pvc_vclock:vc()}
                                                             | {error, reason()}.

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

mrvc_for(Partition) ->
    case get_persistent_table({mrvc_cache, Partition}) of
        {error, Reason} ->
            {error, Reason};
        {ok, MRVC} ->
            {ok, ets:lookup_element(MRVC, mrvc, 2)}
    end.

check_time(Partition, MostRecentVC, VCaggr) ->
    MostRecentTime = pvc_vclock:get_time(Partition, MostRecentVC),
    AggregateTime = pvc_vclock:get_time(Partition, VCaggr),
    case MostRecentTime < AggregateTime of
        true ->
            not_ready;
        false ->
            ready
    end.

clog_read_internal(Partition, Key, VCaggr, HasRead) ->
    case find_maxvc(Partition, VCaggr, HasRead) of
        {error, Reason} ->
            {error, Reason};
        {ok, MaxVC} ->
            vlog_read(Partition, Key, MaxVC)
    end.

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

-spec scan_clog(Partition :: non_neg_integer(),
                VCaggr :: pvc_vclock:vc(),
                Dots :: [non_neg_integer()]) -> {ok, pvc_vclock:vc()} | {error, reason()}.
scan_clog(Partition, VCaggr, Dots) ->
    try
        gen_server:call(generate_partition_name(Partition), {clog, VCaggr, Dots})
    catch _:_Reason ->
        {error, partition_not_started}
    end.

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
