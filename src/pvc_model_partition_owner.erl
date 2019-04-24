-module(pvc_model_partition_owner).

-behaviour(gen_server).

%% API
-export([start_link/2,
         start_partition/1,
         stop_partition/1]).

%% Transactional API
-export([async_read/5,
         sync_read/4,
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

%% @doc Get the ETS name of the given partition
get_cache_name(Partition, Base) ->
    BinBase = atom_to_binary(Base, latin1),
    BinPart = integer_to_binary(Partition),
    Name = <<BinBase/binary, <<"-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

%% TODO(borja)
async_read(_ReplyTo, _Partition, _Key, _VCaggr, _HasRead) ->
    erlang:error(unimplemented).

%% TODO(borja)
sync_read(_Partition, _Key, _VCaggr, _HasRead) ->
    erlang:error(unimplemented).

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
    VLog = open_table(Partition, vlog),
    VLogLastCache = open_table(Partition, vlog_last_cache),

    %% Init MRVC table
    MRVC = open_table(Partition, mrvc_cache),
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

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec generate_partition_name(non_neg_integer()) -> atom().
generate_partition_name(Partition) ->
    BinPart = integer_to_binary(Partition),
    Name = <<<<"owner-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

open_table(Partition, Name) ->
    open_table(Partition, Name, [set, protected, named_table, {read_concurrency, true}]).

open_table(Partition, Name, Options) ->
    CacheName = get_cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            timer:sleep(100),
            try ets:delete(CacheName)
            catch _:_Reason -> ok
            end,
            open_table(Partition, Name, Options)
    end.