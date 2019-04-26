-module(pvc_client).
-include("pvc_model.hrl").

%% API
-export([new/1,
         start_transaction/1,
         read/3,
         update/3,
         update/4,
         prepare/1,
         decide/2,
         commit/1]).

-type tx_ws() :: orddict:orddict(partition_id(), ws()).

-record(tx_state, {
    id :: tx_id(),
    read_only = true :: boolean(),
    writeset = orddict:new() :: tx_ws(),
    vc_dep = pvc_vclock:new() :: vc(),
    vc_aggr = pvc_vclock:new() :: vc(),
    read_partitions = ordsets:new():: read_partitions()
}).

-record(client_state, {
   ring :: ring()
}).

-type tx() :: #tx_state{}.
-type client_state() :: #client_state{}.
-type prepare_result() :: readonly | {ok, vc()} | abort().

%%====================================================================
%% API functions
%%====================================================================

-spec new(ring()) -> client_state().
new(Ring) -> #client_state{ring=Ring}.

-spec start_transaction(tx_id()) -> tx().
start_transaction(TxId) -> #tx_state{id=TxId}.

-spec read(key() | [key()], tx(), client_state()) -> {ok, val() | [val()], tx()} | abort().
read(Keys, Tx, State) when is_list(Keys) ->
    read_batch(Keys, Tx, State, []);

read(Key, Tx, State) ->
    read_internal(Key, Tx, State).

-spec update(key(), val(), tx(), client_state()) -> tx().
update(Key, Value, Tx, #client_state{ring=Ring}) ->
    NewWS = update_internal(Key, Value, Ring, Tx#tx_state.writeset),
    Tx#tx_state{read_only=false, writeset=NewWS}.

-spec update([{key(), val()}], tx(), client_state()) -> tx().
update(Updates, Tx, #client_state{ring=Ring}) when is_list(Updates) ->
    NewWs = lists:foldl(fun({Key, Value}, AccWS) ->
        update_internal(Key, Value, Ring, AccWS)
    end, Tx#tx_state.writeset, Updates),
    Tx#tx_state{read_only=false, writeset=NewWs}.

-spec commit(tx()) -> ok | abort().
commit(Tx) ->
    decide(prepare(Tx), Tx).

-spec prepare(tx()) -> prepare_result().
prepare(#tx_state{read_only=true}) ->
    readonly;

prepare(#tx_state{id=TxId, vc_dep=CommitVC, writeset=WriteSet}) ->
    orddict:fold(fun(PartitionId, WS, Acc) ->
        Version = pvc_vclock:get_time(PartitionId, CommitVC),
        Vote = pvc_model:prepare(PartitionId, TxId, WS, Version),
        update_vote(Vote, Acc)
    end, {ok, CommitVC}, WriteSet).

-spec decide(prepare_result(), tx()) -> ok | abort().
decide(readonly, _) ->
    ok;

decide(Vote, Tx) ->
    case Vote of
        {error, Reason} ->
            ok = decide_internal(abort, Tx),
            {error, Reason};
        {ok, _}=CommitVC ->
            ok = decide_internal(CommitVC, Tx),
            ok
    end.

%%====================================================================
%% Read Internal functions
%%====================================================================

-spec read_batch([key()], tx(), client_state(), [val()]) -> {ok, [val()], tx()} | abort().
read_batch([], AccTx, _State, ReadAcc) ->
    {ok, lists:reverse(ReadAcc), AccTx};

read_batch([Key | Keys], AccTx, State, ReadAcc) ->
    case read_internal(Key, AccTx, State) of
        {error, Reason} ->
            {error, Reason};

        {ok, Value, NewTx} ->
            read_batch(Keys, NewTx, State, [Value | ReadAcc])
    end.

-spec read_internal(key(), tx(), client_state()) -> {ok, val(), tx()} | abort().
read_internal(Key, Tx, #client_state{ring=Ring}) ->
    case key_updated(Key, Ring, Tx#tx_state.writeset) of
        {ok, Value} ->
            {ok, Value, Tx};
        {false, Partition, _} ->
            remote_read(Partition, Key, Tx)
    end.

-spec remote_read(partition_id(), key(), tx()) -> abort() | {ok, val(), tx()}.
remote_read(Partition, Key, Tx=#tx_state{vc_aggr=VCaggr,
    read_partitions=HasRead}) ->

    case pvc_model:read(Partition, Key, VCaggr, HasRead) of
        {error, Reason} ->
            {error, Reason};
        {ok, Value, VersionVC, MaxVC} ->
            NewTx = update_tx(Partition, VersionVC, MaxVC, Tx),
            {ok, Value, NewTx}
    end.

-spec update_tx(partition_id(), vc(), vc(), tx()) -> tx().
update_tx(Partition, VersionVC, MaxVC, Tx) ->
    #tx_state{vc_dep=VCdep, vc_aggr=VCaggr, read_partitions=HasRead} = Tx,
    Tx#tx_state{vc_dep = pvc_vclock:max(VersionVC, VCdep),
                vc_aggr = pvc_vclock:max(MaxVC, VCaggr),
                read_partitions = ordsets:add_element(Partition, HasRead)}.

%%====================================================================
%% Update Internal functions
%%====================================================================

-spec key_updated(key(), ring(), tx_ws()) -> {ok, val()} | {false, partition_id(), partition_server()}.
key_updated(Key, Ring, WS) ->
    {Partition, Owner} = pvc_model:partition_for_key(Key, Ring),
    PartitionWS = get_partition_ws(Partition, WS),
    case pvc_writeset:get(Key, PartitionWS) of
        error ->
            {false, Partition, Owner};
        {ok, Value} ->
            {ok, Value}
    end.

-spec get_partition_ws(partition_id(), tx_ws()) -> ws().
get_partition_ws(Partition, WS) ->
    case orddict:find(Partition, WS) of
        error ->
            pvc_writeset:new();
        {ok, PWS} ->
            PWS
    end.

-spec update_internal(key(), val(), ring(), tx_ws()) -> tx_ws().
update_internal(Key, Value, Ring, TxWriteSet) ->
    {Partition, _} = pvc_model:partition_for_key(Key, Ring),
    PWS = pvc_writeset:put(Key, Value, get_partition_ws(Partition, TxWriteSet)),
    orddict:store(Partition, PWS, TxWriteSet).


%%====================================================================
%% Commit Internal functions
%%====================================================================

-spec decide_internal(outcome(), tx()) -> ok.
decide_internal(Outcome, #tx_state{id=TxId, writeset=WS}) ->
    [pvc_model:decide(Partition, TxId, Outcome) || {Partition, _} <- WS],
    ok.

-spec update_vote(vote(), {ok, vc()} | abort()) -> {ok, vc()} | abort().
update_vote(_, {error, _}=Acc) -> Acc;
update_vote({error, _}=Vote, _) -> Vote;
update_vote({ok, Partition, Seq}, {ok, CommitVC}) ->
    {ok, pvc_vclock:set_time(Partition, Seq, CommitVC)}.