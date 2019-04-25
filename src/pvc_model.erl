-module(pvc_model).
-include("pvc_model.hrl").

%% App API
-export([start/0,
         stop/0]).

%% Boot API
-export([start_partitions/1,
         stop_partitions/1]).

%% Transactional API
-export([read/4,
         prepare/4,
         decide/3]).

%% Util API
-export([partition_for_key/2]).

start() ->
    application:ensure_all_started(pvc_model).

stop() ->
    application:stop(pvc_model).

-spec start_partitions([partition_id()]) -> [pref()].
start_partitions(PartitionList) ->
    lists:map(fun(P) ->
        {ok, _, PRef} = pvc_model_partition_owner:start_partition(P),
        {PRef, P}
    end, PartitionList).

-spec stop_partitions([pref()]) -> ok.
stop_partitions(PRefs) ->
    [pvc_model_partition_owner:stop_partition(Ref) || {Ref, _} <- PRefs],
    ok.

-spec read(partition_id(), key(), vc(), read_partitions()) -> {ok, val(), vc(), vc()}
                                                            | abort().
read(Partition, Key, VCaggr, HasRead) ->
    pvc_model_partition_owner:read(Partition, Key, VCaggr, HasRead).

-spec prepare(partition_id(),
              tx_id(),
              ws(),
              non_neg_integer()) -> {ok, partition_id(), non_neg_integer()}
                                 | abort().

prepare(Partition, TxId, WriteSet, PartitionVersion) ->
    pvc_model_partition_owner:prepare(Partition, TxId, WriteSet, PartitionVersion).

-spec decide(partition_id(), tx_id(), outcome()) -> ok.
decide(Partition, TxId, Outcome) ->
    pvc_model_partition_owner:decide(Partition, TxId, Outcome).

-spec partition_for_key([pref()], key()) -> partition_id().
partition_for_key(PartitionList, Key) ->
    Conv = convert_key(Key),
    Pos = Conv rem length(PartitionList) + 1,
    element(2, lists:nth(Pos, PartitionList)).

-spec convert_key(term()) -> non_neg_integer().
convert_key(Key) when is_binary(Key) ->
    try
        abs(binary_to_integer(Key))
    catch _:_ ->
        %% Looked into the internals of riak_core for this
        HashedKey = crypto:hash(sha, term_to_binary({<<"antidote">>, Key})),
        abs(crypto:bytes_to_integer(HashedKey))
    end;

convert_key(Key) when is_integer(Key) ->
    abs(Key);

convert_key(TermKey) ->
    %% Add bucket information
    BinaryTerm = term_to_binary({<<"antidote">>, term_to_binary(TermKey)}),
    HashedKey = crypto:hash(sha, BinaryTerm),
    abs(crypto:bytes_to_integer(HashedKey)).
