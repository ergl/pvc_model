-module(pvc_model).

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

-spec start_partitions([non_neg_integer()]) -> [pvc_model_partition_owner:pref()].
start_partitions(PartitionList) ->
    lists:map(fun(P) ->
        {ok, _, PRef} = pvc_model_partition_owner:start_partition(P),
        PRef
    end, PartitionList).

-spec stop_partitions([pvc_model_partition_owner:pref()]) -> ok.
stop_partitions(PRefList) ->
    [pvc_model_partition_owner:stop_partition(Ref) || Ref <- PRefList],
    ok.

read(Partition, Key, VCaggr, HasRead) ->
    pvc_model_partition_owner:read(Partition, Key, VCaggr, HasRead).

prepare(Partition, TxId, WriteSet, PartitionVersion) ->
    pvc_model_partition_owner:prepare(Partition, TxId, WriteSet, PartitionVersion).

-spec decide(_, _, _) -> ok.
decide(Partition, TxId, Outcome) ->
    pvc_model_partition_owner:decide(Partition, TxId, Outcome).


-spec partition_for_key([pvc_model_partition_owner:pref()], term()) -> pvc_model_partition_owner:pref().
partition_for_key(PartitionList, Key) ->
    Conv = convert_key(Key),
    Pos = Conv rem length(PartitionList) + 1,
    lists:nth(Pos, PartitionList).

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
