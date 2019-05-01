rr("src/pvc_model_partition_owner.erl").
rr("src/pvc_commit_log.erl").
rr("src/pvc_commit_queue.erl").
rr("src/pvc_version_log.erl").
rr("src/pvc_client.erl").

Ring = pvc_model:start_partitions([0,1,2]).
Client = pvc_client:new(Ring).

% Tx A
TxA0 = pvc_client:start_transaction(tx_a).
{ok, _, TxA1} = pvc_client:read([0,1], TxA0, Client).
TxA2 = pvc_client:update([{0,0},{1,0}], TxA1, Client).

% Tx B, prepare and get stuck
TxB0 = pvc_client:start_transaction(tx_b).
{ok, _, TxB1} = pvc_client:read([2], TxB0, Client).
TxB2 = pvc_client:update([{2,0}], TxB1, Client).
_ = pvc_client:prepare(TxB2).

% Tx C runs to completion
TxC0 = pvc_client:start_transaction(tx_c).
{ok, _, TxC1} = pvc_client:read([1,2], TxC0, Client).
TxC2 = pvc_client:update([{4,0}, {5,0}], TxC1, Client).
pvc_client:commit(TxC2).

% Tx A commits here
ok = pvc_client:commit(TxA2).

% Tx D
TxD0 = pvc_client:start_transaction(tx_d).
{error, abort_read} = pvc_client:read([0,2,1], TxD0, Client).
