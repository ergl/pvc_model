Ring = pvc_model:start_partitions([0,1,2,3,4,5,6,7]).
Client = pvc_client:new(Ring).

% Tx a
Tx00 = pvc_client:start_transaction(tx_a).
{ok, _, Tx01} = pvc_client:read([4], Tx00, Client).
Tx02 = pvc_client:update([{4, 0}], Tx01, Client).
pvc_client:commit(Tx02).

% Tx b
Tx10 = pvc_client:start_transaction(tx_b).
{ok, _, Tx11} = pvc_client:read([0,4], Tx10, Client).
Tx12 = pvc_client:update([{0,0},{4,0}], Tx11, Client).

% Tx c, prepare and get stuck
Tx20 = pvc_client:start_transaction(tx_c).
{ok, _, Tx21} = pvc_client:read([1], Tx20, Client).
Tx22 = pvc_client:update([{1,0}], Tx21, Client).
_ = pvc_client:prepare(Tx22).

% Tx d runs entirely here
Tx30 = pvc_client:start_transaction(tx_d).
{ok, _, Tx31} = pvc_client:read([1, 4], Tx30, Client).
Tx32 = pvc_client:update([{9,0}, {12,0}], Tx31, Client).
pvc_client:commit(Tx32).

% Tx b commits here
ok = pvc_client:commit(Tx12).

% Tx e
Tx40 = pvc_client:start_transaction(tx_e).
{error, abort_read} = pvc_client:read([0,1,4], Tx40, Client).
