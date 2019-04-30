% This should be run on the server side
inter_dc_manager:start_bg_processes(stable).

{ok, Ring, Nodes} = pvc_ring:partition_info('127.0.0.1', 7878).
Conns = [{Ip, element(2,pvc_connection:new(Ip, 7878))} || Ip <- Nodes].

{ok, Client} = pvc:new(Ring, Conns, 0).

% Transaction a
{ok, Tx00} = pvc:start_transaction(Client, 0).
{ok, _, Tx01} = pvc:read(Client, Tx00, [4]).
{ok, Tx02} = pvc:update(Client, Tx01, [{4,0}]).
pvc:commit(Client, Tx02).

% Transaction b
{ok, Tx10} = pvc:start_transaction(Client, 1).
{ok, _, Tx11} = pvc:read(Client, Tx10, [0,4]).
{ok, Tx12} = pvc:update(Client, Tx11, [{0,0}, {4,0}]).

% Transaction c, will never commit
{ok, Tx20} = pvc:start_transaction(Client, 2).
{ok, _, Tx21} = pvc:read(Client, Tx20, [1]).
{ok, Tx22} = pvc:update(Client, Tx21, [{1,0}]).
_ = pvc:prepare(Client, Tx22).

% Transaction d
{ok, Tx30} = pvc:start_transaction(Client, 3).
{ok, _, Tx31} = pvc:read(Client, Tx30, [1,4]).
{ok, Tx32} = pvc:update(Client, Tx31, [{9,0}, {12,0}]).
pvc:commit(Client, Tx32).

% Transaction b commits here
pvc:commit(Client, Tx12).

% Transaction e
{ok, Tx40} = pvc:start_transaction(Client, 4).
pvc:read(Client, Tx40, [0,1,4]).
