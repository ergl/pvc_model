-module(history_executor).
-include("pvc_model.hrl").

%% API
-export([execute_single_history/2]).

%%-record(state, {}).
-record(tx_execution, {
    id :: tx_id(),
    tx = undefined :: pvc_client:tx() | undefined,
    %% Operations that are queued until later,
    %% in case one read blocks, we need to accumulate
    %% here.
    %% The execution will pick up from here until
    %% another transaction commits and unblocks this
    %% transaction
    op_queue = queue:new() :: queue:queue(execution_generator:tx_op())
}).

-spec execute_single_history(execution_generator:tx_history(), pvc_client:client_state()) -> ok | abort().
execute_single_history([#{id := TxId} | _]=History, Client) ->
    hist(History, ok, Client, #tx_execution{id=TxId}).

hist([], Result, _, _) ->
    Result;

hist(_, {error, Reason}, _, _) ->
    {error, Reason};

hist([#{id := Id, op := start_tx} | Rest], ok, Client, S=#tx_execution{id=Id, tx=undefined}) ->
    Tx = pvc_client:start_transaction(Id),
%%    io:format("start_tx := ~p~n", [Tx]),
    hist(Rest, ok, Client, S#tx_execution{tx=Tx});

hist([#{id := Id, op := {read, Keys}} | Rest], ok, Client, S=#tx_execution{id=Id, tx=Tx}) ->
    Res = pvc_client:read(Keys, Tx, Client),
%%    io:format("{read, ~p} := ~p~n", [Keys, Res]),
    case Res of
        {error, Reason} ->
            {error, Reason};
        {ok, _, NewTx} ->
            hist(Rest, ok, Client, S#tx_execution{tx=NewTx})
    end;

hist([#{id := Id, op := {update, Updates}} | Rest], ok, Client, S=#tx_execution{id=Id, tx=Tx}) ->
    NewTx = pvc_client:update(Updates, Tx, Client),
%%    io:format("{update, ~p} := ~p~n", [Updates, NewTx]),
    hist(Rest, ok, Client, S#tx_execution{tx=NewTx});


hist([#{id := Id, op := prepare} | Rest], ok, Client, S=#tx_execution{id=Id, tx=Tx}) ->
    Res = pvc_client:prepare(Tx),
%%    io:format("prepare := ~p~n", [Res]),
    hist(Rest, Res, Client, S);

hist([#{id := Id, op := commit} | _], Outcome, _, #tx_execution{id=Id, tx=Tx}) ->
    pvc_client:decide(Outcome, Tx).
