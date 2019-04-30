-module(history_executor).
-include("pvc_model.hrl").

%% API
-export([execute_single_history/2,
         materialize_execution/2]).

-record(tx_execution, {
    id :: tx_id(),
    tx = undefined :: pvc_client:tx() | undefined
}).

-record(tx_state, {
    tx :: pvc_client:tx(),
    %% Any persistent tx-local state
    %% that we might need from step to step
    acc :: term(),
    %% Operations that are queued until later,
    %% in case one read blocks, we need to accumulate
    %% here.
    %% The execution will pick up from here until
    %% another transaction commits and unblocks this
    %% transaction
    op_queue = queue:new() :: queue:queue(execution_generator:tx_op())
}).
-type tx_state() :: #tx_state{}.

-record(state, {
    client :: pvc_client:client_state(),
    executions = maps:new() :: #{tx_id() => tx_state()}
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

-spec materialize_execution(execution_generator:execution(), pvc_client:client_state()) -> ok | abort().
materialize_execution(Execution, Client) ->
    catch materialize_execution_int(Execution,  #state{client=Client}).

result_for_state(#state{executions=Histories}) ->
    lists:filtermap(fun({Id, #tx_state{acc=Res}}) ->
        case Res of
            {Op, {error, Reason}} ->
                {true, {Id, Op, {error, Reason}}};
            _ -> false
        end
    end, maps:to_list(Histories)).

materialize_execution_int([], State) ->
    result_for_state(State);

materialize_execution_int([#{id := Id, op := Op} | Rest], State) ->
    materialize_execution_int(Rest, execute_for_id(Id, Op, State)).

execute_for_id(Id, Op, State=#state{executions=Map}) ->
    case maps:get(Id, Map, undefined) of
        undefined ->
            case Op of
                start_tx ->
                    Tx = pvc_client:start_transaction(Id),
                    TxHist = #tx_state{tx = Tx},
                    State#state{executions=maps:put(Id, TxHist, Map)};
                _ ->
                    throw([{Id, Op, {error, bad_tx}}])
            end;
        History ->
            hist_for_id(Id, Op, History, State)
    end.

hist_for_id(Id, start_tx, _History, _State) ->
    throw([{Id, start_tx, {error, bad_tx}}]);

hist_for_id(Id, {read, Keys}, History, State = #state{client=Client, executions=Map}) ->
    Res = pvc_client:read(Keys, History#tx_state.tx, Client),
    case Res of
        {error, Reason} ->
            %% TODO(borja): Check read block
            throw([{Id, {read, Keys}, {error, Reason}}]);
        {ok, _, NewTx} ->
            NewHistory = History#tx_state{tx=NewTx},
            State#state{executions=maps:put(Id, NewHistory, Map)}
    end;

hist_for_id(Id, {update, Updates}, History, State = #state{client=Client, executions=Map}) ->
    NewTx = pvc_client:update(Updates, History#tx_state.tx, Client),
    NewHistory = History#tx_state{tx=NewTx},
    State#state{executions=maps:put(Id, NewHistory, Map)};

hist_for_id(Id, prepare, History, State = #state{executions=Map}) ->
    Res = pvc_client:prepare(History#tx_state.tx),
    NewHistory = History#tx_state{acc=Res},
    State#state{executions=maps:put(Id, NewHistory, Map)};

hist_for_id(Id, commit, H = #tx_state{tx=Tx, acc=PrevAcc}, State = #state{executions=Map}) ->
    Res = pvc_client:decide(PrevAcc, Tx),
    RealRes = case Res of
        {error, Reason} ->
            {commit, {error, Reason}};
        ok ->
            ok
    end,
    State#state{executions=maps:put(Id, H#tx_state{acc=RealRes}, Map)}.

