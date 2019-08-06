-module(prop_history).
-include_lib("proper/include/proper.hrl").
-compile([export_all]).

-define(PARTITIONS, [0,1,2,3,4,5,6,7]).

prop_single_commit() ->
    ?SETUP(fun setup/0,
    ?FORALL(History, execution_generator:history(),
        begin
            {Client, Ring} = start_case(),
            Result = history_executor:execute_single_history(History, Client),
            stop_case(Ring),
            ?WHENFAIL(io:format("History = ~p.~n~n %%Result: ~p~n", [History, Result]),
                Result =:= ok)
        end)).

prop_no_read_aborts() ->
    ?SETUP(fun setup/0,
        ?FORALL(Execution, execution_generator:simple_execution(5),
                begin
                    {Client, Ring} = start_case(),
                    Result = history_executor:materialize_execution(Execution, Client),
                    stop_case(Ring),
                    ?WHENFAIL(io:format("History = ~p.~n~n %% Result: ~p~n", [Execution, Result]),
                              no_read_aborts(Result))
                end)).

prop_no_ready_tx() ->
    ?SETUP(fun setup/0,
        ?FORALL(Execution, execution_generator:simple_execution(1),
                begin
                    {Client, Ring} = start_case(),
                    _ = history_executor:materialize_execution(Execution, Client),
                    Queues = commit_queues(),
                    timer:sleep(1000),
                    stop_case(Ring),
                    ?WHENFAIL(io:format("History = ~p.~n~n. %% Queues: ~p~n", [Execution, Queues]),
                              commit_queues_empty(Queues))
                end)).

no_read_aborts([]) ->
    true;

no_read_aborts([{_, _, Error} | Rest]) ->
    case Error of
        {error, abort_read} ->
            false;
        _ ->
            no_read_aborts(Rest)
    end.

commit_queues() ->
    lists:map(fun(P) ->
        State = pvc_model_partition_owner:debug_get_state(P),
        %% FIXME(borja): Export state so we can match on it
        {state, P, _Vlog, _VlogLast, _MRVC, _LastPrep, CQueue, _Clog} = State,
        {P, CQueue}
    end, ?PARTITIONS).


commit_queues_empty(Queues) ->
    lists:all(fun({_, Queue}) ->
        {Ready, _} = pvc_commit_queue:dequeue_ready(Queue),
        Ready =:= []
    end, Queues).

setup() ->
    {ok, _} = pvc_model:start(),
    ok,
    fun() -> teardown() end.

teardown() ->
    ok = pvc_model:stop().

start_case() ->
    Ring = pvc_model:start_partitions(?PARTITIONS),
    Client = pvc_client:new(Ring),
    {Client, Ring}.

stop_case(Ring) ->
    pvc_model:stop_partitions(Ring).

