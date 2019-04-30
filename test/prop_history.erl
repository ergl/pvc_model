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

no_read_aborts([]) ->
    true;

no_read_aborts([{_, _, Error} | Rest]) ->
    case Error of
        {error, abort_read} ->
            false;
        _ ->
            no_read_aborts(Rest)
    end.

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

