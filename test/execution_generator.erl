-module(execution_generator).

-include_lib("proper/include/proper.hrl").

-export_type([tx_history/0, execution/0, op/0]).
-export([history/0, execution/0]).

-type op() :: start_tx
           | {read, [non_neg_integer()]}
           | {update, [non_neg_integer()]}
           | prepare
           | commit.

-type tx_op() :: #{id => non_neg_integer(), op => op()}.
-type tx_history() :: [tx_op()].
-type execution() :: [tx_history()].

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%

history() ->
    ?SIZED(Size,
          ?LET(History, history(Size),
               unique([start_tx] ++ prune(History) ++ [prepare, commit]))).

execution() ->
    ?SIZED(Size,
           ?LET(Histories, vector(Size, history()),
                combine(Histories))).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% History Generators %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

history(Size) ->
    %% A valid sequence always starts with a read
    ?LET(KeyList, keylist(Size),
         history(Size, {read, KeyList, KeyList}, [])).

history(0, _, Acc) ->
    lists:reverse(Acc);

history(Max, State, Acc) ->
    ?LET(Next, next_state(State),
         history(Max - 1, Next, [State | Acc])).

next_state({read, _, KeyAcc}) ->
    oneof([next_read(KeyAcc), next_update(KeyAcc)]);

next_state({update, _, KeyAcc}) ->
    frequency([{2, next_read(KeyAcc)}, {1, next_update(KeyAcc)}]).

%% @doc A read will choose to read again a collection of old keys
next_read(KeyAcc) ->
    Len = length(KeyAcc),
    ?LET(Idx, ?SHRINK(range(1, Len), [range(1, ((Len div 4) + 1)), range(1, ((Len div 2) + 1))]),
        {read, pick(Idx, shuffle(KeyAcc)), KeyAcc}).

%% @doc An update must only read from keys already read
next_update(KeyAcc) ->
    Len = length(KeyAcc),
    ?LET(Idx, ?SHRINK(range(1, Len), [range(1, ((Len div 4) + 1)), range(1, ((Len div 2) + 1))]),
         {update, values(pick(Idx, shuffle(KeyAcc))), KeyAcc}).


%% @doc An unique list of keys
keylist(Size) ->
    ?LET(List, vector(Size, key()), lists:usort(List)).

values(Keys) ->
    [{Key, val()} || Key <- Keys].

key() -> pos_integer().
val() -> integer().

%%%%%%%%%%%%%%%%%%%%%%%
%%% History Helpers %%%
%%%%%%%%%%%%%%%%%%%%%%%

-spec shuffle(list(term())) -> list(term()).
shuffle(L) ->
    Shuffled = lists:sort([{rand:uniform(), X} || X <- L]),
    [X || {_, X} <- Shuffled].

%% @doc Take a prefix `Len` size from `L`
-spec pick(Len :: non_neg_integer(), L :: list(term())) -> list(term()).
pick(Len, L) ->
    lists:sublist(L, Len).

%% @doc Prepend all steps in the history with an unique identifier
%%
%%      A history is per-transaction. Since we're going to interleave
%%      them later, we need to distinguish between them.
-spec unique([op()]) -> tx_history().
unique(Ops) ->
    Unique = erlang:unique_integer([positive]),
    [#{op => Op, id => Unique} || Op <- Ops].

%% @doc Remove the key accumulator, since we don't need it in the
%%      final history
%%
%%      The key accumulator is there to guarantee a correct execution
prune(History) ->
    [{State, Args} || {State, Args, _Acc} <- History].

%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Execution Helpers %%%
%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Combine and interleave the given histories into an execution
-spec combine([tx_history()]) -> execution().
combine([A]) ->
    A;

combine(Histories) ->
    combine(shuffle(Histories), []).

-spec combine([tx_history()], execution()) -> execution().
combine([], Acc) ->
    lists:reverse(Acc);

%% At each step, we interlave a random chunk of the head history
%% the list of histories is shuffled between steps to vary the orders
combine([H|HS], Acc) ->
    L = length(H),
    ?LET(Length, range(1, L), begin
        {Prefix, Suffix} = lists:split(Length, H),
        NewAcc = lists:reverse(Prefix) ++ Acc,
        case Suffix of
            [] ->
                %% We've exhausted the head history,
                %% remove it from the list
                ?LAZY(combine(shuffle(HS), NewAcc));
            _ ->
                ?LAZY(combine(shuffle([Suffix | HS]), NewAcc))
        end
    end).
