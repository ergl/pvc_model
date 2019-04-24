%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(pvc_commit_queue).

-record(cqueue, {
    %% The main commit TxId queue
    q :: queue:queue(txid()),
    %% Mapping between txids and their write sets
    write_sets :: dict:dict(txid(), writeset()),
    %% For the ready tx, put their ids with their commit VC
    %% and their index key list here
    ready_tx :: dict:dict(txid(), {pvc_vc(), list()}),
    %% A set of txids that have been discarded
    discarded_tx :: sets:set(txid())
}).

-type pvc_vc() :: pvc_vclock:vc(non_neg_integer()).
-type txid() :: non_neg_integer().

-opaque cqueue() :: #cqueue{}.

-type writeset() :: pvc_writeset:ws(term(), term()).

-export_type([cqueue/0]).

-export([new/0,
    enqueue/3,
    dequeue_ready/1,
    ready/4,
    remove/2,
    contains_disputed/2]).

-spec contains_disputed(writeset(), cqueue()) -> boolean().
contains_disputed(WS, #cqueue{write_sets = WSDict}) ->
    is_ws_disputed(dict:to_list(WSDict), WS).

-spec new() -> cqueue().
new() ->
    #cqueue{q = queue:new(),
        ready_tx = dict:new(),
        write_sets = dict:new(),
        discarded_tx = sets:new()}.

-spec enqueue(txid(), writeset(), cqueue()) -> cqueue().
enqueue(TxId, WS, CQueue = #cqueue{q = Queue, write_sets = WSDict}) ->
    CQueue#cqueue{q = queue:in(TxId, Queue),
        write_sets = dict:store(TxId, WS, WSDict)}.

-spec ready(txid(), list(), pvc_vc(), cqueue()) -> cqueue().
ready(TxId, IndexList, VC, CQueue = #cqueue{
    q = Queue,
    ready_tx = ReadyDict
}) ->
    case queue:member(TxId, Queue) of
        false ->
            CQueue;
        true ->
            CQueue#cqueue{ready_tx = dict:store(TxId, {VC, IndexList}, ReadyDict)}
    end.

-spec remove(txid(), cqueue()) -> cqueue().
remove(TxId, CQueue = #cqueue{
    q = Queue,
    write_sets = WSDict,
    discarded_tx = DiscardedSet
}) ->
    case queue:member(TxId, Queue) of
        false ->
            CQueue;
        true ->
            CQueue#cqueue{write_sets = dict:erase(TxId, WSDict),
                discarded_tx = sets:add_element(TxId, DiscardedSet)}
    end.

-spec dequeue_ready(cqueue()) -> {[{txid(), writeset(), pvc_vc(), list()}], cqueue()}.
dequeue_ready(#cqueue{q = Queue,
    write_sets = WSDict,
    ready_tx = ReadyDict,
    discarded_tx = DiscardedSet}) ->

    {Acc, NewCQueue} = get_ready(queue:out(Queue), WSDict, ReadyDict, DiscardedSet, []),
    {lists:reverse(Acc), NewCQueue}.

get_ready({empty, Queue}, WSDict, ReadyDict, DiscardedSet, Acc) ->
    {Acc, from(Queue, WSDict, ReadyDict, DiscardedSet)};

get_ready({{value, TxId}, Queue}, WSDict, ReadyDict, DiscardedSet, Acc) ->
    case sets:is_element(TxId, DiscardedSet) of
        true ->
            DeleteDiscard = sets:del_element(TxId, DiscardedSet),
            get_ready(queue:out(Queue), WSDict, ReadyDict, DeleteDiscard, Acc);

        false ->
            case dict:is_key(TxId, ReadyDict) of
                false ->
                    {Acc, from(queue:in_r(TxId, Queue), WSDict, ReadyDict, DiscardedSet)};

                true ->
                    %% Get WS and remove it
                    WS = dict:fetch(TxId, WSDict),
                    NewWSDict = dict:erase(TxId, WSDict),

                    %% Get VC and remove it
                    {VC, IndexList} = dict:fetch(TxId, ReadyDict),
                    NewReadyDict = dict:erase(TxId, ReadyDict),

                    %% Append entry to the Acc
                    NewAcc = [{TxId, WS, VC, IndexList} | Acc],
                    get_ready(queue:out(Queue), NewWSDict, NewReadyDict, DiscardedSet, NewAcc)
            end
    end.

%% Internal

from(Queue, WSDIct, ReadyDict, DiscardedDict) ->
    #cqueue{q = Queue,
        write_sets = WSDIct,
        ready_tx = ReadyDict,
        discarded_tx = DiscardedDict}.

-spec is_ws_disputed([{txid(), writeset()}], writeset()) -> boolean().
is_ws_disputed([], _) ->
    false;

is_ws_disputed(_, []) ->
    false;

is_ws_disputed([{_TxId, OtherWS} | Rest], WS) ->
    case pvc_writeset:intersect(OtherWS, WS) of
        true ->
            true;
        false ->
            is_ws_disputed(Rest, WS)
    end.
