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

-module(pvc_version_log).
-include("pvc_model.hrl").

-define(bottom, {<<>>, pvc_vclock:new()}).

-type versions() :: orddict:dict(integer(), {non_neg_integer(), vc()}).

-record(vlog, {
    %% The partition where this structure resides
    at :: partition_id(),
    %% The actual version list
    %% The data is structured as an ordered dict where the value is a
    %% snapshot, and the key, the time of that snapshot at this partition.
    %%
    %% This snapshot time is stored as a negative number, as the default
    %% ordered dict implementation orders them in ascending order, and
    %% we want them in descending order.
    data :: versions()
}).

-type vlog() :: #vlog{}.

%% API
-export([new/1,
         insert/3,
         get_smaller/2]).

-spec new(partition_id()) -> vlog().
new(AtId) ->
    #vlog{at=AtId, data=orddict:new()}.

-spec insert(vc(), val(), vlog()) -> vlog().
insert(VC, Value, V=#vlog{at=Id, data=Dict}) ->
    Key = pvc_vclock:get_time(Id, VC),
    V#vlog{data=maybe_gc(orddict:store(-Key, {Value, VC}, Dict))}.

maybe_gc(Data) ->
    Size = orddict:size(Data),
    case Size > ?VERSION_THRESHOLD of
        false -> Data;
        true -> lists:sublist(Data, ?MAX_VERSIONS)
    end.

-spec get_smaller(vc(), vlog()) -> {val(), vc()}.
get_smaller(_VC, #vlog{data=[]}) ->
    ?bottom;

get_smaller(VC, #vlog{at=Id, data=[{MaxTime, MaxVersion} | _]=Data}) ->
    LookupKey = pvc_vclock:get_time(Id, VC),
    case LookupKey > abs(MaxTime) of
        true ->
            MaxVersion;
        false ->
            get_smaller_internal(-LookupKey, Data)
    end.

-spec get_smaller_internal(integer(), versions()) -> {val(), vc()}.
get_smaller_internal(_, []) ->
    ?bottom;

get_smaller_internal(LookupKey, [{Time, Version} | Rest]) ->
    case LookupKey =< Time of
        true ->
            Version;
        false ->
            get_smaller_internal(LookupKey, Rest)
    end.
