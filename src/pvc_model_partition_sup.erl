-module(pvc_model_partition_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    Worker = #{id => pvc_model_partition_owner,
               start => {pvc_model_partition_owner, start_link, []},
               restart => transient,
               shutdown => 5000,
               type => worker,
               modeules => [pvc_model_partition_owner]},
    Strategy = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    {ok, {Strategy, [Worker]}}.
