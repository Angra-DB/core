-module(adb_dist_sup).
-behaviour(supervisor).

%% API
-export([start_link/1, start_child/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Config) ->
    supervisor:start({local, ?SERVER}, ?MODULE, Config).

start_child() ->
    supervisor:start_child(?SERVER, []).

init(Args) ->
    Dist = {adb_dist_server, {adb_dist, start_link, Args},
            temporary, brutal_kill, worker, [adb_dist]},
    RestartStrategy = {simple_one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [Dist]}}.