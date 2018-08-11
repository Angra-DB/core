-module(adb_dist_sup).
-behavior(supervisor).

%% API
-export([start_link/1, start_child/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

start_child() ->
    supervisor:start_child(?SERVER, []).

start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

init(Args) ->
    Dist = #{id       => adb_dist, 
             start    => {adb_dist, start_link, [Args]},
             restart  => temporary, 
             shutdown => brutal_kill, 
             type     => worker, 
             modules  => [adb_dist]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [Dist]}}.