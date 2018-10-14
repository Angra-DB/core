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
    GossipServer = #{id       => adb_gossip_server,
                     start    => {adb_gossip_server, start_link, []},
                     restart  => temporary,
                     shutdown => brutal_kill,
                     type     => worker,
                     modules  => [adb_gossip_server]},
    DistServer = #{id       => adb_dist_server, 
                   start    => {adb_dist_server, start_link, [Args]},
                   restart  => temporary, 
                   shutdown => brutal_kill, 
                   type     => worker, 
                   modules  => [adb_dist_server]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [GossipServer, DistServer]}}.