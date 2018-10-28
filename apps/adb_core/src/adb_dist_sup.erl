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
    {DistConfig, OtherSpecs} = case init_server_or_acknowledge(Args) of
        {ok, ack}                  -> {Args, []};
        {ok, {Config, ServerSpec}} -> {lists:keyreplace(server, 1, Config, {server, node()}), [ServerSpec]}
    end,
    GossipServer = #{id       => adb_gossip_server,
                     start    => {adb_gossip_server, start_link, []},
                     restart  => temporary,
                     shutdown => brutal_kill,
                     type     => worker,
                     modules  => [adb_gossip_server]},
    DistStore = #{id       => adb_dist_store,
                  start    => {adb_dist_store, start_link, [DistConfig]},
                  restart  => temporary,
                  shutdown => brutal_kill,
                  type     => worker,
                  modules  => [adb_dist_store]},
    DistServer = #{id       => adb_dist_server, 
                   start    => {adb_dist_server, start_link, []},
                   restart  => temporary, 
                   shutdown => brutal_kill, 
                   type     => worker, 
                   modules  => [adb_dist_server]},
    Specs = lists:append([GossipServer, DistStore, DistServer], OtherSpecs),
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, Specs}}.

%%==============================================================================
%% Internal functions
%%==============================================================================

init_server_or_acknowledge(Config) ->
    case proplists:get_value(server, Config) of
        none   -> ServerSpec = get_server_spec(),
                  lager:info("Server initilized on this node: ~p. ~n", [node()]),
                  {ok, {Config, ServerSpec}};
        Remote -> lager:info("Remote server acknowledged: ~p. ~n", [Remote]),
                  {ok, ack}
    end.

get_server_spec() ->
    {ok, P} = adb_utils:get_env("ADB_PORT"),
    Port = if 
        is_integer(P) -> P;
        true          -> {Num, []} = string:to_integer(P),
                         Num
    end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),
    lager:info("Listening to TCP requests on port ~w.", [Port]),
    #{id       => adb_server_sup,
      start    => {adb_server_sup, start_link, [LSock]},
      restart  => permanent,
      shutdown => infinity,
      type     => supervisor,
      modules  => [adb_server_sup]}.