-module(adb_vnode_sup).
-behavior(supervisor).

%% API
-export([start_link/1, start_child/0]).

%% supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

start_child() ->
    supervisor:start_child(?SERVER, []).

%%=============================================================================
%% supervisor callbacks
%%=============================================================================

init(Args) ->
    {ok, Persistence} = setup_persistence(Args),
	 {ok, VNodes} = adb_dist_store:get_config(vnodes),
    PersistSups = [#{id       => adb_utils:get_vnode_process(persist_sup, Id, VNodes), 
						   start    => {persist_sup, start_link, [Persistence, Id]}, 
						   restart  => permanent, 
						   shutdown => infinity, 
						   type     => supervisor, 
						   modules  => [persist_sup]}, || Id <- lists:seq(1, VNodes)],
    VNodeServer = #{id       => adb_vnode_server,
                    start    => {adb_vnode_server, start_link, []},
                    restart  => permanent,
                    shutdown => brutal_kill,
                    type     => worker,
                    modules  => [adb_vnode_server]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [VNodeServer|Cores]}}.

%%==============================================================================
%% Internal functions
%%==============================================================================

setup_persistence(Args) ->
    lager:info("Setting up the persistence module.", []),
    case proplists:get_value(persistence, Args) of
	hanoidb -> lager:info("Starting HanoiDB..."),
               {ok, hanoidb_persistence};
    ets     -> lager:info("Starting ETS..."),
		       {ok, ets_persistence};
	_       -> lager:info("Starting ADBTree..."),
		       {ok, adbtree_persistence}
    end.
   