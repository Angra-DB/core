-module(adb_vnode_sup).
-behavior(supervisor).

%% API
-export([start_link/1, start_child/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

start_child() ->
    supervisor:start_child(?SERVER, []).

init(Args) ->
    Persistence = setup_persistence(Args),
    VNodes = adb_dist_server:get_config(vnodes),
    CoreNames = [list_to_atom(atom_to_list(adb_persistence_) ++ integer_to_list(X)) || X <- lists:seq(1, VNodes)],
    Cores = [#{id       => Name, 
               start    => {adb_persistence, start_link, [Persistence, Name]},
               restart  => permanent, 
               shutdown => brutal_kill, 
               type     => worker, 
               modules  => [adb_persistence]} || Name <- CoreNames],
    VNodeServer = #{id       => adb_vnode_server,
                    start    => {adb_vnode_server, start_link, []},
                    restart  => permanent,
                    shutdown => brutal_kill,
                    type     => worker,
                    modules  => [adb_vnode_server]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [VNodeServer|Cores]}}.

setup_persistence(Args) ->
    lager:info("Setting up the persistence module.", []),
    case proplists:get_value(persistence, Args) of
	hanoidb -> lager:info("Starting HanoiDB..."),
               hanoidb_persistence;		
    ets     -> lager:info("Starting ETS..."),
		       ets_persistence;
	_       -> lager:info("Starting ADBTree..."),
		       adbtree_persistence
    end.
    