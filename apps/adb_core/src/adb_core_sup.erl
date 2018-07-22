-module(adb_core_sup).

% As explained before (see the adb_server_app.erl module), 
% an active OTP application consists of one or more 
% process that do the work (that is, they receive 
% server side requests and manipulate our database). 
% These processes are started indirectly by supervisors, 
% which are responsible for supervising them and restarting 
% them if necessary (OTP in Action). A running application is 
% a tree of processes, both supervisors and workers. 

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
    VNodes = proplists:get_value(vnodes, Args),
    CoreNames = [list_to_atom(atom_to_list(adb_core_) ++ integer_to_list(X)) || X <- lists:seq(1, VNodes)],
    Cores = [#{id       => Name, 
               start    => {adb_core, start_link, [Persistence, Name]},
               restart  => temporary, 
               shutdown => brutal_kill, 
               type     => worker, 
               modules  => [adb_core]} || Name <- CoreNames],
    DistSup = #{id       => adb_dist_sup, 
                start    => {adb_dist_sup, start_link, [Args]}, 
                restart  => temporary, 
                shutdown => brutal_kill, 
                type     => worker, 
                modules  => [adb_dist_sup]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, Cores ++ [DistSup]}}.

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