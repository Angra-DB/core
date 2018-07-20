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
    Core = {adb_core_server, {adb_core, start_link, Args}, % {Id, Start, Restart, ...}
            temporary, brutal_kill, worker, [adb_core]},
    DistSup = {adb_dist_sup, {adb_dist_sup, start_link, Args},
               temporary, brutal_kill, worker, [adb_dist_sup]},
    RestartStrategy = {simple_one_for_one, 1000, 3600}, % {How, Max, Within} ... Max restarts within a period
    VNodes = proplists:get_value(distribution, Args),
    {ok, {RestartStrategy, lists:duplicate(VNodes, Core) ++ DistSup}}.