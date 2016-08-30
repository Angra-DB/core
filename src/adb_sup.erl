-module(adb_sup).

% As explained before (see the adb_app.erl module), 
% an active OTP application consists of one or more 
% process that do the work (that is, they receive 
% server side requests and manipulate our database). 
% These processes are started indirectly by supervisors, 
% which are responsible for supervising them and restarting 
% them if necessary (OTP in Action). A running application is 
% a tree of processes, both supervisors and workers. 

-behavior(supervisor). 

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE,[]).

init([]) ->
    Server = {adb_server, {adb_server, start_link, []}, % {Id, Start, Restart, ... }  
	      permanent, 2000, worker,[adb_server]},    
    Children = [Server], 
    RestartStrategy = {one_for_one, 0, 1},  % {How, Max, Within} ... Max restarts within a period
    {ok, {RestartStrategy, Children}}. 
 
