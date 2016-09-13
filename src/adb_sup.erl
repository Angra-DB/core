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
-export([start_link/1, start_child/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(LSock) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE,[LSock]).

start_child() ->
    supervisor:start_child(?SERVER, []).

init([LSock]) ->
    Server = {adb_server, {adb_server, start_link, [LSock]}, % {Id, Start, Restart, ... }  
	      temporary, brutal_kill, worker,[adb_server]},    
    Children = [Server], 
    RestartStrategy = {simple_one_for_one, 0, 1},  % {How, Max, Within} ... Max restarts within a period
    {ok, {RestartStrategy, Children}}. 
 
