-module(server_sup).

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
-export([start_link/1, start_link/2, start_child/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(LSock) ->
    start_link(LSock, []).

start_link(LSock, Args) ->
    Persistence = setup_persistence(Args),
    Authentication = setup_authentication(Args),
    Authorization = setup_authorization(Args),
    supervisor:start_link({local, ?SERVER}, ?MODULE, [LSock, Persistence, Authentication, Authorization]).

start_child() ->
    supervisor:start_child(?SERVER, []).

init([LSock, Persistence, Authentication, Authorization]) ->
  lager:info("Initializing server_sup...", []),
  gen_authentication:start(Authentication, Persistence),
  gen_authorization:start(Authorization, Persistence),

  Server = {adb_server, {adb_server, start_link, [LSock, Persistence, Authentication, Authorization]}, % {Id, Start, Restart, ... }
      temporary, brutal_kill, worker, [adb_server]},
  RestartStrategy = {simple_one_for_one, 1000, 3600},  % {How, Max, Within} ... Max restarts within a period
  {ok, {RestartStrategy, [Server]}}.

setup_persistence(Args) ->
  lager:info("Setting up the persistence module.", []),
    case proplists:get_value(persistence, Args) of
      {{name, hanoidb}, Settings } ->
        lager:info("Starting HanoiDB..."),
        {hanoidb_persistence, Settings};
      {{name, ets}, Settings } ->
        lager:info("Starting ets..."),
        {ets_persistence, Settings};
      {{name, _}, Settings } ->
        lager:info("starting ADBtree..."),
        {adbtree_persistence, Settings}
    end.

setup_authentication(Args) ->
  lager:info("Setting up the authentication module.", []),
  case proplists:get_value(authentication, Args) of
    {{name, _}, Settings } ->
      lager:info("starting ADB Authentication..."),
      {adb_authentication, Settings}
  end.

setup_authorization(Args) ->
  lager:info("Setting up the authorization module.", []),
  case proplists:get_value(authorization, Args) of
    {{name, _}, Settings } ->
      lager:info("starting Simple Authorization..."),
      {simple_authorization, Settings}
  end.
