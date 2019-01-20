-module(adb_server_sup).

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
-export([start_link/0, start_child/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child() ->
    supervisor:start_child(?SERVER, []).

init([]) ->
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    Servers = [get_server_spec(Id) || Id <- lists:seq(1, VNodes)],
    RestartStrategy = {one_for_one, 1000, 3600},  % {How, Max, Within} ... Max restarts within a period
    {ok, {RestartStrategy, Servers}}.

%%==============================================================================
%% Internal functions
%%==============================================================================

get_server_spec(VNodeId) ->
    Port = get_port(VNodeId),
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),
    lager:info("Listening to TCP requests on port ~w.", [Port]),
    #{id       => adb_utils:get_vnode_process(adb_server, VNodeId),
      start    => {adb_server, start_link, [LSock, VNodeId]},
      restart  => permanent,
      shutdown => brutal_kill,
      type     => worker,
      modules  => [adb_server]}.

get_port(VNodeId) ->
    {ok, P} = adb_utils:get_env("ADB_PORT"),
    StartPort = if 
        is_integer(P) -> P;
        true          -> {Num, []} = string:to_integer(P),
                         Num
    end,
    StartPort + VNodeId - 1.
