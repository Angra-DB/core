-module(adb_sup).

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
    DistSup = #{id       => adb_dist_sup, 
                start    => {adb_dist_sup, start_link, [Args]}, 
                restart  => permanent, 
                shutdown => infinity, 
                type     => supervisor, 
                modules  => [adb_dist_sup]},
    VNodeSup = #{id       => adb_vnode_sup,
                 start    => {adb_vnode_sup, start_link, [Args]},
                 restart  => permanent,
                 shutdown => infinity,
                 type     => supervisor,
                 modules  => [adb_vnode_sup]},
    RestartStrategy = {one_for_one, 1000, 3600},
    {ok, {RestartStrategy, [DistSup, VNodeSup]}}.

