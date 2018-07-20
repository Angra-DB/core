-module(adb_core_app).
%% The purpose of an active application is 
%% to run one or more processes. In order to 
%% have some control over those process, they 
%% showld be spawned and managed by supervisors: 
%% processes that implements the supervisor 
%% behavior. 
-behavior(application).

%% Application callbacks
-export([start/2, stop/1]).

%%=============================================================================
%% Application callbacks
%%=============================================================================

start(_Type, StartArgs) ->

    lager:info("Starting the AngraDB. ~n"),
    Target = case os:getenv("Access") of
        false  -> none; 
        Access -> lager:info("Trying to connect to node: ~s ~n", [Access]),
                  Access
    end,

    lager:info("Configuring node distribution. ~n"),
    Dist = proplists:get_value(distribution, StartArgs),
    {ok, Config} = case search_for_node(Dist, Target) of
        {notfound}          -> {ok, StartArgs};
        {ok, RemoteConfig}  -> {ok, RemoteConfig}
    end,
    DistMode = lists:get_value(distribution, Config),
    {ok, _Mode} = configure_dist(DistMode),

    lager:info("Starting the AngraDB persistency"),
    case adb_core_sup:start_link(Config) of
        {ok, Pid}   -> adb_core_sup:start_child(),
                       {ok, Pid};
        Other       -> error_logger:error_msg(" error: ~s", [Other]),
                       {error, Other}
    end.
    
stop(_State) ->
    ok.

%%==============================================================================
%% Internal functions
%%==============================================================================

search_for_node(long, Target) ->
    case gen_server:call({adb_dist, Target}, node_up, []) of
        {badrpc, _Reason} -> lager:warning("Node not found.~n"),
                             {notfound};
        _                 -> lager:info("adb_core successfully connected to adb_server"),
                             {ok, Target}  
    end;
search_for_node(long, none) ->
    {notfound};
search_for_node(short, Target) ->
    case gen_server:call({adb_dist, Target}, node_up, []) of
        {badrpc, _Reason} -> lager:error("Node not found.~n"),
                             {notfound};
        _                 -> lager:info("adb_core successfully connected to adb_server"),
                             {ok, Target} 
    end;
search_for_node(short, none) ->
    {notfound};
search_for_node(default, _) ->
    {notfound}.

configure_dist(short) ->
    case net_kernel:start([adb_server, shortnames]) of
        {ok, _Pid}      -> lager:info("Application started on shortname distributed mode."),
                           {ok, short};
        {error, Reason} -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                           lager:info("Application started on default mode."),
                           {error, Reason}
    end;
configure_dist({long, Host}) when is_atom(Host) ->
    Name = list_to_atom("adb_server@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid}      -> lager:info("Application started on longname distributed mode."),
                           {ok, long};
        {error, Reason} -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                           lager:info("Application started on default mode."),
                           {error, Reason}
    end;
configure_dist(default) ->
    lager:info("Application started on default mode."),
    {ok, default};
configure_dist(_) ->
    {error, "Invalid argument"}.



