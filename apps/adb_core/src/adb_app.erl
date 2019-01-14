-module(adb_app).
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

    % Config distribution according to configuration from .app and flags.
    lager:info("Setting up the distribution configuration. ~n"),
    Dist = proplists:get_value(distribution, StartArgs),
    {ok, Mode} = configure_dist(Dist),

    Target = case os:getenv("ADB_ACCESS", none) of
        none   -> none; 
        Access -> lager:info("Trying to connect to node: ~s ~n", [Access]),
                  list_to_atom(Access)
    end,
    Config = case search_for_node(Mode, Target) of
        {ok, RemoteConfig} -> RemoteConfig;
        _                  -> StartArgs
    end,

    lager:info("Starting the AngraDB"),
    case adb_sup:start_link(Config) of
        {ok, Pid}   -> adb_sup:start_child(),
                       {ok, Pid};
        Other       -> error_logger:error_msg(" error: ~s", [Other]),
                       {error, Other}
    end.

stop(_State) ->
    ok.

%%==============================================================================
%% Internal functions
%%==============================================================================

search_for_node(dist, none) ->
    none;
search_for_node(dist, Target) ->
    case net_adm:ping(Target) of
        pang -> lager:warning("Could not contact node ~s.~n", [Target]),
                {error, notfound};
        pong -> syncing_with_remote(Target)
    end;
search_for_node(mono, _) ->
    none.

syncing_with_remote(Target) ->
    DestTarget = {adb_dist_server, Target},
    Request = {node_up, []},
    try gen_server:call(DestTarget, Request) of
        {ok, State} -> lager:info("Successfully contacted to ~s. ~n", [Target]),        
                       {ok, State};
        _           -> lager:warning("Node not found.~n"),
                       {error, notfound}
    catch
        _Class:Err -> lager:warning("Could not contact node ~s: ~p.~n", [Target, Err]),
                      {error, notfound}
    end.

configure_dist(mono) when node() == nonode@nohost ->
    lager:info("Application starting on monolithic mode."),
    {ok, mono};
configure_dist(dist) when node() == nonode@nohost ->
    {ok, Host} = adb_utils:get_env("ADB_HOST"),
    Name = gen_name(Host),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid} -> 
            lager:info("Application starting on longname distributed mode."),
            {ok, dist};
        {error, _Reason} -> 
            lager:warning("Application failed to start on distributed mode: TIP: Verify if the EPMD process is running!"),
            lager:info("Application starting on monolithic mode."),
            {ok, mono}
    end;
configure_dist(_DistMode) when node() =/= nonode@nohost ->
    {ok, dist};
configure_dist(_DistMode) ->
    {error, invalid_dist_mode}.

gen_name(Host) when is_atom(Host) ->
    gen_name(atom_to_list(Host));

gen_name(Host) when is_list(Host) ->
    Name = string:join([adb_utils:gen_name(), "@", Host], ""),
    list_to_atom(Name).
