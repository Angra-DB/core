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

    Target = case os:getenv("ADB_ACCESS", none) of
        none   -> none; 
        Access -> lager:info("Trying to connect to node: ~s ~n", [Access]),
                  list_to_atom(Access)
    end,

    lager:info("Setting up the distribution configuration. ~n"),
    Dist = proplists:get_value(distribution, StartArgs),
    {ok, Host} = adb_utils:get_env("ADB_HOST"),
    Config = case search_for_node(Dist, Target) of
        {ok, RemoteConfig} -> RemoteConfig;
        _                  -> StartArgs ++ [{server, none}]
    end,
    {ok, _Mode} = configure_dist({Dist, Host}, Target),

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
    start_node(),
    try
        Result = case gen_server:call({adb_dist_server, Target}, {node_up, []}) of
            {ok, State} -> lager:info("Successfully contacted to ~s. ~n", [Target]),
                        {ok, State};
            _        -> lager:warning("Node not found.~n"),
                        {error, notfound}
        end,
        stop_node(),
        Result
    catch
        _Class:Err -> lager:warning("Could not contact node: ~p.~n", [Err]),
                      stop_node(),
                      {error, notfound}
    end;
search_for_node(mono, _) ->
    none.

configure_dist({dist, Host}, none) ->
    Name = list_to_atom(adb_utils:gen_name() ++ "@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid}       -> lager:info("Application starting on longname distributed mode."),
                            {ok, dist};
        {error, _Reason} -> lager:warning("Application failed to start on distributed mode: TIP: Verify if the EPMD process is running!"),
                            lager:info("Application starting on monolithic mode."),
                            {ok, mono}
    end;
configure_dist({dist, Host}, RemoteNode) ->
    Name = list_to_atom(adb_utils:gen_name() ++ "@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid}      -> lager:info("Application starting on longname distributed mode."),
                           net_kernel:connect_node(RemoteNode),
                           {ok, dist};
        {error, Reason} -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                           lager:info("Application starting on monolithic mode."),
                           {ok, mono}
    end;
configure_dist(mono, none) ->
    lager:info("Application starting on monolithic mode."),
    {ok, mono};
configure_dist(_, _) ->
    {error, "Invalid argument"}.

start_node() ->
    {ok, Host} = adb_utils:get_env("ADB_HOST"),
    TempName = list_to_atom("temp" ++ adb_utils:gen_name() ++ "@" ++ atom_to_list(Host)),
    case node() of
        nonode@nohost -> net_kernel:start([TempName, longnames]),
                         TempName;
        ChosenName    -> ChosenName
    end.

stop_node() ->
    ok = net_kernel:stop().
