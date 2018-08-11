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

    Target = case os:getenv("Access") of
        false  -> none; 
        Access -> lager:info("Trying to connect to node: ~s ~n", [Access]),
                  list_to_atom(Access)
    end,

    lager:info("Setting up the distribution configuration. ~n"),
    {Dist, Host} = proplists:get_value(distribution, StartArgs),
    {ok, Config} = case search_for_node(Dist, Target) of
        {notfound}          -> {ok, StartArgs ++ [{server, none}]};
        {ok, RemoteConfig}  -> {ok, RemoteConfig}
    end,
    {ok, _Mode} = configure_dist({Dist, Host}, Target),

    lager:info("Starting the AngraDB"),
    Res = case adb_sup:start_link(Config) of
        {ok, Pid}   -> adb_sup:start_child(),
                       {ok, Pid};
        Other       -> error_logger:error_msg(" error: ~s", [Other]),
                       {error, Other}
    end,
    case gen_server:call(adb_dist, {init_server_or_acknowledge, []}) of
        {ok, init}                  -> lager:info("Server initilized on this node: ~p. ~n", [node()]);
        {ok, {acknowledge, Remote}} -> lager:info("Acknowledge to remote server: ~p. ~n", [Remote])
    end,
    Res.

stop(_State) ->
    ok.

%%==============================================================================
%% Internal functions
%%==============================================================================

search_for_node(long, none) ->
    {notfound};
search_for_node(long, Target) ->
    start_node(),
    Result = case gen_server:call({adb_dist, Target}, {node_up, []}) of
        {ok, State} -> lager:info("Successfully contacted to ~s. ~n", [Target]),
                       {ok, State};
        _           -> lager:warning("Node not found.~n"),
                       {notfound}
    end,
    stop_node(),
    Result;
search_for_node(mono, _) ->
    {notfound}.

configure_dist({long, Host}, none) when is_atom(Host) ->
    Name = list_to_atom(adb_utils:gen_name() ++ "@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid}      -> lager:info("Application starting on longname distributed mode."),
                           {ok, long};
        {error, Reason} -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                           lager:info("Application starting on monolithic mode."),
                           {error, Reason}
    end;
configure_dist({long, Host}, RemoteNode) when is_atom(Host) ->
    Name = list_to_atom(adb_utils:gen_name() ++ "@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid}      -> lager:info("Application starting on longname distributed mode."),                       
                           true = net_kernel:connect_node(RemoteNode),
                           lager:info("Successfully connected to ~s. ~n", [RemoteNode]),
                           {ok, long};
        {error, Reason} -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                           lager:info("Application starting on monolithic mode."),
                           {error, Reason}
    end;
configure_dist(mono, none) ->
    lager:info("Application starting on monolithic mode."),
    {ok, mono};
configure_dist(_, _) ->
    {error, "Invalid argument"}.

start_node() ->
    TempName = list_to_atom("temp" ++ adb_utils:gen_name() ++ "@127.0.0.1"),
    case node() of
        nonode@nohost -> net_kernel:start([TempName, longnames]),
                         TempName;
        ChosenName    -> ChosenName
    end.

stop_node() ->
    ok = net_kernel:stop().



