-module(adb_server_app). 
% The purpose of an active application is 
% to run one or more processes. In order to 
% have some control over those process, they 
% showld be spawned and managed by supervisors: 
% processes that implements the supervisor 
% behavior. 
-behavior(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(DEFAULT_PORT, 1234).

%%=============================================================================
%% Application callbacks
%%=============================================================================

start(_Type, StartArgs) ->

    case configure_dist(proplists:get_value(distribution, StartArgs)) of
        {ok, short}      -> lager:info("Application started on shortname distributed mode.");
        {ok, long}       -> lager:info("Application started on longname distributed mode.");
        {ok, none}       -> lager:info("Application started on standard mode.");
        {error, Reason}  -> lager:warning("Application failed to start on distributed mode: ~p.", [Reason]),
                            lager:info("Application started on standard mode.")
    end,

    lager:info("Starting the AngraDB server."), 
    Port = case application:get_env(tcp_interface, port) of
        {ok, P}   -> P;
        undefined -> ?DEFAULT_PORT
    end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),

    lager:info("Listening to TCP requests on port ~w.", [Port]),
    case adb_server_sup:start_link(LSock, StartArgs) of
        {ok, Pid} -> adb_server_sup:start_child(),
                     {ok, Pid};
        Other     -> error_logger:error_msg(" error: ~s.", [Other]),
                     {error, Other}
    end.

stop(_State) ->
    ok. 

%%==============================================================================
%% Internal functions
%%==============================================================================
configure_dist(short) ->
    case net_kernel:start([adb_server, shortnames]) of
        {ok, _Pid} -> {ok, short};
        {error, _Reason} -> {error, short}
    end;
configure_dist({long, Host}) when is_atom(Host) ->
    Name = list_to_atom("adb_server@" ++ atom_to_list(Host)),
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid} -> {ok, long};
        {error, _Reason} -> {error, long}
    end;
configure_dist(none) ->
    {ok, none};
configure_dist(_) ->
    {error, "Invalid argument"}.
