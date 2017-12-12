-module(adb_core_app).
%% The purpose of an active application is 
%% to run one or more processes. In order to 
%% have some control over those process, they 
%% showld be spawned and managed by supervisors: 
%% processes that implements the supervisor 
%% behavior. 
-behavior(application).

%% API functions
-export([get_hostname/0]).

%% Application callbacks
-export([start/2, stop/1]).

%%=============================================================================
%% API functions
%%=============================================================================

get_hostname() -> 
    {_, [_|Host]} = lists:splitwith(fun(C) -> C =/= $@ end, atom_to_list(node())),
    list_to_atom(Host).

%%=============================================================================
%% Application callbacks
%%=============================================================================

start(_Type, StartArgs) ->
    lager:info("Starting the AngraDB core persistence. ~n"),
    %% Always search the server in the same host
    Hosts = lists:append([get_hostname()], proplists:get_value(hosts, StartArgs)),
    {ok, Target} = connect_to_server_node(Hosts),
    case adb_core_sup:start_link(Target) of
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

connect_to_server_node([Host|[]]) when node =/= nonode@nohost ->
    Target = list_to_atom("adb_server@" ++ atom_to_list(Host)),
    case rpc:call(Target, adb_server_app, node_up, [node()]) of
        {badrpc, _Reason} -> lager:error("No server found."),
                            error;
        _                -> lager:info("adb_core successfully connected to adb_server"),
                            {ok, Target}  
    end;
connect_to_server_node([Head | Tail]) when node() =/= nonode@nohost ->
    Target = list_to_atom("adb_server@" ++ atom_to_list(Head)),
    case rpc:call(Target, adb_server_app, node_up, [node()]) of
        {badrpc, _Reason} -> connect_to_server_node(Tail);
        _                -> lager:info("adb_core successfully connected to adb_server"),
                            {ok, Target} 
    end;
connect_to_server_node(_) ->
    {ok, node()}.



