-module(adb_core_app).
%% The purpose of an active application is 
%% to run one or more processes. In order to 
%% have some control over those process, they 
%% showld be spawned and managed by supervisors: 
%% processes that implements the supervisor 
%% behavior. 
-behavior(application).

-export([start/2, stop/1]).

start(_Type, _StartArgs) ->

    lager:info("Starting the AngraDB core. ~n"),
    case adb_core_sup:start_link() of
        {ok, Pid}   -> adb_core_sup:start_child(),
            {ok, Pid};
        Other       -> error_logger:error_msg(" error: ~s", [Other]),
            {error, Other}
    end.

stop(_State) ->
    ok.