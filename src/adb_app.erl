-module(adb_app). 

% The purpose of an active application is 
% to run one or more processes. In order to 
% have some control over those process, they 
% showld be spawned and managed by supervisors: 
% processes that implements the supervisor 
% behavior. 

-behavior(application). 

-export([start/2, stop/1]).

-define(DEFAULT_PORT, 1234). 

% this operation is called when the 
% OTP system wants to start our application. 
% actually, this is the most relevant 
% operation of this model, which is responsible 
% for starting the root supervisor. 

start(_Type, _StartArgs) ->

    lager:info("Starting the AngraDB server. ~n"), 
    Port = case application:get_env(tcp_interface, port) of
             {ok, P}   -> P;
             undefined -> ?DEFAULT_PORT
           end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),

    lager:info("Listening to TCP requests on port ~w ~n", [Port]),
  
    case adb_sup:start_link(LSock, _StartArgs) of
      {ok, Pid} -> adb_sup:start_child(),
                   {ok, Pid};
      Other     -> error_logger:error_msg(" error: ~s", [Other]),
                   {error, Other}
    end.

stop(_State) ->
    ok. 
