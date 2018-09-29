-module(adb_app). 

% The purpose of an active application is 
% to run one or more processes. In order to 
% have some control over those process, they 
% showld be spawned and managed by supervisors: 
% processes that implements the supervisor 
% behavior. 

-behavior(application). 

-export([start/2, stop/1, kickoff/1, kickoff/2]).

-define(DEFAULT_PORT, 1234). 

% this operation is called when the 
% OTP system wants to start our application. 
% actually, this is the most relevant 
% operation of this model, which is responsible 
% for starting the root supervisor. 
kickoff(core) ->
    application:start(adb_core);
kickoff(all) ->
    lager:start(),
    lager:info("Lager's console log level set to 'info'..."),
    application:start(adb_core);
kickoff(_) ->
    invalid_argument.

kickoff(all, LogLevel)->
  lager:start(),

  % setting up the log level...
  ValidLogLevels = [debug, info, notice, warning, error, critical, alert, emergency],
  case [X || X <- ValidLogLevels, X =:= LogLevel] of
    []  -> lager:info("'~p' is not an valid log level. Setting Lager's console log level to 'info'...", [LogLevel]);
    _   ->
      lager:info("Setting Lager's console log level to '~p'...", [LogLevel]),
      lager:set_loglevel(lager_console_backend, LogLevel)
  end,

  application:start(adb_core).

start(_Type, _StartArgs) ->

    lager:info("Starting the AngraDB server. ~n"),
    Port = case application:get_env(tcp_interface, port) of
             {ok, P}   -> P;
             undefined -> ?DEFAULT_PORT
           end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),

    lager:info("Listening to TCP requests on port ~w ~n", [Port]),

    case adb_sup:start_link(LSock, _StartArgs) of
      {ok, Pid} ->
        server_sup:start_child(),
        {ok, Pid};
      Other ->
        error_logger:error_msg(" error: ~s", [Other]),
        {error, Other}
    end.

stop(_State) ->
    ok. 
