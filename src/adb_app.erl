-module(adb_app). 

% The purpose of an active application is 
% to run one or more processes. In order to 
% have some control over those process, they 
% showld be spawned and managed by supervisors: 
% processes that implements the supervisor 
% behavior. 

-behavior(application). 

-export([start/2, stop/1]).

% this operation is called when the 
% OTP system wants to start our application. 
% actually, this is the most relevant 
% operation of this model, which is responsible 
% for starting the root supervisor. 
start(_Type, _StartArgs) ->
     case adb_sup:start_link() of 
	 {ok, Pid} -> {ok, Pid};
	 Other -> {error, Other}
     end. 

stop(_State) ->
    ok. 
