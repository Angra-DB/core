-module(adb_auth).
-include("gen_auth.hrl").

-behaviour(gen_auth).

% API functions

-export([
%%	init_auth_table/1,
	login/2,
	logout/1,
	is_logged_in/1
]).

% verifies the response to the challenge, and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns only {?LoggedOut, none}
login(Username, Password) ->
	{?LoggedIn, #authentication_info{username = Username}}.

logout(Auth_status) ->
	?LoggedOut.

is_logged_in(Auth_status) ->
	?LoggedIn.

%%is_logged_in(Pid, Socket) ->
%%	case ets:lookup(pid_to_table_name(Pid), Socket) of
%%		[Result | _] -> {logged_in, Result};
%%		[] -> {not_logged_in, []}
%%	end.

%%% initializes the authentication/authorization table and other security resources
%%init_auth_table(Pid) ->
%%	_ = ets:new(pid_to_table_name(Pid), [set, private, named_table]),
%%	{ok}.

%%pid_to_table_name(Pid) ->
%%	list_to_atom("auth" ++ pid_to_list(Pid)).
