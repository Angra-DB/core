-module(adb_auth).
-include("gen_auth.hrl").

-behaviour(gen_auth).

% API functions

-export([
	init/2,
	login/2,
	logout/1
]).

% initializes the adb_auth module. It creates a db for the users' authentication/authorization infos, if it does
% not exist (that is why it needs to know the persistence setup).
init(_Auth_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("Initializing adb_auth module...", []),
	case gen_persistence:process_request(connect, none, ?AuthDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("AuthDatabase connected...", []),
			ok;
		db_does_not_exist ->
			lager:debug("AuthDatabase not found, creating a new one...", []),
			gen_persistence:process_request(create_db, none, ?AuthDBName, Persistence_scheme, Persistence_settings);
		Error ->
			lager:debug("Error while trying to connect to AuthDatabase: ~p", [Error]),
			Error
	end.

% verifies the response to the challenge, and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns only {?LoggedOut, none}
login(Username, Password) ->
	{?LoggedIn, #authentication_info{username = Username}}.

logout(_Auth_status) ->
	{?LoggedOut, none}.

%%is_logged_in(Auth_status) ->
%%	?LoggedIn.

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
