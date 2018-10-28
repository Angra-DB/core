-module(adb_authentication).
-include("gen_authentication.hrl").

-behaviour(gen_authentication).

% API functions

-export([
	init/2,
	login/2,
	logout/1
]).

% initializes the adb_authentication module. It creates a db for the users' authentication infos, if it does
% not exist (that is why it needs to know the persistence setup).
init(_Authentication_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("Initializing adb_authentication module...", []),
	case gen_persistence:process_request(connect, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("AuthenticationDatabase connected...", []),
			ok;
		db_does_not_exist ->
			lager:debug("AuthenticationDatabase not found, creating a new one...", []),
			gen_persistence:process_request(create_db, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings);
		Error ->
			lager:debug("Error while trying to connect to AuthenticationDatabase: ~p", [Error]),
			Error
	end.

% verifies the response to the challenge, and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns only {?LoggedOut, none}
login(Username, Password) ->
	{?LoggedIn, #authentication_info{username = Username}}.

logout(_Authentication_status) ->
	{?LoggedOut, none}.
