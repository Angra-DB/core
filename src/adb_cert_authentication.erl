-module(adb_cert_authentication).
-include("gen_authentication.hrl").
-include("adb_cert_authentication.hrl").

-behaviour(gen_authentication).

% gen_authentication callbacks
-export([
	init/2,
	login/2,
	logout/1,
	register/2
]).


% The module adb_cert_authentication saves one document for each user, with the hash (md5) of the user's certificate as key
% of this document, and these documents are saved inside a DB named ?AuthenticationDBName. These documents store informations
% such as username and the certificate itself, as a DER-encoded binary.


% initializes the adb_cert_authentication module. It creates a db for the users' authentication infos, if it does not exist
% using AngraDB gen_persistence(that is why it needs to know the persistence setup). This DB will also store a document that will
% hold a list with all the registered usernames as strings (this init function will also create it).
init(_Authentication_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("adb_cert_authentication -- Initializing adb_authentication module...", []),
	case gen_persistence:process_request(connect, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("adb_cert_authentication -- ~p found and connected...", [?AuthenticationDBName]),
			ok;
		db_does_not_exist -> % if the AuthenticationDB does not exist yet, create it and also create the document that will store the existing usernames, as a list of strings
			lager:debug("adb_cert_authentication -- ~p not found, creating a new one, and also creating the usernames document...", [?AuthenticationDBName]),
			gen_persistence:process_request(create_db, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings),
			gen_persistence:process_request(save_key, list_to_atom(?AuthenticationDBName), {?UsernamesDocKey, "[]"}, Persistence_scheme, none);
		Error ->
			lager:debug("adb_cert_authentication -- Error while trying to connect to ~p: ~p", [?AuthenticationDBName, Error]),
			Error
	end.

% verifies the hash of the presented client certificate, and check whether it already exists in ?AuthenticationDBName,
% and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns {?LoggedOut, userDoesNotExist}.
login(_, Persistence_scheme) ->
	{?LoggedIn, none}. % simple mock, in case you want to test the app without the original adb_authentication:login logic

% simply returns a "logged out" authentication status, formatted as gen_authentication expects.
logout(_Authentication_status) ->
	{?LoggedOut, none}.

% saves the username and user's certificate as a DER-encoded binary in a document whose key is the md5 hash of this certificate converted to hex.
% the username, if does not already exist, is also saved in the UsernamesDocument, to keep track of the registered usernames.
% If everything goes well, it will return {ok, DocKey}
% If the chosen username already exists, or other kind of error occurs, it will return {error, {ErrorType, ErrorMessage}}
% todo: find a way to use less conversions (like list_to_binary, binary_to_list, and so on...)
register({Username, Password}, Persistence_scheme) ->
  {ok, docKey}.
