-module(adb_cert_authentication).
-include("gen_authentication.hrl").
-include("adb_cert_authentication.hrl").

-behaviour(gen_authentication).

% gen_authentication callbacks
-export([
	init/2,
	handle_login/3,
	handle_logout/1,
	handle_register/3
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
% otherwise, it returns {?LoggedOut, user_not_found}.
handle_login(_, Persistence_scheme, Socket) ->
	case ssl:peercert(Socket) of
		{error, Error} ->
			{?LoggedOut, Error};
		{ok, EncodedCert} ->
			EncodedCertHash = auth_utils:get_hash_as_hex(EncodedCert),
			case gen_persistence:process_request(lookup, list_to_atom(?AuthenticationDBName), EncodedCertHash, Persistence_scheme, none) of
				{ok, Doc} ->
					User_infos = jsone:decode(list_to_binary(Doc), [{object_format, proplist}]),
					Username = proplists:get_value(<<"username">>, User_infos), % User_password_hash :: list
					{?LoggedIn, #authentication_info{username = Username}};
				not_found ->
					lager:debug("adb_cert_authentication -- user not found. Log in failed...~n", []),
					{?LoggedOut, user_not_found};
				Error ->
					lager:debug("adb_cert_authentication -- error while logging in: ~p~n", [Error]),
					{?LoggedOut, Error}
			end
	end.

% simply returns a "logged out" authentication status, formatted as gen_authentication expects.
handle_logout(_Authentication_status) ->
	{?LoggedOut, none}.

% saves the username and user's certificate as a DER-encoded binary in a document whose key is the md5 hash of this certificate (converted to hex).
% the username, if does not already exist, is also saved in the UsernamesDocument, to keep track of the registered usernames.
% If everything goes well, it will return {ok, DocKey}
% If the chosen username already exists, or other kind of error occurs, it will return {error, ErrorType}
handle_register({Username, _}, Persistence_scheme, Socket) ->
	case ssl:peercert(Socket) of
		{error, Error} ->
			{error, Error};
		{ok, EncodedCert} ->
			EncodedCertHash = auth_utils:get_hash_as_hex(EncodedCert),
			AuthDBNameAtom = list_to_atom(?AuthenticationDBName),
			% checking if this certificate is already registered
			case gen_persistence:process_request(lookup, AuthDBNameAtom, EncodedCertHash, Persistence_scheme, none) of
				{ok, _} ->
					lager:debug("adb_cert_authentication -- registering failed: client certificate is already registered.~n", []),
					{error, certificateAlreadyRegistered};
				not_found ->
					% now check if this username is already in use
					{ok, UsernamesDoc} = gen_persistence:process_request(lookup, list_to_atom(?AuthenticationDBName), ?UsernamesDocKey, Persistence_scheme, none),
					RegisteredUsernames = jsone:decode(list_to_binary(UsernamesDoc)),
					case lists:member(Username, RegisteredUsernames) of
						true ->
							{error, user_already_exists};
						false ->
							NewUsernamesDoc = jsone:encode([Username | RegisteredUsernames]),
							gen_persistence:process_request(update, AuthDBNameAtom, {?UsernamesDocKey, binary_to_list(NewUsernamesDoc)}, Persistence_scheme, none),
							User_doc = jsone:encode([{username, Username}, {certificate, binary_to_list(EncodedCert)}]), % in order to make jsone:encode work correctly, it is good to avoid using a binary as value, and thats why I transformed it to string
							case gen_persistence:process_request(save_key, AuthDBNameAtom, {EncodedCertHash, binary_to_list(User_doc)}, Persistence_scheme, none) of
								{_, {error, ErrorType}} ->
									lager:debug("adb_cert_authentication -- error while saving user document on AuthenticationDB. Error: ~p~n", [ErrorType]),
									{error, ErrorType};
								DocKey ->
									lager:debug("adb_cert_authentication -- user registered successfully.~n", []),
									{ok, DocKey}
							end
					end;
				Error ->
					lager:debug("adb_cert_authentication -- error while looking for a document whose key is the client certificate hash. Error: ~p~n", [Error]),
					{?LoggedOut, Error}
			end
	end.
