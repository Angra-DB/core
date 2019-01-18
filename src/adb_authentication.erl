-module(adb_authentication).
-include("gen_authentication.hrl").
-include("adb_authentication.hrl").

-behaviour(gen_authentication).

% gen_authentication callbacks
-export([
	init/2,
	handle_login/3,
	handle_logout/1,
	handle_register/3
]).


% Currently, the module adb_authentication saves one document for each user, with the hash (md5) of the username as key
% of this document, and these documents are saved inside a DB named ?AuthenticationDBName. These documents store informations
% such as the user's password (appended with a salt, and hashed with pbkdf2, with the parameters descripted in adb_authentication.hrl) and
% the salt (more information can be added in the future).


% initializes the adb_cert_authentication module. It creates a db for the users' authentication infos, if it does not exist
% using AngraDB gen_persistence(that is why it needs to know the persistence setup).
init(_Authentication_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("adb_authentication -- Initializing adb_authentication module...", []),
	case gen_persistence:process_request(connect, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("adb_authentication -- ~p found and connected...", [?AuthenticationDBName]),
			ok;
		db_does_not_exist ->
			lager:debug("adb_authentication -- ~p not found, creating a new one...", [?AuthenticationDBName]),
			gen_persistence:process_request(create_db, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings);
		Error ->
			lager:debug("adb_authentication -- Error while trying to connect to ~p: ~p", [?AuthenticationDBName, Error]),
			Error
	end.

% verifies the response to the challenge, and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns {?LoggedOut, ?ErrorInvalidPasswordOrUsername} in case the username or password are invalid,
% or {?LoggedOut, _} if any other login failure/error occurs
handle_login({Username, Typed_password}, Persistence_scheme, _Socket) ->
	Username_hash = auth_utils:get_hash_as_hex(Username),
	case gen_persistence:process_request(lookup, list_to_atom(?AuthenticationDBName), Username_hash, Persistence_scheme, none) of
		{ok, Doc} ->
			User_infos = jsone:decode(list_to_binary(Doc), [{object_format, proplist}]),
			User_password_hash = proplists:get_value(<<"password">>, User_infos), % User_password_hash :: list
			User_salt = proplists:get_value(<<"salt">>, User_infos), % User_salt :: list
			{ok, Typed_password_hash} = pbkdf2:pbkdf2(?Pbkdf2_hash_algorithm, list_to_binary(Typed_password), list_to_binary(User_salt), ?Pbkdf2_iterations, ?Pbkdf2_derived_key_length), % Typed_password_hash :: binary
			case User_password_hash == binary_to_list(Typed_password_hash) of
				true ->
					lager:debug("adb_authentication -- correct credentials for user ~p. Logging in...~n", [Username]),
					{?LoggedIn, #authentication_info{username = Username}};
				false ->
					lager:debug("adb_authentication -- invalid credentials for user ~p. Log in failed...~n", [Username]),
					{?LoggedOut, invalid_password_or_username}
			end;
		not_found ->
			lager:debug("adb_authentication -- user ~p not found. Log in failed...~n", [Username]),
			{?LoggedOut, invalid_password_or_username};
		Error ->
			lager:debug("adb_authentication -- error while logging in: ~p~n", [Error]),
			{?LoggedOut, Error}
	end.
	%	{?LoggedIn, #authentication_info{username = Username}}. % simple mock, in case you want to test the app without the original adb_authentication:login logic

% simply returns a "logged out" authentication status, formatted as gen_authentication expects.
handle_logout(_Authentication_status) ->
	{?LoggedOut, none}.

% saves the user's password and salt in a document whose key is the md5 hash of the username converted to hex.
% the password is hashed using pbkdf2 (with the parameters Pbkdf2_hash_algorithm, Pbkdf2_iterations and Pbkdf2_derived_key_length, defined in "adb_authentication.hrl"),
% before being stored on the document
% todo: find a way to use less conversions (like list_to_binary, binary_to_list, and so on...)
handle_register({Username, Password}, Persistence_scheme, _Socket) ->
	Username_hash = auth_utils:get_hash_as_hex(Username), % hash (binary converted to [string] hex) used to find the document that stores the user's data
	User_salt = randomstring:get(22, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"), % the recommended size of salts is 128 bits. Therefore, 22 alphanumeric characters gets close to the number of possibilities of 128 bits.
	{ok, Derived_key} = pbkdf2:pbkdf2(?Pbkdf2_hash_algorithm, list_to_binary(Password), list_to_binary(User_salt), ?Pbkdf2_iterations, ?Pbkdf2_derived_key_length),
	User_doc = jsone:encode([{password, binary_to_list(Derived_key)}, {salt, User_salt}]), % in order to make jsone:encode work correctly, it is good to avoid using a binary as value, and thats why I transformed it to string
	AuthDBNameAtom = list_to_atom(?AuthenticationDBName),
	case gen_persistence:process_request(lookup, AuthDBNameAtom, Username_hash, Persistence_scheme, none) of
		not_found ->
			case gen_persistence:process_request(save_key, AuthDBNameAtom, {Username_hash, binary_to_list(User_doc)}, Persistence_scheme, none) of
				{_, {error, ErrorType}} -> {error, ErrorType};
				DocKey -> {ok, Username}
			end;
		_Error -> {error, user_already_exists}
	end.
