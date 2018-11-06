-module(adb_authentication).
-include("gen_authentication.hrl").
-include("adb_authentication.hrl").

-behaviour(gen_authentication).

% API functions

-export([
	init/2,
	login/3,
	logout/1,
	register/3
]).

% initializes the adb_authentication module. It creates a db for the users' authentication infos, if it does
% not exist (that is why it needs to know the persistence setup).
init(_Authentication_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("adb_authentication -- Initializing adb_authentication module...", []),
	case gen_persistence:process_request(connect, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("adb_authentication -- AuthenticationDatabase found and connected...", []),
			ok;
		db_does_not_exist ->
			lager:debug("adb_authentication -- AuthenticationDatabase not found, creating a new one...", []),
			gen_persistence:process_request(create_db, none, ?AuthenticationDBName, Persistence_scheme, Persistence_settings);
		Error ->
			lager:debug("adb_authentication -- Error while trying to connect to AuthenticationDatabase: ~p", [Error]),
			Error
	end.

% verifies the response to the challenge, and returns the filled {?LoggedIn, #authentication_info} tuple if everything is ok.
% otherwise, it returns {?LoggedOut, ?ErrorInvalidPasswordOrUsername} in case the username or password are invalid,
% or {?LoggedOut, _} if any other login failure/error occurs
login(Username, Typed_password, Persistence_scheme) ->
	Username_hash = get_hash_as_hex(Username),
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
					{?LoggedOut, ?ErrorInvalidPasswordOrUsername}
			end;
		not_found ->
			lager:debug("adb_authentication -- user ~p not found. Log in failed...~n", [Username]),
			{?LoggedOut, ?ErrorInvalidPasswordOrUsername};
		Error ->
			lager:debug("adb_authentication -- error while logging in: ~p~n", [Error]),
			{?LoggedOut, Error}
	end.
	%	{?LoggedIn, #authentication_info{username = Username}}. % simple mock, in case you want to test the app without the original adb_authentication:login logic

% simply returns a "logged out" authentication status, formatted as gen_authentication expects.
logout(_Authentication_status) ->
	{?LoggedOut, none}.

% saves the user's password and salt in a document whose key is the md5 hash of the username converted to hex.
% the password is hashed using pbkdf2 (with the parameters Pbkdf2_hash_algorithm, Pbkdf2_iterations and Pbkdf2_derived_key_length, defined in "adb_authentication.hrl"),
% before being stored on the document
% todo: find a way to use less conversions (like list_to_binary, binary_to_list, and so on...)
register(Username, Password, Persistence_scheme) ->
	Username_hash = get_hash_as_hex(Username), % hash (binary converted to [string] hex) used to find the document that stores the user's data
	User_salt = randomstring:get(32, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"), % 32 is the recommended size of a salt
	{ok, Derived_key} = pbkdf2:pbkdf2(?Pbkdf2_hash_algorithm, list_to_binary(Password), list_to_binary(User_salt), ?Pbkdf2_iterations, ?Pbkdf2_derived_key_length),
	User_doc = jsone:encode([{password, binary_to_list(Derived_key)}, {salt, User_salt}]),
	AuthDBNameAtom = list_to_atom(?AuthenticationDBName),
	case gen_persistence:process_request(lookup, AuthDBNameAtom, Username_hash, Persistence_scheme, none) of
		not_found ->
			case gen_persistence:process_request(save_key, AuthDBNameAtom, {Username_hash, binary_to_list(User_doc)}, Persistence_scheme, none) of
				{_, {error, ErrorType}} -> {error, ErrorType};
				DocKey -> {ok, DocKey}
			end;
		_Error -> {error, ?ErrorUserAlreadyExists}
	end.

% calculates the md5 hash, and then converts the digest to hex (as list), in the expected format of a doc key.
get_hash_as_hex(Data) ->
	get_hash_as_hex(Data, md5).

% calculates the hash using the given algorithm, and then converts the digest to hex (as list), in the expected format of a doc key.
get_hash_as_hex(Data, Hash_algorithm) ->
binary_to_list(bin_to_hex:bin_to_hex(crypto:hash(Hash_algorithm, Data))).
