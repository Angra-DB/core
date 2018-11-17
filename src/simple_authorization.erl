-module(simple_authorization).
-include("gen_authorization.hrl").
-include("gen_authentication.hrl").
-include("simple_authorization.hrl").

-behaviour(gen_authorization).

% gen_authorization callbacks
-export([
	init/2,
	request_permission/4,
	grant_permission/3,
	revoke_permission/3,
	show_permission/3
]).


% The simple_authorization module saves authorization information about a database, such as users' permissions, in documents that
% are stored in a DB called ?AuthorizationDBName. Each of these documents can be retrieved with a key that is actually
% the md5 hash of the referred database (in hex string format). Inside one of these documents, you can find a json whose
% keys are usernames, and values are their permissions upon that specific database.
% The permissions, in simple_authorization module, are integers defined in "simple_authorization.hrl", and, to verify if the user
% has indeed certain permission, we check if the integer that represents his permission is greater or equal than the permission
% required for his action. Therefore, we can also state that the permissions, in this module, are hierarchical.
% In this module, the first user who requires permission to one DB earns owner access to it, except for the critical DBs (such as
% AuthenticationDatabase or AuthorizationDatabase. To manipulate the permissions for these critical DBs, you will have to do it
% by calling the API of this module directly from the erlang VM, instead of doing this in any sort of client, since these last ones
% are subject to permissions checking, and actions that are done on the erlang VM are not.


% initializes the simple_authorization module. It creates a DB for permission infos, if it does
% not exist (that is why it needs to know the persistence setup).
% In this initialization function, we need to create permission docs for the critical databases, in order to avoid granting
% owner permission to anyone who first requests permission to these DBs
init(_Authentication_settings, {Persistence_scheme, Persistence_settings}) ->
	lager:debug("simple_authorization -- Initializing simple_authorization module...", []),
	case gen_persistence:process_request(connect, none, ?AuthorizationDBName, Persistence_scheme, Persistence_settings) of
		ok ->
			lager:debug("simple_authorization -- ~p found and connected...", [?AuthorizationDBName]),
			ok;
		db_does_not_exist ->
			lager:debug("simple_authorization -- ~p not found, creating a new one...", [?AuthorizationDBName]),
			gen_persistence:process_request(create_db, none, ?AuthorizationDBName, Persistence_scheme, Persistence_settings),
			% create these permission docs for the critical databases (Authentication and Authorization), in order to avoid granting owner permission to anyone who first requests permission to these DBs
			gen_persistence:process_request(save_key, list_to_atom(?AuthorizationDBName), {auth_utils:get_hash_as_hex(?AuthenticationDBName), "[]"}, Persistence_scheme, none),
			gen_persistence:process_request(save_key, list_to_atom(?AuthorizationDBName), {auth_utils:get_hash_as_hex(?AuthorizationDBName), "[]"}, Persistence_scheme, none);
		Error ->
			lager:debug("simple_authorization -- Error while trying to connect to ~p: ~p", [?AuthorizationDBName, Error]),
			Error
	end.

% since, in this definition, the Database was not set yet (Database = none), we only need to verify if the requested action need any access other than NoPermission.
% If positive, than this function will return {?Forbidden, databaseNotSet}. Otherwise, the access will be granted, since the action does not
% need any special permission, and the function will return {?Granted, databaseNotSet}.
request_permission(_Persistence_scheme, none, _Username, Request = {Command, _Args}) ->
	Needed_permission = retrieve_permission_from_proplist(Command, ?CommandsAndPermissions),
	case Needed_permission of
		?NoPermission ->
			lager:debug("simple_authorization -- Although database is not set, the request ~p does not need any special permission. Access granted.~n", [Request]),
			{?Granted, databaseNotSet};
		Permission ->
			lager:debug("simple_authorization -- Database is not set, and the request ~p needs the following permission: ~p. Access denied.~n", [Request, permission_integer_to_string(Permission)]),
			{?Forbidden, databaseNotSet}
	end;

% this definition ensures that Database will be used in list format
request_permission(Persistence_scheme, Database, Username, {Command, _Args}) when is_atom(Database) ->
	request_permission(Persistence_scheme, atom_to_list(Database), Username, {Command, _Args});

% verifies the database, the Command and the Args, and then returns {?Granted, User_permission_string} if the user indeed has the required permission,
% or {?Forbidden, User_permission_string} otherwise. If any error occurs, it returns {?Forbidden, Error}.
% it's important to mention that, if the current user is the first to request permission to one DB (in other words, the permissions document of
% this DB doesn't exist yet), he/she gets owner access to it. This will not happen to critical databases, such as AuthenticationDatabase or
% AuthorizationDatabase, since they are already created when this module is initialized.
request_permission(Persistence_scheme, Database, Username, Request = {Command, _Args}) ->
	Database_hash = auth_utils:get_hash_as_hex(Database),
	case gen_persistence:process_request(lookup, list_to_atom(?AuthorizationDBName), Database_hash, Persistence_scheme, none) of
		{ok, Permissions_doc} ->
			lager:debug("simple_authorization -- Permissions document of ~p found. Checking if user ~p has access to the request ~p...~n", [Database, Username, Request]),
			Permissions = jsone:decode(list_to_binary(Permissions_doc), [{object_format, proplist}]),
			Needed_permission = retrieve_permission_from_proplist(Command, ?CommandsAndPermissions),
			User_permission = retrieve_permission_from_proplist(list_to_binary(Username), Permissions),
			case User_permission >= Needed_permission of
				true ->
					lager:debug("simple_authorization -- Access granted for user ~p. Needed permission: ~p. User's permission: ~p...~n", [Username, permission_integer_to_string(Needed_permission), User_permission_string = permission_integer_to_string(User_permission)]),
					{?Granted, User_permission_string};
				false ->
					lager:debug("simple_authorization -- Access denied for user ~p. Needed permission: ~p. User's permission: ~p...~n", [Username, Needed_permission_string = permission_integer_to_string(Needed_permission), User_permission_string = permission_integer_to_string(User_permission)]),
					{?Forbidden, "Required: " ++ Needed_permission_string ++ ". Your: " ++ User_permission_string}
			end;
		not_found ->
			lager:debug("simple_authorization -- Permissions document of ~p not found. Creating one, and granting owner permission for user ~p...~n", [Database, Username]),
			New_permission_doc = jsone:encode([{list_to_binary(Username), ?OwnerPermission}]), % New_permission_doc :: binary
			gen_persistence:process_request(save_key, list_to_atom(?AuthorizationDBName), {Database_hash, binary_to_list(New_permission_doc)}, Persistence_scheme, none),
			{?Granted, permission_integer_to_string(?OwnerPermission)};
		Error ->
			lager:debug("simple_authorization -- Access denied. Error while requesting permission. Error: ~p~n", [Error]),
			{?Forbidden, Error}
	end.
%%  {?Granted, none}.


% this definition ensures that Database will be used in list format
grant_permission(Persistence_scheme, Database, {Target_username, Permission_in_API_format}) when is_atom(Database) ->
	grant_permission(Persistence_scheme, atom_to_list(Database), {Target_username, Permission_in_API_format});

% this function grants the permission <Permission_in_API_format> upon database <Database> to the user <Target_username>.
% if the user already had certain permission upon that DB, that permission is updated to the new given permission
% if the permission is granted successfully, the function returns {ok, Permission_as_string}, and {error, Error}, otherwise.
grant_permission(Persistence_scheme, Database, {Target_username, Permission_in_API_format}) ->
	Database_hash = auth_utils:get_hash_as_hex(Database),
	Desired_permission = permission_API_format_to_integer(Permission_in_API_format),
	case gen_persistence:process_request(lookup, list_to_atom(?AuthorizationDBName), Database_hash, Persistence_scheme, none) of
		{ok, Permissions_doc} ->
			lager:debug("simple_authorization -- Permissions document of ~p found. Checking if user ~p already has any permission upon this database...~n", [Database, Target_username]),
			Permissions = jsone:decode(list_to_binary(Permissions_doc), [{object_format, proplist}]),
			Target_username_binary = list_to_binary(Target_username),
			User_permission = proplists:get_value(Target_username_binary, Permissions),
			case User_permission of
				undefined ->
					New_permission_doc = jsone:encode([{Target_username_binary, Desired_permission} | Permissions]), % New_permission_doc :: binary
					gen_persistence:process_request(update, list_to_atom(?AuthorizationDBName), {Database_hash, binary_to_list(New_permission_doc)}, Persistence_scheme, none),
					lager:debug("simple_authorization -- Granted permission ~p to user ~p~n", [Permission_string = permission_integer_to_string(Desired_permission), Target_username]),
					{ok, Permission_string};
				_ ->
					New_permission_doc = jsone:encode(lists:keyreplace(Target_username_binary, 1, Permissions, {Target_username_binary, Desired_permission})), % New_permission_doc :: binary
					gen_persistence:process_request(update, list_to_atom(?AuthorizationDBName), {Database_hash, binary_to_list(New_permission_doc)}, Persistence_scheme, none),
					lager:debug("simple_authorization -- User ~p already has permission ~p. Updating to ~p...~n", [Target_username, permission_integer_to_string(User_permission), Desired_permission_string = permission_integer_to_string(Desired_permission)]),
					{ok, Desired_permission_string}
			end;
		Error ->
			lager:debug("simple_authorization -- Grant failed. Error: ~p~n", [Error]),
			{error, Error}
	end.


% this definition ensures that <Database> will be used in list format
revoke_permission(Persistence_scheme, Database, Username) when is_atom(Database)->
	revoke_permission(Persistence_scheme, atom_to_list(Database), Username);

% revokes the permission that the user <Username> has upon the database <Database>
% if the revocation is successful, this function returns {ok, RevokedPermission},
% and, if some error occurs, it returns {error, Error}
revoke_permission(Persistence_scheme, Database, Username) ->
	Database_hash = auth_utils:get_hash_as_hex(Database),
	case gen_persistence:process_request(lookup, list_to_atom(?AuthorizationDBName), Database_hash, Persistence_scheme, none) of
		{ok, Permissions_doc} ->
			Permissions = jsone:decode(list_to_binary(Permissions_doc), [{object_format, proplist}]),
			Target_username_binary = list_to_binary(Username),
			User_permission = retrieve_permission_from_proplist(Target_username_binary, Permissions),
			New_permission_doc = jsone:encode(proplists:delete(Target_username_binary, Permissions)), % New_permission_doc :: binary
			gen_persistence:process_request(update, list_to_atom(?AuthorizationDBName), {Database_hash, binary_to_list(New_permission_doc)}, Persistence_scheme, none),
			lager:debug("simple_authorization -- Permission (~p) of the user ~p upon ~p revoked successfully...~n", [Permission_string = permission_integer_to_string(User_permission), Username, Database]),
			{ok, "Revoked permission: " ++ Permission_string};
		Error ->
			lager:debug("simple_authorization -- Revocation failed. Error: ~p~n", [Error]),
			{error, Error}
	end.


% this definition ensures that <Database> will be used in list format
show_permission(Persistence_scheme, Database, Username) when is_atom(Database)->
	show_permission(Persistence_scheme, atom_to_list(Database), Username);

% simply shows which permission the user <Username> has upon the database <Database>
% if the fetching is successful, this function returns {ok, Permission_as_string},
% and, if some error occurs, it returns {error, Error}
show_permission(Persistence_scheme, Database, Username) ->
	Database_hash = auth_utils:get_hash_as_hex(Database),
	case gen_persistence:process_request(lookup, list_to_atom(?AuthorizationDBName), Database_hash, Persistence_scheme, none) of
		{ok, Permissions_doc} ->
			Permissions = jsone:decode(list_to_binary(Permissions_doc), [{object_format, proplist}]),
			User_permission = retrieve_permission_from_proplist(list_to_binary(Username), Permissions),
			lager:debug("simple_authorization -- User ~p has the permission ~p upon ~p~n", [Username, Permission_string = permission_integer_to_string(User_permission), Database]),
			{ok, Permission_string};
		Error ->
			lager:debug("simple_authorization -- Error while fetching user's permission. Error: ~p~n", [Error]),
			{error, Error}
	end.


retrieve_permission_from_proplist(Key, Proplist) ->
	case proplists:get_value(Key, Proplist) of
		undefined -> ?NoPermission;
		Permission -> Permission
	end.

% Pretty formats the Perrmission integer (for print purposes only)
permission_integer_to_string(Permission_integer) ->
	case Permission_integer of
		?OwnerPermission -> "OwnerPermission";
		?ReadAndWritePermission -> "ReadAndWritePermission";
		?ReadPermission -> "ReadPermission";
		?NoPermission -> "NoPermission"
	end.

permission_API_format_to_integer(Permission) ->
	case Permission of
		"o" -> ?OwnerPermission;
		"rw" -> ?ReadAndWritePermission;
		"r" -> ?ReadPermission;
		_ -> ?NoPermission
	end.


