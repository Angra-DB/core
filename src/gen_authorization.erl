-module(gen_authorization).

-export([behaviour_info/1]).

-export([
  start/2,
  request_permission/5,
  grant_permission/4,
  revoke_permission/4,
  show_permission/4
]).

behaviour_info(callbacks) ->
  [{init, 2}, {request_permission, 4}].

% everything needed to start the authentication module stays in this function, and inside init/2
start({Authorization_scheme, Authorization_settings}, Persistence_setup) ->
  Authorization_scheme:init(Authorization_settings, Persistence_setup).

% Description: confirms whether the current user has permission to make action or not
% Expected returns:
% - {?Granted, _ExtraInfo}, in case the permission was granted
% - {?Forbidden, _ExtraInfo}, in case the permission was denied
request_permission(Authorization_scheme, Persistence_scheme, Database, Username, {Command, Args}) ->
  Authorization_scheme:request_permission(Persistence_scheme, Database, Username, {Command, Args}).

% Description: grants some permission to a user (both desired user and permission are inside Grant_args, among other desired args)
% Expected returns:
% - {ok, _ExtraInfo}, in case the process was successful
% - {error, ErrorInfo}, otherwise
grant_permission(Authorization_scheme, Persistence_scheme, Database, Grant_args) ->
  Authorization_scheme:grant_permission(Persistence_scheme, Database, Grant_args).

% Description: revokes permission of a user (the username and other desired args are inside Revoke_args parameter)
% Expected returns:
% - {ok, _ExtraInfo}, in case the process was successful
% - {error, ErrorInfo}, otherwise
revoke_permission(Authorization_scheme, Persistence_scheme, Database, Revoke_args) ->
  Authorization_scheme:revoke_permission(Persistence_scheme, Database, Revoke_args).

% Description: shows the permission of an user (the username and other desired args are inside Args parameter)
% Expected returns:
% - {ok, Permission}, in case the process was successful
% - {error, ErrorInfo}, otherwise
show_permission(Authorization_scheme, Persistence_scheme, Database, Args) ->
  Authorization_scheme:show_permission(Persistence_scheme, Database, Args).