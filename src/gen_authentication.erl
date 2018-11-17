-module(gen_authentication).

-export([behaviour_info/1]).

-export([start/2, login/4, logout/2, register/4]).

behaviour_info(callbacks) ->
  [{init, 2}, {handle_login, 3}, {handle_logout, 1}, {handle_register, 3}].


%%%%%%%%% IMPORTANT
% . If your module is storing authentication information inside AngraDB, in a common database, it is important to
% use ?AuthenticationDBName as name. This way, this DB can be treated as a critical DB (by having different authorization
% treatments and so on)
%%%%%%%%% IMPORTANT

% everything needed to start the authentication module stays in this function, and inside init/2
start({Authentication_scheme, Authentication_settings}, Persistence_setup) ->
    Authentication_scheme:init(Authentication_settings, Persistence_setup).

% Description: ordinary login function
% LoginArgs: a tuple with two elements that were passed after the 'login' command. One authentication module might use
% both elements on its implementation to store, for example, a username and a password.
% Expected returns:
% - {?LoggedIn, #authentication_info}, in case the login is successful
% - {?LoggedOut, ?ErrorInvalidPasswordOrUsername}, in case the login failed because of invalid password/username
% - {?LoggedOut, none [or ErrorMessage]}, if any other failure/error happens while processing login
login(LoginArgs, Authentication_scheme, Persistence_scheme, Socket) ->
    Authentication_scheme:handle_login(LoginArgs, Persistence_scheme, Socket).

% Description: ordinary logout function
% Expected returns:
% - {?LoggedOut, none}
logout(Authentication_scheme, Authentication_status) ->
    Authentication_scheme:handle_logout(Authentication_status).

% Description: ordinary register function
% RegisterArgs: a tuple with two elements that were passed after the 'register' command. One authentication module might use
% both elements on its implementation to store, for example, a username and a password.
% Expected returns:
% - {ok, _}, in case the registering is successful (example: {ok, DocKey}
% - {error, _}, otherwise (example: {error, ?ErrorUserAlreadyExists}, in case the username used already exists)
register(RegisterArgs, Authentication_scheme, Persistence_scheme, Socket) ->
  Authentication_scheme:handle_register(RegisterArgs, Persistence_scheme, Socket).



% command, Authentication_scheme, CommandArgs, Authentication_status, Persistence_scheme