-module(gen_authentication).

-export([behaviour_info/1]).

-export([start/2, process_request/6]).

behaviour_info(callbacks) ->
  [{init, 2}, {login, 3}, {logout, 1}, {register, 3}].

% everything needed to start the authentication module stays in this function, and inside init/2
start({Authentication_scheme, Authentication_settings}, Persistence_setup) ->
    Authentication_scheme:init(Authentication_settings, Persistence_setup).

% Description: ordinary login function
% Expected returns:
% - {?LoggedIn, #authentication_info}, in case the login is successful
% - {?LoggedOut, ?ErrorInvalidPasswordOrUsername}, in case the login failed because of invalid password/username
% - {?LoggedOut, none [or ErrorMessage]}, if any other failure/error happens while processing login
process_request(login, Authentication_scheme, Username, Password, _, Persistence_scheme) ->
    Authentication_scheme:login(Username, Password, Persistence_scheme);

% Description: ordinary logout function
% Expected returns:
% - {?LoggedOut, none}
process_request(logout, Authentication_scheme, _,_, Authentication_status, _) ->
    Authentication_scheme:logout(Authentication_status);

% Description: ordinary register function
% Expected returns:
% - {ok, _}, in case the registering is successful (example: {ok, DocKey}
% - {error, _}, otherwise (example: {error, ?ErrorUserAlreadyExists}, in case the username used already exists)
process_request(register, Authentication_scheme, Username, Password, _, Persistence_scheme) ->
  Authentication_scheme:register(Username, Password, Persistence_scheme).