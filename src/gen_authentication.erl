-module(gen_auth).

-export([behaviour_info/1]).

-export([start/2, process_request/5]).

behaviour_info(callbacks) ->
  [{init, 2}, {login, 2}, {logout, 1}].

% everything needed to start the auth module stays in this function, and inside init/2
start({Auth_scheme, Auth_settings}, Persistence_setup) ->
    Auth_scheme:init(Auth_settings, Persistence_setup).

% login/2 is expected to process everything it needs to, and then return the tuple {?LoggedIn, #authentication_info}
% if the login is successful, and the tuple {?LoggedOut, none} otherwise
process_request(login, Auth_scheme, Username, Password, _) ->
    Auth_scheme:login(Username, Password);

% logout/1 is supposed to process everything it needs to, and then return {?LoggedOut, none}
process_request(logout, Auth_scheme, _,_, Auth_status) ->
    Auth_scheme:logout(Auth_status).