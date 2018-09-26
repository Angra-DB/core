-module(gen_auth).

-export([behaviour_info/1]).

-export([start/1, process_request/5]).

behaviour_info(callbacks) ->
  [{login, 2}, {logout, 1}, {is_logged_in, 1}].

start(_Auth_setup) ->  ok.

% login/2 is expected to process everything it needs to, and then return the tuple {?LoggedIn, #authentication_info}
% if the login is successful, and the tuple {?LoggedOut, none} otherwise
process_request(login, Auth_scheme, Username, Password, _) ->
    Auth_scheme:login(Username, Password);

% logout/1 is supposed to process everything it needs to, and then return ?LoggedOut
process_request(logout, Auth_scheme, _,_, Auth_status) ->
    Auth_scheme:logout(Auth_status);

% is_logged_in/1 needs to return either ?LoggedIn or ?LoggedOut
process_request(is_logged_in, Auth_scheme, _,_, Auth_status) ->
    Auth_scheme:is_logged_in(Auth_status).
