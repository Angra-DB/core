-module(gen_authentication).

-export([behaviour_info/1]).

-export([start/2, process_request/5]).

behaviour_info(callbacks) ->
  [{init, 2}, {login, 2}, {logout, 1}].

% everything needed to start the authentication module stays in this function, and inside init/2
start({Authentication_scheme, Authentication_settings}, Persistence_setup) ->
    Authentication_scheme:init(Authentication_settings, Persistence_setup).

% login/2 is expected to process everything it needs to, and then return the tuple {?LoggedIn, #authentication_info}
% if the login is successful, and the tuple {?LoggedOut, none} otherwise
process_request(login, Authentication_scheme, Username, Password, _) ->
    Authentication_scheme:login(Username, Password);

% logout/1 is supposed to process everything it needs to, and then return {?LoggedOut, none}
process_request(logout, Authentication_scheme, _,_, Authentication_status) ->
    Authentication_scheme:logout(Authentication_status).