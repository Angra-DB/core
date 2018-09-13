-module(gen_auth).

-export([behaviour_info/1]).

-export([start/1, process_request/5]).

behaviour_info(callbacks) ->
  [{login, 2}, {logout, 1}, {is_logged_in, 1}].

start(_Auth_scheme) ->  ok.

process_request(login, Auth_scheme, Username, Password, _) ->
    Auth_scheme:login(Username, Password);

process_request(logout, Auth_scheme, _,_, Auth_info) ->
    Auth_scheme:logout(Auth_info);

process_request(is_logged_in, Auth_scheme, _,_, Auth_info) ->
    Auth_scheme:is_logged_in(Auth_info).
