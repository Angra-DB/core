-module(adbsecurity).
-compile(export_all).

% initializes the authentication/authorization table and other security resources
init() ->
	_ = ets:new(auth_table, [set, private, named_table]),
	{ok}.

is_logged_in(Socket) ->
	case ets:lookup(auth_table, Socket) of
		[Result | _] -> {ok, Result};
		[] -> {not_logged_in, nil}
	end.

login(Socket) ->
	ets:insert_new(auth_table, {Socket, user_info}).

logout(Socket) ->
	ets:delete(auth_table, Socket).