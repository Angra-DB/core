% ------------------------------------------------
% @author Rodrigo Bonifacio <rbonifacio@unb.br>
%
% @doc a first attempt to build the Angra-DB server
%
% @end
% ------------------------------------------------



-module(adb_server).
-include_lib("eunit/include/eunit.hrl").
-include("gen_authentication.hrl").
-include("gen_authorization.hrl").

-behavior(gen_server).

%
% API functions
%

-export([ start_link/4
, get_count/0   % we can understand both get_count and stop as adm operations
, stop/0]).

% gen_server callbacks
-export([ init/1
, handle_call/3
, handle_cast/2
, handle_info/2
, terminate/2
, code_change/3]).

-export([split/1]).

-define(SERVER, ?MODULE).      % declares a SERVER macro constant (?MODULE is the module's name)

% persistence_setup (the persistence scheme and its configurations), authentication_setup (the authentication scheme and its configurations),
% authentication_status (the authentication current status of this specific connection), authorization_setup (the authorization scheme and its configurations)
-record(state, {lsock, persistence_setup, parent, current_db = none, authentication_setup, authentication_status = {?LoggedOut, none}, authorization_setup}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the
%%% API section, calls one of the gen_server library
%%% functions (start_link/4, call/2, cast/2)... This
%%% is a bit trick.
%%%======================================================

start_link(LSock, Persistence, Authentication, Authorization) ->
    gen_server:start_link(?MODULE, [LSock, Persistence, Authentication, Authorization], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock, Persistence, Authentication, Authorization]) ->
    {ok, #state{lsock = LSock, persistence_setup = Persistence, authentication_setup = Authentication, authorization_setup = Authorization}, 0}.

handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({tcp, Socket, RawData}, State) ->
    NewState = process_request(Socket, State, RawData),
    {noreply, NewState};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(timeout, #state{lsock = LSock} = State) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    server_sup:start_child(),
    {noreply, State#state{ lsock = Sock }}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% ==========================================================
% process the TCP requests. here, we accept the following
% commands:
%
%   create_db <DBName>
%   delete_db <DBName>
%   connect <DBName>
%
%   save <Document>
%   save_key <given_Id> <document>
%   lookup <Id>
%   update <Id> <Document>
%   delete <Id>
%   query_term
%
%   login <user_name> <user_password>
%   logout
%
%   grant_permission <username> <permission>     (the format of the permission depends on the Authorization scheme)
%   revoke_permission <username>
%   show_permission <username>
%%% =========================================================
process_request(Socket, State, RawData) ->
    try
      Tokens = preprocess(RawData),
      evaluate_request(Socket, State, Tokens)
    catch
      _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))
    end.

evaluate_request(Socket, State, Tokens) ->
    {Logged, User_auth_info} = State#state.authentication_status,
    {Authorization_scheme, _Authorization_settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,

    case Logged of
        ?LoggedIn ->
            case gen_authorization:request_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, User_auth_info#authentication_info.username, Tokens) of
                {?Granted, _} ->
                    evaluate_authenticated_request(Socket, State, Tokens);
                {?Forbidden, ForbiddanceInfo} ->
                    case ForbiddanceInfo of
                        databaseNotSet ->
                            gen_tcp:send(Socket, io_lib:fwrite("For this action to be performed, you first need to connect to a database. Please use the command 'connect [db_name]'.~n", [])),
                            State;
                        _ ->
                            gen_tcp:send(Socket, io_lib:fwrite("You are not authorized to perform this action. (Forbiddance info: ~p)~n", [ForbiddanceInfo])),
                            State
                    end
            end;
        ?LoggedOut ->
            evaluate_not_authenticated_request(Socket, State, Tokens)
    end.

evaluate_authenticated_request(Socket, State, Tokens) ->
    lager:debug("adb_server -- Evaluating authenticated request... Tokens: ~p", [Tokens]),
    case Tokens of
        {"login", _} ->
            gen_tcp:send(Socket, "You are already logged in. In order to log in with a different account, use the command 'logout' first.~n"),
            State;
        {"logout", _} ->
            logout(Socket, State);
        {"grant_permission", Grant_args} ->
            grant_permission(Socket, State, Grant_args);
        {"revoke_permission", Revoke_args} ->
            revoke_permission(Socket, State, Revoke_args);
        {"show_permission", Show_permission_args} ->
            show_permission(Socket, State, Show_permission_args);
        {"connect", Database} ->
            connect(Socket, State, Database);
        {"register", {Username, Password}} ->
            register(Socket, State, Username, Password);
        {"create_db", Database} ->
            persist(Socket, State#state.persistence_setup, Database, Tokens),
            State;
        _ ->
            persist(Socket, State#state.persistence_setup, State#state.current_db, Tokens),
            State
    end.

evaluate_not_authenticated_request(Socket, State, Tokens) ->
    lager:debug("adb_server -- Evaluating non-authenticated request... Tokens: ~p", [Tokens]),
    case Tokens of
        {"login", {Username, Password}} ->
            login(Socket, State, Username, Password);
        {"logout", _} ->
            gen_tcp:send(Socket, io_lib:fwrite("You are already logged out.~n", [])),
            State;
        {"register", {Username, Password}} ->
            register(Socket, State, Username, Password);
        _ ->
            gen_tcp:send(Socket, io_lib:fwrite("You are not logged in yet. To do so, use the command 'login [user_name] [password]'. On other hand, to create a new user, use 'register [user_name] [password]' ~n", [])),
            State
    end.

%
% grants some permission for a given user (the args depend on the Authorization scheme)
%
grant_permission(Socket, State, Grant_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:grant_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Grant_args) of
        {ok, ExtraInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("Permission granted successfully (~p)...~n", [ExtraInfo]));
        {error, FailureInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("Permission could not be granted. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% grants some permission for a given user (the args depend on the Authorization scheme)
%
revoke_permission(Socket, State, Revoke_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:revoke_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Revoke_args) of
        {ok, ExtraInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("Permission revoked successfully for the specified user. (~p)~n", [ExtraInfo]));
        {error, FailureInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("Permission could not be revoked. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% show the permission of a given user (the args depend on the Authorization scheme)
%
show_permission(Socket, State, Show_permission_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:show_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Show_permission_args) of
        {ok, Permission} ->
            gen_tcp:send(Socket, io_lib:fwrite("The permission of the given user upon ~p is ~p.~n", [State#state.current_db, Permission]));
        {error, FailureInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("Permission could not be fetched. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% logs in using a given auth scheme (default: adb_authentication)
%
login(Socket, State, Username, Password) ->
    {Authentication_scheme, _Authentication_Settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    lager:debug("adb_server -- User ~p trying to log in... Auth Scheme: ~p. Persistence Scheme: ~p ~n", [Username, Authentication_scheme, Persistence_scheme]),
    LoginResult = gen_authentication:process_request(login, Authentication_scheme, Username, Password, none, Persistence_scheme),
    case LoginResult of
        {?LoggedIn, _AuthenticationInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("User ~p logged in successfully...~n", [Username]));
        {?LoggedOut, FailureInfo} ->
            case FailureInfo of
                ?ErrorInvalidPasswordOrUsername ->
                    gen_tcp:send(Socket, io_lib:fwrite("Log in failed. You have entered an invalid username or password... If you would like to create a new user, use the command 'register [user_name] [password]'~n", []));
                Error ->
                    gen_tcp:send(Socket, io_lib:fwrite("An internal error occurred while trying to log in. Error: ~p~n", [Error]))
            end
    end,
    NewState = State#state{ authentication_status = LoginResult},
    % {_Status, _AuthInfo} = NewState#state.authentication_status, % for possible future uses...
    NewState.

%
% logs out using a given auth scheme (default: adb_authentication)
%
logout(Socket, State) ->
    {Authentication_scheme, _Settings} = State#state.authentication_setup,
    % update the following field of the State: current_db (to 'none') and authentication_status (remove the ?LoggedIn status and the previous user's information)
    NewState = State#state{ current_db = none, authentication_status = gen_authentication:process_request(logout, Authentication_scheme, none, none, State#state.authentication_status, none)},
    {_Status, AuthInfo} = State#state.authentication_status,
    lager:debug("adb_server -- User ~p logged out.", [AuthInfo#authentication_info.username]),
    gen_tcp:send(Socket, io_lib:fwrite("User logged out...~n", [])),
    NewState.

%
% registers a user with a given auth and persistence scheme
%
register(Socket, State, Username, Password) ->
    {Authentication_scheme, _Auth_settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    lager:debug("adb_server -- Trying to register user ~p... Auth Scheme: ~p ~n", [Username, Authentication_scheme]),
    case gen_authentication:process_request(register, Authentication_scheme, Username, Password, none, Persistence_scheme) of
        {ok, _} ->
            gen_tcp:send(Socket, io_lib:fwrite("User ~p registered...~n", [Username]));
        {error, Error} ->
            gen_tcp:send(Socket, io_lib:fwrite("User ~p could not be registered. Error: ~p~n", [Username, Error]))
    end,
    State.

%
% connects to an existing database.
%
connect(Socket, State, Database) ->
    {Persistence, Settings} = State#state.persistence_setup,
    Res = gen_persistence:process_request(connect, Database, Database, Persistence, Settings),
    case Res of
        db_does_not_exist ->
            gen_tcp:send(Socket, io_lib:fwrite("Database ~p does not exist~n", [Database])),
	    State;
	ok ->
            NewState = State#state{ current_db = list_to_atom(Database) },
            gen_tcp:send(Socket, io_lib:fwrite("Database set to ~p...~n", [Database])),
            NewState
    end.


persist(Socket, _, none, _) ->
    gen_tcp:send(Socket, io_lib:fwrite("Database not set. Please use the command 'connect [db_name]'.~n", []));

persist(Socket, {Persistence, Settings}, CurrentDB, {Command, Args}) ->
    Res = gen_persistence:process_request(list_to_atom(Command), CurrentDB, Args, Persistence, Settings),
    case Res of
        _Response ->
            gen_tcp:send(Socket, io_lib:fwrite("~p~n", [_Response]))
    end.

preprocess(RawData) ->
    _reverse = lists:reverse(RawData),
    Pred = fun(C) -> (C == $\n) or (C == $\r) end,
    _trim = lists:reverse(lists:dropwhile(Pred, _reverse)),
    {Command, Args} = split(_trim),
    case filter_command(Command) of
      []         -> throw(invalid_command);
      ["update"] -> {Command, split(Args)};
			["save_key"] -> {Command, split(Args)};
			["login"] -> {Command, split(Args)};
			["register"] -> {Command, split(Args)};
			["grant_permission"] -> {Command, split(Args)};
      _          -> {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "save_key", "lookup", "update", "delete", "connect", "create_db", "delete_db", "query_term", "login", "logout", "register", "grant_permission", "revoke_permission", "show_permission"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.