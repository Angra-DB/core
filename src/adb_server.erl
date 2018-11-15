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

-export([ start_link/5
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
% authentication_status (the authentication current status of this specific connection), authorization_setup (the authorization scheme and its configurations),
% communication (if ssl options were set on "adb_core.app", the app will use SSL; thus, this field will store the atom ssl, which will the erlang module used;
% if no ssl options are set, the app will use purely TCP, and this field will contain only gen_tcp, which is the erlang TCP module that will be used)
-record(state, {lsock, persistence_setup, parent, current_db = none, authentication_setup, authentication_status = {?LoggedOut, none}, authorization_setup, communication}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the
%%% API section, calls one of the gen_server library
%%% functions (start_link/4, call/2, cast/2)... This
%%% is a bit trick.
%%%======================================================

start_link(LSock, Persistence, Authentication, Authorization, Communication) ->
    gen_server:start_link(?MODULE, [LSock, Persistence, Authentication, Authorization, Communication], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock, Persistence, Authentication, Authorization, Communication]) ->
    {ok, #state{lsock = LSock, persistence_setup = Persistence, authentication_setup = Authentication, authorization_setup = Authorization, communication = Communication}, 0}.

handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({Protocol, Socket, RawData}, State) when Protocol == tcp; Protocol == ssl ->
    NewState = process_request(Socket, State, RawData),
    {noreply, NewState};

handle_info({Protocol_closed, _Socket}, State) when Protocol_closed == tcp_closed; Protocol_closed == ssl_closed ->
    {stop, normal, State};

% used to initialize this server immediately after the function init/1, as the timeout was set as zero in the tuple returned by it
handle_info(timeout, #state{lsock = LSock, communication = Communication_module} = State) ->
    {ok, Sock} = accept_and_handshake(LSock, Communication_module),
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
%   register <user_name> <user_password>        (depending on the authentication scheme, these parameters might not be needed)
%   login <user_name> <user_password>           (depending on the authentication scheme, these parameters might not be needed)
%   logout
%
%   grant_permission <username> <permission>    (the format of the permission depends on the authorization scheme)
%   revoke_permission <username>
%   show_permission <username>
%%% =========================================================
process_request(Socket, #state{communication = Communication_module} = State, RawData) ->
    try
      Tokens = preprocess(RawData),
      evaluate_request(Socket, State, Tokens)
    catch
      _Class:Err -> Communication_module:send(Socket, io_lib:fwrite("~p~n", [Err]))
    end.

% do the required transport accepts and protocol handshakes if needed, and then return {ok, Socket}
accept_and_handshake(ListenSocket, gen_tcp) ->
    lager:debug("adb_server -- doing transport accept on TCP listen socket. (PID: ~p)~n", [self()]),
    % do a simple transport accept on the TCP listen socket
    {ok, _Socket} = gen_tcp:accept(ListenSocket); % (blocking function)

% do the required transport accepts and protocol handshakes if needed, and then return {ok, Socket}
accept_and_handshake(ListenSocket, ssl) ->
    lager:debug("adb_server -- doing transport accept on SSL listen socket, and then the handshake. (PID: ~p)~n", [self()]),
    % do a transport accept on the SSL listen socket, and then the SSL handshake
    {ok, HsSocket} = ssl:transport_accept(ListenSocket), % (blocking function)
    {ok, _Socket} = ssl:handshake(HsSocket).

evaluate_request(Socket, #state{communication = Communication_module} = State, Tokens) ->
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
                            Communication_module:send(Socket, io_lib:fwrite("For this action to be performed, you first need to connect to a database. Please use the command 'connect [db_name]'.~n", [])),
                            State;
                        _ ->
                            Communication_module:send(Socket, io_lib:fwrite("You are not authorized to perform this action. (Forbiddance info: ~p)~n", [ForbiddanceInfo])),
                            State
                    end
            end;
        ?LoggedOut ->
            evaluate_not_authenticated_request(Socket, State, Tokens)
    end.

evaluate_authenticated_request(Socket, #state{communication = Communication_module} = State, Tokens) ->
    lager:debug("adb_server -- Evaluating authenticated request... Tokens: ~p", [Tokens]),
    case Tokens of
        {"login", _} ->
            Communication_module:send(Socket, "You are already logged in. In order to log in with a different account, use the command 'logout' first.~n"),
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
            persist(Socket, State, Database, Tokens),
            State;
        _ ->
            persist(Socket, State, State#state.current_db, Tokens),
            State
    end.

evaluate_not_authenticated_request(Socket, #state{communication = Communication_module} = State, Tokens) ->
    lager:debug("adb_server -- Evaluating non-authenticated request... Tokens: ~p", [Tokens]),
    case Tokens of
        {"login", {Username, Password}} ->
            login(Socket, State, Username, Password);
        {"logout", _} ->
            Communication_module:send(Socket, io_lib:fwrite("You are already logged out.~n", [])),
            State;
        {"register", {Username, Password}} ->
            register(Socket, State, Username, Password);
        _ ->
            Communication_module:send(Socket, io_lib:fwrite("You are not logged in yet. To do so, use the command 'login [user_name] [password]'. On other hand, to create a new user, use 'register [user_name] [password]' ~n", [])),
            State
    end.

%
% grants some permission for a given user (the args depend on the Authorization scheme)
%
grant_permission(Socket, #state{communication = Communication_module} = State, Grant_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:grant_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Grant_args) of
        {ok, ExtraInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("Permission granted successfully (~p)...~n", [ExtraInfo]));
        {error, FailureInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("Permission could not be granted. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% grants some permission for a given user (the args depend on the Authorization scheme)
%
revoke_permission(Socket, #state{communication = Communication_module} = State, Revoke_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:revoke_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Revoke_args) of
        {ok, ExtraInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("Permission revoked successfully for the specified user. (~p)~n", [ExtraInfo]));
        {error, FailureInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("Permission could not be revoked. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% show the permission of a given user (the args depend on the Authorization scheme)
%
show_permission(Socket, #state{communication = Communication_module} = State, Show_permission_args) ->
    {Authorization_scheme, _Authorization_Settings} = State#state.authorization_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    case gen_authorization:show_permission(Authorization_scheme, Persistence_scheme, State#state.current_db, Show_permission_args) of
        {ok, Permission} ->
            Communication_module:send(Socket, io_lib:fwrite("The permission of the given user upon ~p is ~p.~n", [State#state.current_db, Permission]));
        {error, FailureInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("Permission could not be fetched. Error: ~p~n", [FailureInfo]))
    end,
    State.

%
% logs in using a given auth scheme (default: adb_authentication)
%
login(Socket, #state{communication = Communication_module} = State, Username, Password) ->
    {Authentication_scheme, _Authentication_Settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    lager:debug("adb_server -- User ~p trying to log in... Auth Scheme: ~p. Persistence Scheme: ~p ~n", [Username, Authentication_scheme, Persistence_scheme]),
    LoginResult = gen_authentication:process_request(login, Authentication_scheme, Username, Password, none, Persistence_scheme),
    case LoginResult of
        {?LoggedIn, _AuthenticationInfo} ->
            Communication_module:send(Socket, io_lib:fwrite("User ~p logged in successfully...~n", [Username]));
        {?LoggedOut, FailureInfo} ->
            case FailureInfo of
                ?ErrorInvalidPasswordOrUsername ->
                    Communication_module:send(Socket, io_lib:fwrite("Log in failed. You have entered an invalid username or password... If you would like to create a new user, use the command 'register [user_name] [password]'~n", []));
                Error ->
                    Communication_module:send(Socket, io_lib:fwrite("An internal error occurred while trying to log in. Error: ~p~n", [Error]))
            end
    end,
    NewState = State#state{ authentication_status = LoginResult},
    % {_Status, _AuthInfo} = NewState#state.authentication_status, % for possible future uses...
    NewState.

%
% logs out using a given auth scheme (default: adb_authentication)
%
logout(Socket, #state{communication = Communication_module} = State) ->
    {Authentication_scheme, _Settings} = State#state.authentication_setup,
    % update the following field of the State: current_db (to 'none') and authentication_status (remove the ?LoggedIn status and the previous user's information)
    NewState = State#state{ current_db = none, authentication_status = gen_authentication:process_request(logout, Authentication_scheme, none, none, State#state.authentication_status, none)},
    {_Status, AuthInfo} = State#state.authentication_status,
    lager:debug("adb_server -- User ~p logged out.", [AuthInfo#authentication_info.username]),
    Communication_module:send(Socket, io_lib:fwrite("User logged out...~n", [])),
    NewState.

%
% registers a user with a given auth and persistence scheme
%
register(Socket, #state{communication = Communication_module} = State, Username, Password) ->
    {Authentication_scheme, _Auth_settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence_setup,
    lager:debug("adb_server -- Trying to register user ~p... Auth Scheme: ~p ~n", [Username, Authentication_scheme]),
    case gen_authentication:process_request(register, Authentication_scheme, Username, Password, none, Persistence_scheme) of
        {ok, _} ->
            Communication_module:send(Socket, io_lib:fwrite("User ~p registered...~n", [Username]));
        {error, Error} ->
            Communication_module:send(Socket, io_lib:fwrite("User ~p could not be registered. Error: ~p~n", [Username, Error]))
    end,
    State.

%
% connects to an existing database.
%
connect(Socket, #state{communication = Communication_module} = State, Database) ->
    {Persistence, Settings} = State#state.persistence_setup,
    Res = gen_persistence:process_request(connect, Database, Database, Persistence, Settings),
    case Res of
        db_does_not_exist ->
            Communication_module:send(Socket, io_lib:fwrite("Database ~p does not exist~n", [Database])),
	          State;
	      ok ->
            NewState = State#state{ current_db = list_to_atom(Database) },
            Communication_module:send(Socket, io_lib:fwrite("Database set to ~p...~n", [Database])),
            NewState
    end.


persist(Socket, #state{communication = Communication_module} = State, none, _) ->
    Communication_module:send(Socket, io_lib:fwrite("Database not set. Please use the command 'connect [db_name]'.~n", [])),
    State;

persist(Socket, #state{communication = Communication_module} = State, CurrentDB, {Command, Args}) ->
    {Persistence_scheme, Persistence_settings} = State#state.persistence_setup,
    Res = gen_persistence:process_request(list_to_atom(Command), CurrentDB, Args, Persistence_scheme, Persistence_settings),
    case Res of
        _Response ->
            Communication_module:send(Socket, io_lib:fwrite("~p~n", [_Response]))
    end,
    State.

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