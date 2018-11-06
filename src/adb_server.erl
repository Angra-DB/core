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

-behavior(gen_server).

%
% API functions
%

-export([ start_link/3
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

% persistence (the persistence setup and its configurations), authentication_setup (the authentication scheme and its configurations),
% authentication_status (the authentication current status of this specific connection)
-record(state, {lsock, persistence, parent, current_db = none, authentication_setup, authentication_status = {?LoggedOut, none}}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the
%%% API section, calls one of the gen_server library
%%% functions (start_link/4, call/2, cast/2)... This
%%% is a bit trick.
%%%======================================================

start_link(LSock, Persistence, Authentication) ->
    gen_server:start_link(?MODULE, [LSock, Persistence, Authentication], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock, Persistence, Authentication]) ->
    {ok, #state{lsock = LSock, persistence = Persistence, authentication_setup = Authentication}, 0}.

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
%   login <user_name> <<user_password>>
%   logout
%%% =========================================================
process_request(Socket, State, RawData) ->
    try
      Tokens = preprocess(RawData),
      evaluate_request(Socket, State, Tokens)
    catch
      _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))
    end.

evaluate_request(Socket, State, Tokens) ->
    {Logged, _UserAuthStatus} = State#state.authentication_status,

    case Logged of
        ?LoggedIn -> evaluate_authenticated_request(Socket, State, Tokens);
        ?LoggedOut -> evaluate_not_authenticated_request(Socket, State, Tokens)
    end.

evaluate_authenticated_request(Socket, State, Tokens) ->
    lager:debug("adb_server -- Evaluating authenticated request... Tokens: ~p", [Tokens]),
    case Tokens of
        {"login", _} ->
            gen_tcp:send(Socket, "You are already logged in. In order to log in with a different account, use the command 'logout' first.~n"),
            State;
        {"logout", _} ->
            logout(Socket, State);
        {"connect", Database} ->
            connect(Socket, State, Database);
        {"register", {Username, Password}} ->
            register(Socket, State, Username, Password);
        {"create_db", Database} ->
            persist(Socket, State#state.persistence, Database, Tokens),
            State;
        _ ->
            persist(Socket, State#state.persistence, State#state.current_db, Tokens),
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
            gen_tcp:send(Socket, io_lib:fwrite("You are not logged in yet. To do so, use the command 'login [user_name] [password]'~n", [])),
            State
    end.


%
% logs in using a given auth scheme (default: adb_authentication)
%
login(Socket, State, Username, Password) ->
    {Auth_scheme, _Settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence,
    lager:debug("adb_server -- User ~p trying to log in... Auth Scheme: ~p. Persistence Scheme: ~p ~n", [Username, Auth_scheme, Persistence_scheme]),
    LoginResult = gen_authentication:process_request(login, Auth_scheme, Username, Password, none, Persistence_scheme),
    case LoginResult of
        {?LoggedIn, _AuthenticationInfo} ->
            gen_tcp:send(Socket, io_lib:fwrite("User ~p logged in successfully...~n", [Username]));
        {?LoggedOut, FailureInfo} ->
            case FailureInfo of
                ?ErrorInvalidPasswordOrUsername ->
                    gen_tcp:send(Socket, io_lib:fwrite("Log in failed. You have entered an invalid username or password...~n", []));
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
    {Auth_scheme, _Settings} = State#state.authentication_setup,
    NewState = State#state{ authentication_status = gen_authentication:process_request(logout, Auth_scheme, none, none, State#state.authentication_status, none)},
    {_Status, AuthInfo} = State#state.authentication_status,
    lager:debug("adb_server -- User ~p logged out.", [AuthInfo#authentication_info.username]),
    gen_tcp:send(Socket, io_lib:fwrite("User logged out...~n", [])),
    NewState.

%
% registers a user with a given auth and persistence scheme
%
register(Socket, State, Username, Password) ->
    {Auth_scheme, _Auth_settings} = State#state.authentication_setup,
    {Persistence_scheme, _Persistence_settings} = State#state.persistence,
    lager:debug("adb_server -- Trying to register user ~p... Auth Scheme: ~p ~n", [Username, Auth_scheme]),
    case gen_authentication:process_request(register, Auth_scheme, Username, Password, none, Persistence_scheme) of
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
    {Persistence, Settings} = State#state.persistence,
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
      _          -> {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "save_key", "lookup", "update", "delete", "connect", "create_db", "delete_db", "query_term", "login", "logout", "register"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.