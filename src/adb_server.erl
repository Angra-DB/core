% ------------------------------------------------
% @author Rodrigo Bonifacio <rbonifacio@unb.br>
%
% @doc a first attempt to build the Angra-DB server
%
% @end
% ------------------------------------------------



-module(adb_server).
-include_lib("eunit/include/eunit.hrl").
-include("gen_auth.hrl").

-behavior(gen_server).

%
% API functions
%

-export([ start_link/2
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

-record(state, {lsock, persistence, parent, current_db = none, auth_scheme, auth_info = ?LoggedOut}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the
%%% API section, calls one of the gen_server library
%%% functions (start_link/4, call/2, cast/2)... This
%%% is a bit trick.
%%%======================================================

start_link(LSock, Persistence) ->
    gen_server:start_link(?MODULE, [LSock, Persistence], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock, Persistence]) ->
    {ok, #state{lsock = LSock, persistence = Persistence}, 0}.

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
    {Logged, _} = adbsecurity:is_logged_in(self(), Socket),

    case Logged of
        logged_in -> evaluate_authenticated_request(Socket, State, Tokens);
        not_logged_in -> evaluate_not_authenticated_request(Socket, State, Tokens)
    end.

evaluate_authenticated_request(Socket, State, Tokens) ->
    case Tokens of
        {"login", _, _} ->
            gen_tcp:send(Socket, io_lib:fwrite("You are already logged in. In order to log in with a different account, use the command 'logout' first.~n"));
        {"logout", _} ->
            adbsecurity:logout(self(), Socket);
        {"connect", Database} ->
            connect(Socket, State, Database);
        {"create_db", Database} ->
            persist(Socket, State#state.persistence, Database, Tokens),
            State;
        _ ->
            persist(Socket, State#state.persistence, State#state.current_db, Tokens),
            State
    end.

evaluate_not_authenticated_request(Socket, State, Tokens) ->
    case Tokens of
        {"login", Username, Password} ->
            adbsecurity:login(self(), Socket);
        {"logout", _} ->
            gen_tcp:send(Socket, io_lib:fwrite("You are already logged out.~n"));
        _ ->
            gen_tcp:send(Socket, io_lib:fwrite("You are not logged in yet. To do so, use the command 'login [user_name] [<password>]'~n"))
    end.


%
% logs in using a given auth scheme (default: adbsecurity)
%
login(Socket, State, Username) ->
    NewState = State#state{ auth_info = adb_auth:login(Username) },
    gen_tcp:send(Socket, io_lib:fwrite("Logged in as ~p...~n", [Username])),
    NewState.

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
    gen_tcp:send(Socket, io_lib:fwrite("Database not set. Please use the 'use' command.~n", []));

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
      _          -> {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "save_key", "lookup", "update", "delete", "connect", "create_db", "delete_db", "query_term", "login", "logout"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.