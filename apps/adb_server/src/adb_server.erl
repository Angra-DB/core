% ------------------------------------------------
% @author Rodrigo Bonifacio <rbonifacio@unb.br>
%
% @doc a first attempt to build the Angra-DB server
%
% @end
% ------------------------------------------------



-module(adb_server).
-include_lib("eunit/include/eunit.hrl").

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

-record(state, {lsock, persistence, parent, current_db = none}). % a record for keeping the server state

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
    adb_server_sup:start_child(),
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
%%% =========================================================
process_request(Socket, State, RawData) ->
    try
      Tokens = preprocess(RawData),
      evaluate_request(Socket, State, Tokens)
    catch
      _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))
    end.

evaluate_request(Socket, State, Tokens) ->
    case Tokens of
        {"connect", Database}   -> connect(Socket, State, Database);
        {"create_db", Database} -> persist(Socket, State#state.persistence, Database, Tokens),
                                   State;
        _                       -> persist(Socket, State#state.persistence, State#state.current_db, Tokens),
                                   State
    end.

%
% connects to an existing database.
%
connect(Socket, State, Database) ->
    Persistence = State#state.persistence,
    Res = forward(process_request, [connect, Database, Database, Persistence]),
    case Res of
        db_does_not_exist -> gen_tcp:send(Socket, io_lib:fwrite("Database ~p does not exist~n", [Database])),
                             State;
        ok                -> NewState = State#state{ current_db = list_to_atom(Database) },
                             gen_tcp:send(Socket, io_lib:fwrite("Database set to ~p...~n", [Database])),
                             NewState
    end.


persist(Socket, _, none, _) ->
    gen_tcp:send(Socket, io_lib:fwrite("Database not set. Please use the 'use' command.~n", []));
persist(Socket, Persistence, CurrentDB, {Command, Args}) ->
    Res = forward(process_request, [list_to_atom(Command), CurrentDB, Args, Persistence]),
    gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Res])).

preprocess(RawData) ->
    Reverse = lists:reverse(RawData),
    Pred = fun(C) -> (C == $\n) or (C == $\r) end,
    Trim = lists:reverse(lists:dropwhile(Pred, Reverse)),
    {Command, Args} = split(Trim),
    case filter_command(Command) of
        []           -> throw(invalid_command);
        ["update"]   -> {Command, split(Args)};
        ["save_key"] -> {Command, split(Args)};
        _            -> {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "save_key", "lookup", "update", "delete", "connect", "create_db", "delete_db"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.


forward(process_request, Args) when node() == nonode@nohost ->
    gen_server:call(adb_core, {process_request, Args});
forward(process_request, Args) ->
    case nodes() of
        [] -> no_core_node_found;
        _  -> rpc:multicall(nodes(), adb_core, receive_request, Args)
    end.
