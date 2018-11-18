% ------------------------------------------------
% @author Rodrigo Bonifacio <rbonifacio@unb.br>
%
% @doc a first attempt to build the Angra-DB server
%
% @end
% ------------------------------------------------

-module(adb_server).

-behavior(gen_server).

%
% API functions
%

-export([ start_link/1
	, get_count/0   % we can understand both get_count and stop as adm operations
    , stop/0]).

% gen_server callbacks
-export([ init/1
	, handle_call/3
	, handle_cast/2
	, handle_info/2
	, terminate/2
	, code_change/3]).

-export([split_next_token/1]).

-define(SERVER, ?MODULE).      % declares a SERVER macro constant (?MODULE is the module's name)

%% Declares all commands available.
-define(SAVE_CMD, "save").
-define(SAVE_KEY_CMD, "save_key").
-define(LOOKUP_CMD, "lookup").
-define(UPDATE_CMD, "update").
-define(DELETE_CMD, "delete").
-define(CONNECT_CMD, "connect").
-define(CREATE_DB_CMD, "create_db").
-define(DELETE_DB_CMD, "delete_db").
-define(VALID_COMMANDS, [?SAVE_CMD, ?SAVE_KEY_CMD, ?LOOKUP_CMD, ?UPDATE_CMD, ?DELETE_CMD, ?CONNECT_CMD, ?CREATE_DB_CMD, ?DELETE_DB_CMD]).

-record(state, {lsock, parent, current_db = none}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the
%%% API section, calls one of the gen_server library
%%% functions (start_link/4, call/2, cast/2)... This
%%% is a bit trick.
%%%======================================================

start_link(LSock) ->
    gen_server:start_link(?MODULE, [LSock], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock]) ->
    {ok, #state{lsock = LSock}, 0}.

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
      Tokens = preprocess_input(RawData),
      evaluate_request(Socket, State, Tokens)
    catch
      _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))
    end.

evaluate_request(Socket, State, Tokens) ->
    case Tokens of
        {invalid, Command}         -> gen_tcp:send(Socket, io_lib:fwrite("The provided command '~p' is invalid.~n", [Command])),
                                      State;
        {?CONNECT_CMD, Database}   -> connect(Socket, State, Database);
        {?CREATE_DB_CMD, Database} -> create_db(Socket, Database),
                                      State;
        _                          -> persist(Socket, State#state.current_db, Tokens),
                                      State
    end.

%
% connects to an existing database.
%
connect(Socket, State, Database) ->
    case adb_dist_server:forward_request(connect, Database) of
        {error, db_does_not_exist} -> gen_tcp:send(Socket, io_lib:fwrite("Database ~p does not exist.~n", [Database])),
                                      State;
        {ok, _}                    -> NewState = State#state{ current_db = Database },
                                      gen_tcp:send(Socket, io_lib:fwrite("Database set to ~p...~n", [Database])),
                                      NewState
    end.

create_db(Socket, Database) ->
    Response = adb_dist_server:forward_request(create_db, Database),
    gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Response])).

persist(Socket, none, _) ->
    gen_tcp:send(Socket, io_lib:fwrite("Database not set. Please use the 'use' command.~n", []));
persist(Socket, CurrentDB, {Command, Args}) ->
    Response = adb_dist_server:forward_request(list_to_atom(Command), {CurrentDB, Args}),
    gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Response])).

preprocess_input(RawData) ->
    ChompedData = string:chomp(RawData),
    {Command, Args} = split_next_token(ChompedData),
    case {lists:member(Command, ?VALID_COMMANDS), Command} of
        {false, _}         -> {invalid, Command};
        {_, ?UPDATE_CMD}   -> {Command, split_next_token(Args)};
        {_, ?SAVE_KEY_CMD} -> {Command, split_next_token(Args)};
        {_, _}             -> {Command, Args}
    end.

split_next_token(Str) ->
    Trim = string:trim(Str, both, " "),
    [Head|Tail] = string:split(Trim, " "),
    {Head, string:trim(Tail)}.