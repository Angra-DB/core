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
-define(QUERY, "query").
-define(QUERY_TERM, "query_term").
-define(BULK_LOOKUP, "bulk_lookup").
-define(VALID_COMMANDS, [?SAVE_CMD, ?SAVE_KEY_CMD, ?LOOKUP_CMD, ?UPDATE_CMD, ?DELETE_CMD, ?CONNECT_CMD, ?CREATE_DB_CMD, ?DELETE_DB_CMD, ?QUERY, ?QUERY_TERM, ?BULK_LOOKUP]).

-record(state, {lsock, parent, current_db = none, waiting_request = false, body = {}}). % a record for keeping the server state

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

handle_info({tcp, Socket, RawData}, State = #state{ waiting_request = true, body = Request}) ->
    {NewRequest, Concatenated, Size} = update_request_data(Request, string:chomp(RawData)),
    case list_to_integer(Size) > length(Concatenated) of
        true -> 
            {noreply, State#state{waiting_request = true, body = NewRequest}};
        _ ->
            NewState = process_request(Socket, State, NewRequest),
            {noreply, NewState#state{ waiting_request = false }}
    end;

handle_info({tcp, Socket, RawData}, State) ->
    case preprocess_input(RawData) of
        {wait, Request} ->
            {noreply, State#state{waiting_request = true, body = Request}};
        Tokens ->
            NewState = process_request(Socket, State, Tokens),
            {noreply, NewState#state{ waiting_request = false }}
    end;
    

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

process_request(Socket, State, Tokens) ->
    try
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
		{error, db_does_not_exist} ->
			send_response(Socket, "Database ~p does not exist~n", [Database]),
			State;
		{ok, _} -> NewState = State#state{ current_db = Database },
			send_response(Socket, "Database set to ~p...~n", [Database]),
			NewState
	end.
	
create_db(Socket, Database) ->
	Response = adb_dist_server:forward_request(create_db, Database),
	send_response(Socket, "~p~n", [Response]).
	
persist(Socket, none, _) ->
	send_response(Socket, "Database not set. Please use the 'use' command.~n", []);
persist(Socket, CurrentDB, {Command, Args}) ->
	Response = adb_dist_server:forward_request(list_to_atom(Command), {CurrentDB, Args}),
	send_response(Socket, "~p~n", [Response]).

preprocess_input(RawData) ->
	ChompedData = string:chomp(RawData),
	{Command, Args} = split_next_token(ChompedData),
	case {lists:member(Command, ?VALID_COMMANDS), Command} of
		{false, _} -> 
			{invalid, Command};
		{_, C} when C =:= ?SAVE_KEY_CMD; C =:= ?UPDATE_CMD -> 
			{Key, DocInfo} = split_next_token(Args),
			get_doc_arguments(DocInfo, fun({Size, Doc}) -> {Command, {Key, Size, Doc}} end);
		{_, ?SAVE_CMD} ->
			get_doc_arguments(Args, fun({Size, Doc}) -> {Command, {Size, Doc}} end);
		{_, _} -> 
			{Command, Args}
	end.

split_next_token(Str) ->
	Trim = string:trim(Str, both, " "),
	[Head|Tail] = string:split(Trim, " "),
	{Head, string:trim(Tail)}.

get_doc_arguments(Args, Map) ->
	Split = {Size, Doc} = split(Args),
	case list_to_integer(Size) > length(Doc) of
			true ->
				{wait, Map(Split)};
			false ->
				Map(Split)
	end.

send_response(Socket, Format, Arg) when is_list(Format) ->
	F = lists:flatten(io_lib:fwrite(Format, Args)),
	gen_tcp:send(Socket, io_lib:fwrite("~p ~s", [length(F), F])),

update_request_data({Command, {Key, Size, Data}}, NewData) ->
	Concatenated = Data++NewData,
	{{Command, {Key, Size, Concatenated}}, Concatenated, Size };
	
update_request_data({Command, {Size, Data}}, NewData) ->
	Concatenated = Data++NewData,
	{{Command, {Size, Concatenated}}, Concatenated, Size}.