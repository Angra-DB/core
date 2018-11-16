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

-record(state, {lsock, persistence, parent, current_db = none, waiting_request = false, body = {} }). % a record for keeping the server state

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

handle_info({tcp, Socket, RawData}, State = #state{ waiting_request = true, body = Request}) ->
    {NewRequest, Concatenated, Size} = update_request_data(Request, clean_request(RawData)),
    case list_to_integer(Size) > length(Concatenated) of
        true -> 
            {noreply, State#state{waiting_request = true, body = NewRequest}};
        _ ->
            NewState = process_request(Socket, State, NewRequest),
            {noreply, NewState#state{ waiting_request = false }}
    end;

handle_info({tcp, Socket, RawData}, State) ->
    case preprocess(RawData) of
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
%%% =========================================================
process_request(Socket, State, Tokens) ->
    evaluate_request(Socket, State, Tokens).
    % try
      
    % catch
    %   _Class:Err ->
    %       gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err])),
    %       State
    % end.

evaluate_request(Socket, State, Tokens) ->
    case Tokens of
        {"connect", Database} ->
            connect(Socket, State, Database);
        {"create_db", Database} ->
            persist(Socket, State#state.persistence, Database, Tokens),
            State;
        _ ->
            persist(Socket, State#state.persistence, State#state.current_db, Tokens),
            State
    end.

%
% connects to an existing database.
%
connect(Socket, State, Database) ->
    {Persistence, Settings} = State#state.persistence,
    Res = gen_persistence:process_request(connect, Database, Database, Persistence, Settings),
    case Res of
        db_does_not_exist ->
            F = lists:flatten(io_lib:fwrite("Database ~p does not exist~n", [Database])),
            gen_tcp:send(Socket, io_lib:fwrite("~p ~s", [length(F), F])),
	        State;
	    ok ->
            NewState = State#state{ current_db = list_to_atom(Database) },
            F = lists:flatten(io_lib:fwrite("Database set to ~p...~n", [Database])),
            gen_tcp:send(Socket, io_lib:fwrite("~p ~s", [length(F), F])),
            NewState
    end.

persist(Socket, _, none, _) ->
    F = lists:flatten(io_lib:fwrite("Database not set. Please use the 'use' command.~n", [])),
    gen_tcp:send(Socket, io_lib:fwrite("~p ~s", [length(F), F]));

persist(Socket, {Persistence, Settings}, CurrentDB, {Command, Args}) ->
    Res = gen_persistence:process_request(list_to_atom(Command), CurrentDB, Args, Persistence, Settings),
    case Res of
        _Response ->
            F = lists:flatten(io_lib:fwrite("~p~n", [_Response])),
            gen_tcp:send(Socket, io_lib:fwrite("~p ~s", [length(F), F]))
    end.

clean_request(RawData) ->
    _reverse = lists:reverse(RawData),
    Pred = fun(C) -> (C == $\n) or (C == $\r) end,
    _trim = lists:reverse(lists:dropwhile(Pred, _reverse)).

preprocess(RawData) ->
    Cleaned = clean_request(RawData),
    lager:info(RawData),
    {Command, Args} = split(Cleaned),
    case filter_command(Command) of
        [] -> 
            throw(invalid_command);
	    [C] when C =:= "save_key"; C =:= "update" -> 
            {Key, DocInfo} = split(Args),
            get_doc_arguments(DocInfo, fun({Size, Doc}) -> {Command, {Key, Size, Doc}} end);
        ["save"] ->
            get_doc_arguments(Args, fun({Size, Doc}) -> {Command, {Size, Doc}} end);
        _ -> 
            {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "save_key", "lookup", "update", "delete", "connect", "create_db", "delete_db", "query_term", "query", "bulk_lookup"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.

get_doc_arguments(Args, Map) ->
    Split = {Size, Doc} = split(Args),
    case list_to_integer(Size) > length(Doc) of
        true ->
            {wait, Map(Split)};
        false ->
            Map(Split)
    end.

update_request_data({Command, {Key, Size, Data}}, NewData) ->
    Concatenated = Data++NewData,
    {{Command, {Key, Size, Concatenated}}, Concatenated, Size };

update_request_data({Command, {Size, Data}}, NewData) ->
    Concatenated = Data++NewData,
    {{Command, {Size, Concatenated}}, Concatenated, Size}.