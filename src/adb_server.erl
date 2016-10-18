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


-export([split/1]).


-define(SERVER, ?MODULE).      % declares a SERVER macro constant (?MODULE is the module's name) 

-record(state, {lsock, request_count = 0}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the 
%%% API section, calls one of the gen_server library 
%%% functions (start_link/4, call/2, cast/2)... This 
%%% is a bit trick. 
%%%======================================================

start_link(LSock) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [LSock], []).
			  
get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop). 

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([LSock]) ->
    {ok, #state{lsock = LSock}, 0}.

handle_call(get_count, _From, State) ->
     {reply, {ok, State#state.request_count}, State};
handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}. 

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({tcp, Socket, RawData}, State) -> 
    process_request(Socket, RawData),
    RequestCount = State#state.request_count,
    {noreply,State#state{request_count = RequestCount + 1}};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(timeout, #state{lsock = LSock} = State) ->
    {ok, _Sock} = gen_tcp:accept(LSock),
    adb_sup:start_child(),
    {noreply, State}. 

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}. 

process_request(Socket, RawData) ->
    try 
      Tokens = preprocess(RawData),
      evaluate_request(Socket, Tokens)
    catch
      _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))	
    end.

evaluate_request(Socket, {Command, Args}) ->
    Interpreter = spawn(ets_persistence, list_to_atom(Command), []),
    Interpreter ! {self(), Args},
    receive
      {_, _Response} ->
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
      _          -> {Command, Args}
    end.

filter_command(Command) ->
    ValidCommands = ["save", "lookup", "update", "delete"],
    [X || X <- ValidCommands, X =:= Command].

split(Str) ->
    Stripped = string:strip(Str),
    Pred = fun(A) -> A =/= $  end,
    {Command, Args} = lists:splitwith(Pred, Stripped),
    {Command, string:strip(Args)}.
