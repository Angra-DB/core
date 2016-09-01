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
	, start_link/0
	, get_count/0   % we can understand both get_count and stop as adm operations
	, stop/0]).

% gen_server callbacks
-export([ init/1
	, handle_call/3
	, handle_cast/2
	, handle_info/2
	, terminate/2
	, code_change/3]).

-export([parse/1]).

-define(SERVER, ?MODULE).      % declares a SERVER macro constant (?MODULE is the module's name) 
-define(DEFAULT_PORT, 1234).   % declares a DEFAULT_PORT macro constant

-record(state, {port, lsock, request_count = 0}). % a record for keeping the server state

%%%======================================================
%%% API
%%%
%%% Each one of the functions that appear in the 
%%% API section, calls one of the gen_server library 
%%% functions (start_link/4, call/2, cast/2)... This 
%%% is a bit trick. 
%%%======================================================

start_link(Port) ->
    interpreter:start(),  
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Port], []).
			  
start_link() ->
    start_link(?DEFAULT_PORT).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop). 

%%%===========================================
%%% gen_server callbacks
%%%===========================================

init([Port]) ->
    {ok, LSock} = gen_tcp:listen(Port, [{active, true}]),
    {ok, #state{port = Port, lsock = LSock}, 0}.

handle_call(get_count, _From, State) ->
     {reply, {ok, State#state.request_count}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({tcp, Socket, RawData}, State) -> 
    process_request(Socket, RawData),
    RequestCount = State#state.request_count,
    {noreply,State#state{request_count = RequestCount + 1}};
handle_info(timeout, #state{lsock = LSock} = State) ->
    {ok, _Sock} = gen_tcp:accept(LSock),
    {noreply, State}. 

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}. 

process_request(Socket, RawData) ->
    try 
       Command = parse(RawData),
       Pid = spawn(interpreter, execute, []),
       Pid ! {self(), {Command}},
       receive
          {_From, ok} -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", ["ok"]));
          {_From, Doc} -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Doc]))
       end
    catch
       _Class:Err -> gen_tcp:send(Socket, io_lib:fwrite("~p~n", [Err]))	
    end.


parse(RawData) ->    
    Tokens = string:tokens(RawData, " "),    
    case Tokens of 
	["save", Key | Doc] -> [save, Key, (string:join(Doc, " "))];    
	["lookup", Key] -> [lookup, Key];
	["delete", Key] -> [delete, Key];
	["update", Key| Doc] -> [update, Key, (string:join(Doc, " "))]
    end.
			    
			 
