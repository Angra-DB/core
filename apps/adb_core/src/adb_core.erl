%%-----------------------------------------------------------------------------
%% @author Ismael Coelho Medeiros <140083162@aluno.unb.br>
%%
%% @doc a first attempt to build the Angra-DB server
%% 
%% @end
%%-----------------------------------------------------------------------------

-module(adb_core).

-behavior(gen_server).

%% API functions
-export([start_link/1, get_count/0, stop/0, receive_request/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE). % declares a SERVER macro constant (?MODULE is the module's name)

%%=============================================================================
%% API functions
%%=============================================================================

start_link(State) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [State], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

receive_request(Args) ->
    gen_server:call(?SERVER, {process_request, Args}).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([State]) ->
    {ok, State, 0}.

handle_call({process_request, Args}, _From, State) -> 
    Res = process_request(Args),
    {reply, {ok, Res}, State};
handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%==============================================================================
%% Internal functions
%%==============================================================================

process_request([Command, Database, Arg, Persistence]) ->
    gen_persistence:process_request(Command, Database, Arg, Persistence).