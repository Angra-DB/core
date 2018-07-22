%%-----------------------------------------------------------------------------
%% @author Ismael Coelho Medeiros <140083162@aluno.unb.br>
%%
%% @doc a first attempt to build the Angra-DB persistence.
%% 
%%-----------------------------------------------------------------------------
-module(adb_core).
-behavior(gen_server).

%% API functions
-export([start_link/2, get_count/0, stop/0, receive_request/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {persistence = none}).

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Persistence, Name) ->
    lager:info("Initializing ~s.~n", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, [Persistence], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

receive_request(Args) ->
    Response = case gen_server:call(?SERVER, {process_request, Args}) of
        {ok, _Res} -> ok;
        Res        -> Res
    end,
    Response.

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([Persistence]) ->
    {ok, #state{persistence = Persistence}, 0}.

handle_call({process_request, Args}, _From, State) -> 
    Res = process_request(Args, State),
    {reply, Res, State};
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

process_request([Command, Database, Arg], State) ->
    gen_persistence:process_request(Command, Database, Arg, State#state.persistence).