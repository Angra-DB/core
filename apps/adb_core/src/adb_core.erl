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
-export([start_link/1, get_count/0, stop/0, process_request/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE). % declares a SERVER macro constant (?MODULE is the module's name)

-record(state, {persistence}). % a record for keeping the server state

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Host) ->
    {ok, Persistence} = get_persistence_from_server(Host),
    gen_server:start_link(?SERVER, [Persistence], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

process_request(Args)->
    gen_server:call(?SERVER, process_request, Args).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([Persistence]) ->
    {ok, #state{persistence = Persistence}, 0}.

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

get_persistence_from_server(Host) ->
    case rpc:call(Host, adb_server, inform_persistence, []) of
        {badrpc, _Reason} -> lager:warning("Persistence not found. Assuming standard persistence."),
                             adbtree_persistence;
        Response          -> lager:info("Persistence acknowledged: ~p.", [Response]),
                             Response
    end.