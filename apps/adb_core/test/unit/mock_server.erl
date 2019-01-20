-module(mock_server).
-behavior(gen_server).

%% API
-export([start_link/0, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% ============================================================================
%% API functions
%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(_Args) ->
    {ok, [], 0}.

handle_call(one, _From, State) ->
    {reply, ok, State};
handle_call(two, _From, State) ->
    {reply, error, State};
handle_call(three, _From, State) ->
    {reply, {ok, three}, State};
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
