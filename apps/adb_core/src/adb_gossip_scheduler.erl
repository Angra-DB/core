%%
%% Reference: https://stackoverflow.com/questions/5883741/how-to-perform-actions-periodically-with-erlangs-gen-server
%%
-module(adb_gossip_scheduler).
-behavior(gen_server).

%% API
-export([start_link/0, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {gossip_interval, tref}).

%% ============================================================================
%% API functions
%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    get_server:cast(?SERVER, stop).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(_Args) ->
    {ok, Interval} = adb_dist_store:get_config(gossip_interval),
    TRef = erlang:start_timer(Interval, self(), trigger),
    {ok, #state{gossip_interval = Interval, tref = TRef}, 0}.

handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}.

handle_cast(stop_timer, State) ->
    TRef = State#state.tref,
    erlang:cancel_timer(TRef),
    receive
        {timeout, TRef, _} -> void
        after 0            -> void
    end,
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({timeout, _Ref, trigger}, State) ->
    erlang:cancel_timer(State#state.tref),
    %% =========================
    %% Perform syncing.
    try 
        adb_gossip_server:sync()
    catch
        _Class:_Err -> lager:error("An error ocurred while syncing.")
    end,
    %% =========================
    TRef = erlang:start_timer(State#state.gossip_interval, self(), trigger),
    NewState = State#state{tref = TRef},
    {noreply, NewState};
handle_info(_Into, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.