%%-----------------------------------------------------------------------------
%% @author Ismael Medeiros <ismael.medeiros96@gmail.com>
%%
%% @doc a first attempt to build the Angra-DB server
%% 
%% @end
%%-----------------------------------------------------------------------------
-module(adb_dist_store).
-behavior(gen_server).

%% API functions
-export([start_link/1, get_count/0, stop/0]).
-export([get_config/0, get_config/1, set_config/2, get_ring_info/0, get_ring_info/1, set_ring_info/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(CONFIG, [persistence, max_index_size, distribution, partition, replication, write_quorum, read_quorum, vnodes, gossip_interval, server]).

-record(state, {configStore, ringInfoStore}).


%%=============================================================================
%% API functions
%%=============================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

get_config() ->
    gen_server:call(?SERVER, get_config).

get_config(Config) when is_atom(Config) ->
    gen_server:call(?SERVER, {get_config, Config}).

set_config(Config, Value) when is_atom(Config) ->
    gen_server:call(?SERVER, {set_config, {Config, Value}}).

get_ring_info() ->
    gen_server:call(?SERVER, get_ring_info).

get_ring_info(Node) when is_atom(Node) ->
    gen_server:call(?SERVER, {get_ring_info, Node}).

set_ring_info(Node, Value) when is_atom(Node) ->
    gen_server:call(?SERVER, {set_ring_info, {Node, Value}}).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init(Config) ->
    lager:info("Initializing adb_dist store.~n"),
    %% Create stores.
    ConfigStore = ets:new(config, [set, public, named_table]),
    RingInfoStore = ets:new(ring, [set, public, named_table]),
    %% Populate config store.
    [ets:insert(config, proplists:lookup(K, Config)) || K <- ?CONFIG],
    {ok, #state{configStore   = ConfigStore,
                ringInfoStore = RingInfoStore}, 0}.

handle_call(get_config, _From, State) ->
    Matches = ets:match(config, '$1'),
    Config = [X || [X] <- Matches],
    {reply, {ok, Config}, State};
handle_call({get_config, Config}, _From, State) ->
    Response = case ets:lookup(config, Config) of 
        []                -> {ok, none};
        [{Config, Value}] -> {ok, Value}
    end,
    {reply, Response, State};
handle_call({set_config, {Config, Value}}, _From, State) ->
    Result = ets:insert(config, {Config, Value}),
    {reply, {ok, Result}, State};

handle_call(get_ring_info, _From, State) ->
    Matches = ets:match(ring, '$1'),
    Info = [X || [X] <- Matches],
    {reply, {ok, Info}, State};
handle_call({get_ring_info, Node}, _From, State) ->
    Response = case ets:lookup(ring, Node) of 
        []              -> {ok, none};
        [{Node, Value}] -> {ok, Value}
    end,
    {reply, Response, State};
handle_call({set_ring_info, {Node, Value}}, _From, State) ->
    Result = ets:insert(ring, {Node, Value}),
    {reply, {ok, Result}, State};

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