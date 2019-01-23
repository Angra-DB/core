%%-----------------------------------------------------------------------------
%% @author Ismael Medeiros <ismael.medeiros96@gmail.com>
%%
%% @doc 
%% 
%% @end
%%-----------------------------------------------------------------------------
-module(adb_dist_server).
-behavior(gen_server).

%% API functions
-export([start_link/0, get_count/0, stop/0]).
-export([find_next_node/0, find_next_node/1]).
-export([get_ring_id/0, get_ring_id/1, forward_request/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%=============================================================================
%% API functions
%%=============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

find_next_node() ->
    find_next_node(node()).

find_next_node(Node) ->
    {ok, RingInfo} = adb_dist_store:get_ring_info(),
    {ok, SortedRingInfo} = adb_utils:sort_ring_info(RingInfo),
    ThisNodeWithNexts = lists:dropwhile(fun({A, _Rindid}) -> A =/= Node end, SortedRingInfo),
    case tl(ThisNodeWithNexts) of 
        []       -> [H|_] = SortedRingInfo,
                    {ok, H}; 
        [Next|_] -> {ok, Next}
    end.

get_ring_id() ->
    get_ring_id(cache).

get_ring_id(Force) ->
    {ok, RingId} = gen_server:call(?SERVER, {get_ring_id, [Force]}),
    gen_server:call(?SERVER, {publish_ring_id, []}),
    RingId.

forward_request(Command, Args) when is_atom(Command) ->
    gen_server:call(?SERVER, {forward_request, {Command, Args}}).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([]) ->
    lager:info("Initializing adb_dist server."),
    RingId = get_or_create_ring_id(),
    adb_gossip_server:sync(),
    adb_dist_store:set_ring_info(node(), RingId),
    {ok, RingInfo} = adb_dist_store:get_ring_info(),
    adb_gossip_server:create(ring_info, RingInfo, fun store_ring_info/1, fun compare_ring_info/2),
    lager:info("Ring ID defined as '~p'.", [RingId]),
    {ok, none, 0}.

handle_call({forward_request, {Command, Args}}, _From, State) ->
    PartitionModule = get_partition_module(),
    case gen_partition:forward_request(Command, Args, PartitionModule) of
        {ok, Response}    -> {reply, {ok, Response}, State};
        {error, Response} -> {reply, {error, Response}, State}
    end;

handle_call({publish_ring_id, []}, _From, State) ->
    {reply, ok, State};
handle_call({node_up, []}, _From, State) ->
    {ok, Config} = adb_dist_store:get_config(),
    {reply, {ok, Config}, State};
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

get_or_create_ring_id() ->
    case adb_gossip_server:sync() of
        {warning, no_remote_nodes} -> 
            RingId = {1, 1},
            adb_gossip_server:create(ring_id, {node(), RingId}, fun store_ring_id/1),
            RingId;
        {ok, synced} -> 
            case adb_gossip_server:get(ring_id) of
                {warning, key_not_found} -> 
                    RingId = {1, 1},
                    adb_gossip_server:create(ring_id, {node(), RingId}, fun store_ring_id/1),
                    RingId;
                {ok, Store} -> 
                    {_, LastRingId} = maps:get(value, Store),
                    NewRingId = generate_new_ring_id(LastRingId),
                    adb_gossip_server:update(ring_id, [{value, {node(), NewRingId}}, {timestamp, os:timestamp()}]),
                    NewRingId
            end
    end.

generate_new_ring_id(LastRingId) ->
    {Num, Den} = LastRingId,
    if
        Num + 2 > Den -> {1, Den * 2};
        true          -> {Num + 2, Den}
    end.

get_partition_module() ->
    {ok, Partition} = adb_dist_store:get_config(partition),
    case Partition of
        {consistent, _HashFunc} -> consistent_partition;
        full                    -> full_partition
    end.

store_ring_id({Node, RingId}) ->
    adb_dist_store:set_ring_info(Node, RingId),
    {ok, RingInfo} = adb_dist_store:get_ring_info(),
    adb_gossip_server:update(ring_info, [{value, RingInfo}, {timestamp, os:timestamp()}]).

store_ring_info(RingInfo) ->
    lists:map(fun({Node, RingId}) -> 
        adb_dist_store:set_ring_info(Node, RingId)
    end, RingInfo).

compare_ring_info(StoreOne, StoreTwo) ->
    RingInfoOne = maps:get(value, StoreOne),
    RingInfoTwo = maps:get(value, StoreTwo),
    TwoDiff = lists:filter(fun({Key, _Value}) ->
        case lists:keysearch(Key, 1, RingInfoTwo) of 
            false      -> false;
            {value, _} -> true
        end
    end, RingInfoOne),
    OneDiff = lists:filter(fun({Key, _Value}) ->
        case lists:keysearch(Key, 1, RingInfoOne) of 
            false      -> false;
            {value, _} -> true
        end
    end, RingInfoTwo),
    TamOneDiff = length(OneDiff),
    TamTwoDiff = length(TwoDiff),
    if
        (TamOneDiff > 0) ->
            lists:map(fun({Key, RingId}) -> 
                adb_dist_store:set_ring_info(Key, RingId)
            end, OneDiff),
            {ok, RingInfo} = adb_dist_store:get_ring_info(),
            spawn(fun() -> 
                adb_gossip_server:update(ring_info, [{value, RingInfo}, {timestamp, os:timestamp()}]) 
            end),
            if 
                (TamOneDiff =< TamTwoDiff) -> older;
                (TamOneDiff < TamTwoDiff) -> newer
            end;
        (TamOneDiff =:= 0) and (TamTwoDiff =:= 0) -> equal;
        (TamOneDiff =< TamTwoDiff) -> newer
    end.