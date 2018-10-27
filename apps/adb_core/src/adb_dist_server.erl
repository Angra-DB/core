%%-----------------------------------------------------------------------------
%% @author Ismael Coelho Medeiros <140083162@aluno.unb.br>
%%
%% @doc a first attempt to build the Angra-DB server
%% 
%% @end
%%-----------------------------------------------------------------------------
-module(adb_dist_server).
-behavior(gen_server).

%% API functions
-export([start_link/0, get_count/0, stop/0]).
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
    lager:info("Initializing adb_dist server.~n"),
    {ok, none, 0}.

handle_call({forward_request, {Command, Args}}, _From, State) ->
    PartitionMode = get_partition_module(),
    case gen_partition:forward_request(Command, Args, PartitionMode) of
        {ok, Response}    -> {reply, {ok, Response}, State};
        {error, Response} -> {reply, {error, Response}, State}
    end;

handle_call({get_ring_id, [cache]}, _From, State) ->
    {ok, RingId} = adb_dist_store:get_ring_info(node()),
    case RingId of
        none  -> NewId = get_or_create_ring_id(),
                 adb_dist_store:set_ring_info(node(), NewId),
                 {reply, {ok, NewId}, State};
        Value -> {reply, {ok, Value}, State}
    end;
handle_call({get_ring_id, [refresh]}, _From, State) ->
    RingId = get_or_create_ring_id(),
    adb_dist_store:set_ring_info(node(), RingId),
    {reply, {ok, RingId}, State};

handle_call({publish_ring_id, []}, _From, State) ->
    {ok, RingId} = adb_dist_store:get_ring_info(node()),
    adb_gossip_server:update(ring_id, {node(), RingId}),
    adb_gossip_server:push(ring_id),
    {reply, ok, State};
handle_call({node_up, []}, _From, State) ->
    {ok, Persistence} = adb_dist_store:get_config(persistence),
    {ok, Distribution} = adb_dist_store:get_config(distribution),
    {ok, Partition} = adb_dist_store:get_config(partition),
    {ok, Replication} = adb_dist_store:get_config(replication),
    {ok, WriteQuorum} = adb_dist_store:get_config(write_quorum),
    {ok, ReadQuorum} = adb_dist_store:get_config(read_quorum),
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    {ok, GossipInterval} = adb_dist_store:get_config(gossip_interval),
    {ok, Server} = adb_dist_store:get_config(server),
    {reply, {ok, [{persistence, Persistence},
                  {distribution, Distribution},
                  {partition, Partition},
                  {replication, Replication},
                  {write_quorum, WriteQuorum},
                  {read_quorum, ReadQuorum},
                  {vnodes, VNodes},
                  {gossip_interval, GossipInterval},
                  {server, Server}]}, State};
handle_call({init_server_or_acknowledge, []}, _From, State) ->
    case adb_dist_store:get_config(server) of
        {ok, none}   -> {ok, _Pid} = init_server(),
                        adb_dist_store:set_config(server, node()),
                        {reply, {ok, init}, State};
        {ok, Remote} -> {reply, {ok, {acknowledge, Remote}}, State}
    end;
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

init_server() ->
    {ok, P} = adb_utils:get_env("ADB_PORT"),
    Port = if 
        is_integer(P) -> P;
        true          -> {Num, []} = string:to_integer(P),
                         Num
    end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),
    lager:info("Listening to TCP requests on port ~w.", [Port]),
    ServerSupSpec = #{id       => adb_server_sup,
                      start    => {adb_server_sup, start_link, [LSock]},
                      restart  => permanent,
                      shutdown => infinity,
                      type     => supervisor,
                      modules  => [adb_server_sup]},
    case adb_dist_sup:start_child(ServerSupSpec) of
        {ok, Pid} -> {ok, Pid};
        Other     -> error_logger:error_msg(" error: ~s.", [Other]),
                     {error, Other}
    end.

get_or_create_ring_id() ->
    case adb_gossip_server:pull(ring_id) of
        noremotenode -> RingId = {1, 1},
                        adb_gossip_server:create(ring_id, {node(), RingId}),
                        RingId;
        ok           -> case adb_gossip_server:get(ring_id) of
                            none  -> RingId = {1, 1},
                                     adb_gossip_server:create(ring_id, {node(), RingId}),
                                     RingId;
                            Store -> {Node, LastRingId} = maps:get(value, Store),
                                     store_last_ring_id(Node, LastRingId),
                                     generate_new_ring_id(LastRingId)
                        end
    end.

generate_new_ring_id(LastRingId) ->
    {Num, Dem} = LastRingId,
    if
        Num + 2 > Dem -> {1, Dem * 2};
        true          -> {Num + 2, Dem}
    end.

get_partition_module() ->
    {ok, Partition} = adb_dist_store:get_config(partition),
    case Partition of
        {consistent, _} -> consistent_partition;
        _Full           -> full_partition
    end.

store_last_ring_id(Node, LastRingId) ->
    adb_dist_store:set_ring_info(Node, LastRingId).
