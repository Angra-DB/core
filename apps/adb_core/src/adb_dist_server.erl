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
-export([start_link/1, get_count/0, stop/0]).
-export([get_config/1, get_ring_id/0, get_ring_id/1, forward_request/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {persistence, distribution, partition, replication, write_quorum, read_quorum, vnodes, server=none, ring_id=none}).

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

get_config(Config) when is_atom(Config) ->
    {ok, Value} = gen_server:call(?SERVER, {get_config, Config}),
    Value.

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

init([Persistence, Distribution, Partition, Replication, WriteQuorum, ReadQuorum, VNodes, RemoteServer]) ->
    lager:info("Initializing adb_dist server.~n"),
    {ok, #state{persistence  = Persistence, 
                distribution = Distribution,
                partition    = Partition,
                replication  = Replication,
                write_quorum = WriteQuorum,
                read_quorum  = ReadQuorum,
                vnodes       = VNodes,
                server       = RemoteServer,
                ring_id      = none}, 0}.

handle_call({get_config, Config}, _From, State) ->
    {Config, Value} = retrieve_config(Config, State),
    {reply, {ok, Value}, State};
handle_call({forward_request, {Command, Args}}, _From, State) ->
    PartitionMode = get_partition_module(State),
    case gen_partition:forward_request(Command, Args, PartitionMode) of
        {ok, Response}    -> {reply, {ok, Response}, State};
        {error, Response} -> {reply, {error, Response}, State}
    end;
handle_call({get_ring_id, [cache]}, _From, State) ->
    case State#state.ring_id of
        none  -> RingId = get_or_create_ring_id(),
                 NewState = State#state{ ring_id = RingId },
                 {reply, {ok, RingId}, NewState};
        Value -> {reply, {ok, Value}, State}
    end;
handle_call({get_ring_id, [refresh]}, _From, State) ->
    RingId = get_or_create_ring_id(),
    NewState = State#state{ ring_id = RingId },
    {reply, {ok, RingId}, NewState};
handle_call({publish_ring_id, []}, _From, State) ->
    adb_gossip_server:update(ring_id, State#state.ring_id),
    adb_gossip_server:push(ring_id),
    {reply, ok, State};
handle_call({node_up, []}, _From, State) ->
    {reply, {ok, [State#state.persistence,
                  State#state.distribution,
                  State#state.partition,
                  State#state.replication,
                  State#state.write_quorum,
                  State#state.read_quorum,
                  State#state.vnodes,
                  State#state.server]}, State};
handle_call({init_server_or_acknowledge, []}, _From, State) ->
    case State#state.server of
        {server, none}   -> {ok, _Pid} = init_server(),
                            {reply, {ok, init}, State#state{server=node()}};
        Remote           -> {reply, {ok, {acknowledge, Remote}}, State}
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
        noremotenode -> Value = {1, 1},
                        adb_gossip_server:create(ring_id, Value),
                        Value;
        ok           -> case adb_gossip_server:get(ring_id) of
                            none  -> Value = {1, 1},
                                     adb_gossip_server:create(ring_id, Value),
                                     Value;
                            Store -> generate_new_ring_id(maps:get(value, Store))
                        end
    end.

generate_new_ring_id(OldRingId) ->
    {Num, Dem} = OldRingId,
    if
        Num + 2 > Dem -> {1, Dem * 2};
        true          -> {Num + 2, Dem}
    end.

retrieve_config(Config, State) when is_atom(Config) ->
    case Config of
        persistence  -> State#state.persistence;
        distribution -> State#state.distribution;
        partition    -> State#state.partition;
        replication  -> State#state.replication;
        write_quorum -> State#state.write_quorum;
        read_quorum  -> State#state.read_quorum;
        vnodes       -> State#state.vnodes;
        server       -> State#state.server;
        ring_id      -> State#state.ring_id;
        _            -> undefined
    end.

get_partition_module(State) ->
    {partition, Partition} = State#state.partition,
    case Partition of
        {consistent, _} -> consistent_partition;
        _Full           -> full_partition
    end.
