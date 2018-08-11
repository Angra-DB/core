%%-----------------------------------------------------------------------------
%% @author Ismael Coelho Medeiros <140083162@aluno.unb.br>
%%
%% @doc a first attempt to build the Angra-DB server
%% 
%% @end
%%-----------------------------------------------------------------------------
-module(adb_dist).
-behavior(gen_server).

%% API functions
-export([start_link/1, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_PORT, 1234).

-record(state, {port, persistence, distribution, replication, vnodes, server=none}).

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    get_server:cast(?SERVER, stop).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([Port, Persistence, Distribution, Replication, VNodes, RemoteServer]) ->
    lager:info("Initializing adb_dist.~n"),
    {ok, #state{port = Port, 
                persistence = Persistence, 
                distribution = Distribution,
                replication = Replication,
                vnodes = VNodes,
                server = RemoteServer}, 0}.

handle_call({node_up, []}, _From, State) ->
    {reply, {ok, tl(tuple_to_list(State))}, State};
handle_call({init_server_or_acknowledge, []}, _From, State) ->
    case State#state.server of
        {server, none}   -> {ok, _Pid} = init_server(State#state.port),
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

init_server(PortConfig) ->
    Port = case PortConfig of
        {port, P} -> P;
        true      -> ?DEFAULT_PORT
    end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),
    lager:info("Listening to TCP requests on port ~w.", [Port]),
    ServerSpec = #{id       => adb_server_sup,
                   start    => {adb_server_sup, start_link, [LSock]},
                   restart  => permanent,
                   shutdown => infinity,
                   type     => supervisor,
                   modules  => [adb_server_sup]},
    case adb_dist_sup:start_child(ServerSpec) of
        {ok, Pid} -> {ok, Pid};
        Other     -> error_logger:error_msg(" error: ~s.", [Other]),
                     {error, Other}
    end.
