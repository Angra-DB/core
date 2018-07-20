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

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Config) ->
    lager:info("Starting the AngraDB server."), 
    Port = case application:get_env(tcp_interface, port) of
        {ok, P}   -> P;
        undefined -> proplists:get_value(port, Config)
    end,
    {ok, LSock} = gen_tcp:listen(Port, [{active,true}, {reuseaddr, true}]),
    lager:info("Listening to TCP requests on port ~w.", [Port]),
    case adb_server_sup:start_link(LSock, Config) of
        {ok, Pid} -> adb_server_sup:start_child(),
                     {ok, Pid};
        Other     -> error_logger:error_msg(" error: ~s.", [Other]),
                     {error, Other}
    end,
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Config], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    get_server:cast(?SERVER, stop).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([State]) ->
    {ok, State, 0}.

handle_call({node_up, Args}, From, State) ->
    {reply, Args, State};
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