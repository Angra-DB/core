%%-----------------------------------------------------------------------------
%% @author Ismael Medeiros <ismael.medeiros96@gmail.com>
%%
%% @doc a first attempt to build the Angra-DB persistence.
%% 
%%-----------------------------------------------------------------------------
-module(adb_persistence).
-behavior(gen_server).

%% API functions
-export([start_link/2, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([replicate_request/2]).

-define(SERVER, ?MODULE).

-record(state, {persistence = none, name = none, vnode_id = none}).

%%=============================================================================
%% API functions
%%=============================================================================

start_link(Persistence, Id) ->
    Name = adb_utils:get_vnode_process(?MODULE, Id), 
    lager:info("Initializing ~s.", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, [Persistence, Name, Id], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

replicate_request(Args, Copies) ->
    gen_server:cast(?SERVER, {replicate_request, Args, Copies}).

%%=============================================================================
%% gen_server callbacks
%%=============================================================================

init([Persistence, Name, Id]) ->
    {ok, #state{persistence = Persistence, name = Name, vnode_id = Id}, 0}.

handle_call({process_request, Args}, _From, State) -> 
    PersistRes = process_request(Args, State),
    Response = standardize_response(PersistRes),
    {reply, Response, State};
handle_call({process_request, Args, replicate}, _From, State) -> 
    PersistRes = process_request(Args, State),
    Response = standardize_response(PersistRes),
    case adb_dist_server:find_next_node() of
        {ok, Next} when Next =:= node() -> do_nothing;
        {ok, Next}                      -> spawn(Next, State#state.name, replicate_request, [Args, [node()]])
    end,
    {reply, Response, State};
handle_call({replicate_request, Args, Copies}, _From, State) ->
    {ok, Replication} = adb_dist_store:get_config(),
    if 
        length(Copies) =< Replication -> 
            process_request(Args, State),
            case adb_dist_server:find_next_node() of
                {ok, Next} when Next =:= node() -> do_nothing;
                {ok, Next}                      -> 
                    case lists:search(fun(X) -> X =:= Next end, Copies) of
                        {value, _} -> do_nothing;
                        false      -> spawn(Next, State#state.name, replicate_request, [Args, [node()|Copies]])
                    end
            end;
        true                          -> do_nothing
    end,
    {reply, ok, State};
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

process_request({Command, Database, Database}, State) ->
    DatabaseName = adb_utils:get_database_name(Database, State#state.name),
    gen_persistence:process_request(Command, DatabaseName, DatabaseName, State#state.persistence, State#state.vnode_id);
process_request({Command, Database, Args}, State) ->
    DatabaseName = list_to_atom(adb_utils:get_database_name(Database, State#state.name)),
    gen_persistence:process_request(Command, DatabaseName, Args, State#state.persistence, State#state.vnode_id).
    
standardize_response(ok)                -> {ok, []};
standardize_response({ok, Response})    -> {ok, Response};
standardize_response({error, ErrorMsg}) -> {error, ErrorMsg};
standardize_response(db_does_not_exist) -> {ok, db_does_not_exist};
standardize_response(Response)          -> {ok, Response}.