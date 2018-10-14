-module(adb_gossip_server).
-behavior(gen_server).

%% API
-export([start_link/0, get_count/0, stop/0, get/1, create/2, update/2, push/1, push/2, pull/1, pull/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {store}).

%% ============================================================================
%% API functions
%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    get_server:cast(?SERVER, stop).

create(Subject, Value) ->
    gen_server:call(?SERVER, {create, {Subject, Value}}).

update(Subject, Value) ->
    gen_server:call(?SERVER, {update, {Subject, Value}}).

get(Subject) ->
    {ok, Value} = gen_server:call(?SERVER, {get, Subject}),
    Value.

push(Subject) ->
    %% Ignore observer node.
    Nodes = [X || X <- nodes(), not is_observer_node(X)],
    case gen_server:call(?SERVER, {push, Subject, Nodes}) of
        %% Only call the method again, if the subject is on "infected" state.
        {ok, infected, Node} -> push(Subject, [X || X <- Nodes, X =/= Node]);
        Result               -> Result
    end.

push(_Subject, Nodes) when Nodes == [] -> ok;
push(Subject, Nodes) ->
    case gen_server:call(?SERVER, {push, Subject, Nodes}) of
        %% Only call the method again, if the subject is on "infected" state.
        {ok, infected, Node} -> push(Subject, [X || X <- Nodes, X =/= Node]);
        Result               -> Result
    end.

pull(Subject) ->
    %% Ignore observer node.
    Nodes = [X || X <- nodes(), not is_observer_node(X)],
    case gen_server:call(?SERVER, {pull, Subject, Nodes}) of
        %% Only call the method again, if the subject is on "infected" state.
        {ok, infected, Node} -> pull(Subject, [X || X <- Nodes, X =/= Node]);
        Result               -> Result
    end.

pull(_Subject, Nodes) when Nodes == [] -> ok;
pull(Subject, Nodes) ->
    case gen_server:call(?SERVER, {pull, Subject, Nodes}) of
        %% Only call the method again, if the subject is on "infected" state.
        {ok, infected, Node} -> pull(Subject, [X || X <- Nodes, X =/= Node]);
        Result               -> Result
    end.

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(_Args) ->
    lager:info("Initializing adb_gossip server.~n"),
    {ok, #state{store = []}, 0}.

%% Handle "get" call.
handle_call({get, Subject}, _From, State) ->
    {reply, {ok, try_get_subject_store(Subject, State)}, State};

%% Handle "create" call.
handle_call({create, {Subject, Value}}, _From, State) ->
    %% First, verify if the subject store exists.
    case try_get_subject_store(Subject, State) of
        none -> UpdatedState = add_subject_store(Subject, Value, State),
                {reply, ok, UpdatedState};
        Old  -> lager:warning("Tried to create a subject store, but it already exist.~n"),
                {reply, {warning, maps:get(value, Old)}, State}
    end;

%% Handle "update" call.
handle_call({update, {Subject, Value}}, _From, State) ->
    %% First, verify if the subject store exists.
    case try_get_subject_store(Subject, State) of
        none -> lager:error("Tried to update a subject store, but it does not exist.~n"),
                {reply, notfound, State};
        Old  -> UpdatedState = update_subject_store(Subject, Value, State),
                {reply, {ok, maps:get(value, Old)}, UpdatedState}
    end;

%% Handle "push" call.
handle_call({push, _Subject, Nodes}, _From, State) when Nodes == [] -> {reply, noremotenode, State};
handle_call({push, Subject, Nodes}, _From, State) ->
    %% First, verify if the subject store exists.
    case try_get_subject_store(Subject, State) of
        none   -> lager:error("No subject store was found."),
                  {reply, notfound, State};
        Value  -> TNode = adb_utils:choose_randomly(Nodes),
                  TModule = {adb_gossip_server, TNode},
                  TFunction = {handle_push, {Subject, Value}},
                  case gen_server:call(TModule, TFunction) of 
                      {ok, target_already_known}          -> NewState = update_subject_store_state(Subject, removed, State),
                                                             {reply, {ok, removed}, NewState};
                      {ok, target_updated}                -> {reply, {ok, infected, TNode}, State};
                      {ok, {target_has_newer_info, Data}} -> NewState = update_subject_store(Subject, maps:get(value, Data), maps:get(timestamp, Data), State),
                                                             {reply, {ok, infected, TNode}, NewState};
                      _                                   -> larger:warning("Could not push to target node (~s).~n", [TNode]),
                                                             {reply, error, State}
                  end
                  
    end;

%% Handle "handle_push" call.
handle_call({handle_push, {Subject, Info}}, _From, State) ->
    case try_get_subject_store(Subject, State) of
        none  -> NewState = add_existing_subject_store(Subject, Info, State),
                 spawn(fun() -> push(Subject) end),
                 {reply, {ok, target_updated}, NewState};
        Value -> case compare_subject_stores(Value, Info) of
                    older -> NewState = update_subject_store(Subject, maps:get(value, Info), maps:get(timestamp, Info), State),
                             spawn(fun() -> push(Subject) end),
                             {reply, {ok, target_updated}, NewState};
                    equal -> {reply, {ok, target_already_known}, State};
                    newer -> {reply, {ok, {target_has_newer_info, Value}}, State}
                 end
    end;

%% Handle "pull" call.
handle_call({pull, _Subject, Nodes}, _From, State) when Nodes == [] -> {reply, noremotenode, State};
handle_call({pull, Subject, Nodes}, _From, State) ->
    %% First, varify if the subject store exists.
    case try_get_subject_store(Subject, State) of
        none  -> TNode = adb_utils:choose_randomly(Nodes),
                 TModule = {adb_gossip_server, TNode},
                 TFunction = {handle_pull, {Subject, none}},
                 case gen_server:call(TModule, TFunction) of
                     {ok, target_has_same_info}          -> {reply, {ok, removed}, State};
                     {ok, {target_has_newer_info, Data}} -> NewState = add_existing_subject_store(Subject, Data, State),
                                                            {reply, {ok, infected, TNode}, NewState};
                     _                                   -> lager:warning("Could not pull from target node (~s).~n", [TNode]),
                                                            {reply, error, State}
                 end;
        Value -> TNode = adb_utils:choose_randomly(Nodes),
                 TModule = {adb_gossip_server, TNode},
                 TFunction = {handle_pull, {Subject, Value}},
                 case gen_server:call(TModule, TFunction) of
                     {ok, target_has_same_info}          -> NewState = update_subject_store_state(Subject, removed, State),
                                                            {reply, {ok, removed}, NewState};
                     {ok, target_has_older_info}         -> NewState = update_subject_store_state(Subject, removed, State),
                                                            {reply, {ok, removed}, NewState};
                     {ok, {target_has_newer_info, Data}} -> NewState = update_subject_store(Subject, maps:get(value, Data), maps:get(timestamp, Data), State),
                                                            {reply, {ok, infected, TNode}, NewState};
                     _                                   -> lager:warning("Could not pull from target node (~s).~n", [TNode]),
                                                            {reply, error, State}
                end
    end;

%% Handle "handle_pull" call.
handle_call({handle_pull, {Subject, none}}, _From, State) ->
    case try_get_subject_store(Subject, State) of
        none  -> {reply, {ok, target_has_same_info}, State};
        Value -> {reply, {ok, {target_has_newer_info, Value}}, State}
    end;
handle_call({handle_pull, {Subject, Info}}, _From, State) ->
    case try_get_subject_store(Subject, State) of
        none -> NewState = add_existing_subject_store(Subject, Info, State),
                spawn(fun() -> pull(Subject) end),
                {reply, {ok, target_has_older_info}, NewState};
        Value -> case compare_subject_stores(Value, Info) of
                    older -> NewState = update_with_existing_store(Subject, Info, State),
                             spawn(fun() -> pull(Subject) end),
                             {reply, {ok, target_has_older_info}, NewState};
                    equal -> {reply, {ok, target_has_same_info}, State};
                    newer -> {reply, {ok, {target_has_newer_info, Value}}, State}
                 end
    end;
handle_call(Msg, _From, State) ->
    {reply, {ok, Msg}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Into, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==============================================================================
%% Internal functions
%%==============================================================================

try_get_subject_store(Subject, State) ->
    proplists:get_value(Subject, State#state.store, none).

add_existing_subject_store(Subject, SubjectStore, State) ->
    #state{store = [{Subject, SubjectStore} | State#state.store]}.

add_subject_store(Subject, Value, State) ->
    #state{store = [create_subject_store(Subject, Value) | State#state.store]}.
    
update_with_existing_store(Subject, SubjectStore, State) ->
    Store = proplists:delete(Subject, State#state.store),
    #state{store = [{Subject, SubjectStore} | Store]}.

update_subject_store(Subject, Value, State) ->
    Store = proplists:delete(Subject, State#state.store),
    #state{store = [create_subject_store(Subject, Value) | Store]}.

update_subject_store(Subject, Value, Timestamp,  State) ->
    Store = proplists:delete(Subject, State#state.store),
    #state{store = [create_subject_store(Subject, Value, Timestamp) | Store]}.

update_subject_store_state(Subject, StoreState, State) ->
    OlderSubjectStore = try_get_subject_store(Subject, State),
    NewSubjectStore = {Subject,
        #{value     => maps:get(value, OlderSubjectStore),
          state     => StoreState,
          timestamp => maps:get(timestamp, OlderSubjectStore)}},
    #state{store = lists:keyreplace(Subject, 1, State#state.store, NewSubjectStore)}.

create_subject_store(Subject, Value) ->
    {Subject,
        #{value     => Value,
         state     => infected,
         timestamp => os:timestamp()}
    }.

create_subject_store(Subject, Value, Timestamp) ->
    {Subject,
        #{value     => Value,
         state     => infected,
         timestamp => Timestamp}
    }.

get_node_by_reference({Pid, _Tag}) ->
    node(Pid).

compare_subject_stores(StoreOne, StoreTwo) ->
    StoreOneTsp = maps:get(timestamp, StoreOne),
    StoreTwoTsp = maps:get(timestamp, StoreTwo),
    MinTsp = min(StoreOneTsp, StoreTwoTsp),
    case StoreOneTsp of
        StoreTwoTsp -> equal;
        MinTsp      -> older;
        _           -> newer
    end.

is_observer_node(Node) ->
    NodeName = atom_to_list(Node),
    [Name | _Host] = string:split(NodeName, "@"),
    case Name of
        "observer" -> true;
        _          -> false
    end.
