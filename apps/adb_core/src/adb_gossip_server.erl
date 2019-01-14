-module(adb_gossip_server).
-behavior(gen_server).

%% API
-export([start_link/0, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1, create/2, create/3, create/4, update/2, sync/0, sync/1]).

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

get(Subject) ->
    gen_server:call(?SERVER, {get, Subject}).

create(Subject, Value, UpCallback, CmpCallback) ->
    gen_server:call(?SERVER, {create, {Subject, Value, [UpCallback], CmpCallback}}).

create(Subject, Value, UpCallback) ->
    gen_server:call(?SERVER, {create, {Subject, Value, [UpCallback], none}}).

create(Subject, Value) ->
    gen_server:call(?SERVER, {create, {Subject, Value, [], none}}).

update(Subject, Changes) ->
    gen_server:call(?SERVER, {update, {Subject, Changes}}).

sync() -> sync(nodes()).

sync([]) -> {warning, no_remote_nodes};
sync(Nodes) ->
    NotObsNodes = [X || X <- Nodes, not is_observer_node(X)],
    iterate_sync(NotObsNodes).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(_Args) ->
    lager:info("Initializing adb_gossip_server.~n"),
    %% Create ets store to store gossip state.
    Store = ets:new(store, [set, public, named_table]),
    {ok, #state{store = Store}, 0}.

%% Handles "get" call.
handle_call({get, Subject}, _From, State) ->
    case ets:lookup(store, Subject) of 
        []                 -> lager:warning("Tried to get subject ~p, but it doesn't exist.~n", [Subject]),
                              {reply, {warning, key_not_found}, State};
        [{Subject, Value}] -> {reply, {ok, Value}, State}
    end;

%% Handles "create" call.
handle_call({create, {Subject, Value, UpCallbackList, CmpCallback}}, _From, State) ->
    %% Verifies if the subject store exists.
    case ets:lookup(store, Subject) of
        []       -> NState = create_subject({Subject, Value, UpCallbackList, CmpCallback}, store),
                    {reply, {ok, Subject}, NState};
        [_Value] -> lager:warning("Tried to create a subject ~p, but it already exist.~n", [Subject]),
                    {reply, {warning, already_exists}, State}
    end;

%% Handles "update" call.
handle_call({update, {Subject, Params}}, _From, State) ->
    %% Verifies if the subject store exists.
    case ets:lookup(store, Subject) of
        []                 -> lager:warning("Tried to get subject ~p, but it doesn't exist.~n", [Subject]),
                              {reply, {warning, key_not_found}, State};
        [{Subject, Store}] -> ets:delete(store, Subject),
                              {ok, NewStore} = update_subject(Params, Store),
                              Result = ets:insert(store, {Subject, NewStore}),
                              {reply, {ok, Result}, State}
    end;

%% Handles "sync" call.
handle_call({sync, TNode}, _From, State) ->
    lager:info("Syncing with ~p.~n", [TNode]),
    Matches = ets:match(store, '$1'),
    LocalStore = [X || [X] <- Matches],
    TModule = {adb_gossip_server, TNode},
    try gen_server:call(TModule, {handle_sync, LocalStore}) of
        {ok, {even, []}}         -> {reply, {ok, even}, State};
        {ok, {synced, []}}       -> {reply, {ok, synced}, State};
        {ok, {synced, Changes}}  -> {ok, _} = apply_changes(Changes, store),
                                    {reply, {ok, synced}, State};
        {error, Reason}          -> lager:error("Could not sync with node ~p: ~p", [TNode, Reason]),
                                    {reply, {error, Reason}, State}
    catch
        exit:{Err, _Details} -> lager:error("Could not sync with node ~p: ~p", [TNode, Err]),
                                {reply, {error, Err}, State}; 
        _Class:Err ->           lager:error("Could not sync with node ~p", [TNode]),
                                {reply, {error, Err}, State}
    end;

%% Handles "handle_sync" call.
handle_call({handle_sync, RemoteStore}, _From, State) ->
    Matches = ets:match(store, '$1'),
    LocalStore = [X || [X] <- Matches],
    case diff(LocalStore, RemoteStore) of 
        {ok, {[], []}}        -> {reply, {ok, {even, []}}, State};
        {ok, {Local, Remote}} -> {ok, _} = apply_changes(Local, store),
                                 {reply, {ok, {synced, Remote}}, State};
        {error, Reason}       -> {reply, {error, Reason}, State}
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
 
diff(LocalStore, RemoteStore) -> 
    diff(LocalStore, RemoteStore, [], []).

diff([], [], LocalChanges, RemoteChanges) -> 
    {ok, {LocalChanges, RemoteChanges}};
diff([LocalStore|Rest], [], LocalChanges, RemoteChanges) ->
    NRemoteChanges = lists:append(RemoteChanges, [LocalStore]),
    diff(Rest, [], LocalChanges, NRemoteChanges);
diff([], [RemoteStore|Rest], LocalChanges, RemoteChanges) ->
    NLocalChanges = lists:append(LocalChanges, [RemoteStore]),
    diff([], Rest, NLocalChanges, RemoteChanges);
diff([{Key, LStore}|Rest], RemoteStore, LocalChanges, RemoteChanges) ->
    case proplists:get_value(Key, RemoteStore, none) of 
        none   -> NRemoteChanges = lists:append(RemoteChanges, [{Key, LStore}]),
                  diff(Rest, RemoteStore, LocalChanges, NRemoteChanges);
        RStore -> {LStoreDiff, RStoreDiff} = store_diff(LStore, RStore),
                  NRemoteStore = proplists:delete(Key, RemoteStore),
                  NLocalChanges = case LStoreDiff of
                      []    -> LocalChanges;
                      LDiff -> lists:append(LocalChanges, [{Key, LDiff}])
                  end,
                  NRemoteChanges = case RStoreDiff of 
                      []    -> RemoteChanges;
                      RDiff -> lists:append(RemoteChanges, [{Key, RDiff}])
                  end,
                  diff(Rest, NRemoteStore, NLocalChanges, NRemoteChanges)
    end.

store_diff(LocalStore, RemoteStore) -> 
    %% Strategy: always trust that the two stores use the same compare function.
    Compare = case maps:get(compare_callback, LocalStore, none) of 
        none    -> fun default_compare_stores/2;
        CmpFunc -> CmpFunc
    end,
    case Compare(LocalStore, RemoteStore) of 
        equal -> {[], []};
        older -> {[{value, maps:get(value, RemoteStore)}, {timestamp, maps:get(timestamp, RemoteStore)}], []};
        newer -> {[], [{value, maps:get(value, LocalStore)}, {timestamp, maps:get(timestamp, LocalStore)}]}
    end.

apply_changes([], _)          -> {ok, updated};
apply_changes([{Key, NewStore}|Rest], Store) when is_map(NewStore) ->
    case ets:lookup(Store, Key) of 
        []       -> ets:insert(Store, {Key, NewStore});
        [_Value] -> ets:delete(Store, Key),
                    ets:insert(Store, {Key, NewStore})
    end,
    apply_changes(Rest, Store);
apply_changes([{Key, Changes}|Rest], Store)  when is_list(Changes) ->
    case ets:lookup(Store, Key) of 
        []              -> lager:error("Store ~p not found.", [Key]),
                           {error, not_found};
        [{Key, LValue}] -> ets:delete(Store, Key),
                           {ok, NValue} = apply_changes_on_store(LValue, Changes),
                           %% Call Update callbacks
                           UpCallbacks = maps:get(update_callback_list, NValue),
                           execute_callbacks(UpCallbacks, maps:get(value, NValue)),
                           ets:insert(Store, {Key, NValue}),
                           apply_changes(Rest, Store)
    end.

apply_changes_on_store(Store, [])                       -> {ok, Store};
apply_changes_on_store(OldStore, [{Field, Value}|Rest]) ->
    NewStore = maps:update(Field, Value, OldStore),
    apply_changes_on_store(NewStore, Rest).

execute_callbacks(UpCallbacks, Value) ->
    execute_callbacks(UpCallbacks, Value, []).

execute_callbacks([], _, Pids) -> {ok, Pids};
execute_callbacks([UpCallback|Rest], NewValue, Pids) -> 
    Pid = spawn(fun() ->UpCallback(NewValue) end),
    execute_callbacks(Rest, NewValue, [Pid|Pids]).

create_subject({Subject, Value, UpCallbackList, CmpCallback}, Store) ->
    NewStore = {Subject, 
        #{value                => Value,
          timestamp            => os:timestamp(),
          update_callback_list => UpCallbackList,
          compare_callback     => CmpCallback}},
    Result = ets:insert(Store, NewStore),
    {ok, Result}.

update_subject([], Store)                  -> {ok, Store};
update_subject([{Key, Value}|Rest], Store) -> 
    case maps:get(Key, Store, none) of
        none   -> lager:warning("The key ~p was not found store.~n", [Key]),
                  {warning, key_not_found};
        _Value -> NewStore = maps:update(Key, Value, Store),
                  update_subject(Rest, NewStore)
    end.

is_observer_node(Node) ->
    NodeName = atom_to_list(Node),
    [Name|_Host] = string:split(NodeName, "@"),
    Name =:= "observer".

iterate_sync([]) -> ok;
iterate_sync(Nodes) ->
    TNode = adb_utils:choose_randomly(Nodes),
    Rest = [X || X <- Nodes, X =/= TNode],
    case gen_server:call(?SERVER, {sync, TNode}) of 
        {ok, synced} when Rest =/= [] -> iterate_sync(Rest);
        {error, _}   when Rest =/= [] -> iterate_sync(Rest);
        _Result                       -> {ok, synced}
    end.

default_compare_stores(StoreOne, StoreTwo) ->
    StoreOneTsp = maps:get(timestamp, StoreOne),
    StoreTwoTsp = maps:get(timestamp, StoreTwo),
    MinTsp = min(StoreOneTsp, StoreTwoTsp),
    case StoreOneTsp of
        StoreTwoTsp -> equal;
        MinTsp      -> older;
        _           -> newer
    end.