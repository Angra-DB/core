-module(consistent_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, bulk_lookup/2, update/3, delete/2, query_term/2, query/2]).

%% Private functions (only exported on testing mode).
-ifdef(TEST).
-export([get_hash_func/0]).
-endif.

-define(HASH_SPACES, [{md5, 16}, {sha, 20}, {sha256, 32}, {sha384, 48}, {sha512, 64}]).

%%=============================================================================
%% gen_partition callbacks
%%=============================================================================

create_db(Database) ->
    Targets = [node()|adb_utils:valid_nodes()],
    Request = {process_request, {all, {create_db, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), write) of
        {success, _Res}                        -> {ok, Database};
        {failed, {badmatch, {error, eexist}}}  -> {error, already_exists};
        {failed, Response}                     -> {error, Response};
        {error, Reason}                        -> {error, Reason}
    end.

connect(Database) ->
    Targets = [node()|adb_utils:valid_nodes()],
    Request = {process_request, {all, {connect, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

save(Database, Key, {Size, Doc}) ->
    HashFunc = get_hash_func(),
    {ok, {_HashKey, Target, VNode}} = map_key(HashFunc, Key),
    %% Continue to use the original key to store the document. The hash value of the
    %% key is only used to map it on the cluster.
    Request = {process_request, {VNode, {save_key, Database, {Key, Size, Doc}}, replicate}},
    gen_server:call({adb_vnode_server, Target}, Request).

lookup(Database, Key) ->
    HashFunc = get_hash_func(),
    {ok, {_HashKey, Target, VNode}} = map_key(HashFunc, Key),
    %% Continue to use the original key to store the document. The hash value of the
    %% key is only used to map it on the cluster.
    Request = {process_request, {VNode, {lookup, Database, Key}}},
    lookup_call(Target, Request).

bulk_lookup(Database, Keys) ->
    HashFunc = get_hash_func(),
    {ok, HashKeys} = map_keys(HashFunc, Keys),
    bulk_lookup(Database, HashKeys, []).

update(Database, Key, {Size, Doc}) ->
    HashFunc = get_hash_func(),
    {ok, {_HashKey, Target, VNode}} = map_key(HashFunc, Key),
    %% Continue to use the original key to store the document. The hash value of the
    %% key is only used to map it on the cluster.
    Request = {process_request, {VNode, {update, Database, {Key, Size, Doc}}, replicate}},
    gen_server:call({adb_vnode_server, Target}, Request).

delete(Database, Key) ->
    HashFunc = get_hash_func(),
    {ok, {_HashKey, Target, VNode}} = map_key(HashFunc, Key),
    %% Continue to use the original key to store the document. The hash value of the
    %% key is only used to map it on the cluster.
    Request = {process_request, {VNode, {delete, Database, Key}, replicate}},
    gen_server:call({adb_vnode_server, Target}, Request).

query_term(Database, Term) ->
    Targets = [node()|adb_utils:valid_nodes()],
    Request = {process_request, {all, {query_term, Database, Term}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), write) of
        {success, _Res}                        -> {ok, Database};
        {failed, Response}                     -> {error, Response};
        {error, Reason}                        -> {error, Reason}
    end.

query(Database, Query) ->
    Targets = [node()|adb_utils:valid_nodes()],
    Request = {process_request, {all, {query, Database, Query}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), write) of
        {success, _Res}                        -> {ok, Database};
        {failed, Response}                     -> {error, Response};
        {error, Reason}                        -> {error, Reason}
    end.

%%==============================================================================
%% Internal functions
%%==============================================================================

map_key(HashFunc, Key) ->
    {ok, SpaceSize} = validate_hash(HashFunc),
    HashKey = crypto:bytes_to_integer(crypto:hash(HashFunc, Key)),
    {ok, {PartSize, TargetVNode}} = find_target_vnode(SpaceSize, HashKey),
    {ok, Target} = find_target(PartSize, TargetVNode, HashKey),
    {ok, {HashKey, Target, TargetVNode}}.

map_keys(HashFunc, Keys) ->
    MappedKeys = lists:map(fun(OriginalKey) ->
        {ok, {_HashKey, Target, VNode}} = map_key(HashFunc, OriginalKey),
        %% Continue to use the original key to store the document. The hash 
        %% value of the key is only used to map it on the cluster.
        {{Target, VNode}, OriginalKey}
    end, Keys),
    Targets = proplists:get_keys(MappedKeys),
    GroupedKeys = lists:map(fun({Target, VNode} = T) -> 
        Values = [Value || {Key, Value} <- MappedKeys, Key =:= T],
        {Values, Target, VNode}
    end, Targets),
    {ok, GroupedKeys}.

validate_hash(HashFunc) ->
    case proplists:lookup(HashFunc, ?HASH_SPACES) of
        none                 -> {error, invalid_hash};
        {HashFunc, HashSize} -> SpaceSize = math:pow(2, 8 * HashSize),
                                % As the math:pow returns a float, we trucade this value.
                                {ok, trunc(SpaceSize)}
    end.

find_target_vnode(SpaceSize, HashKey) when is_integer(SpaceSize) ->
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    PartSize = SpaceSize / VNodes,
    %% Forces to always return the first Id that satify the predicate.
    VNodeId = case lists:filter(fun(X) -> HashKey =< trunc(PartSize * X) end, lists:seq(1, VNodes)) of 
        []     -> %% This adjustment is necessary for the case, the last element edge can be 
                  %% less than the theorical edge, not mapping some key. This is because of
                  %% our truncation.
                  VNodes; 
        [Id|_] -> Id 
    end,
    {ok, {PartSize, VNodeId}}.

find_target(PartSize, TargetVNode, HashKey) ->
    {ok, RingInfo} = adb_dist_store:get_ring_info(),
    {ok, SortedRingInfo} = adb_utils:sort_ring_info(RingInfo),
    StartPoint = PartSize * (TargetVNode - 1),
    Target = case lists:filter(fun({_, {Num, Den}}) -> HashKey =< trunc(StartPoint + (Num * PartSize) / Den) end, SortedRingInfo) of
        []           -> %% This adjustment is necessary for the case, the last element edge can be 
                        %% less than the theorical edge, not mapping some key. This is because of
                        %% our truncation.
                        {Tgt, _} = lists:last(SortedRingInfo),
                        Tgt;
        [{Tgt, _}|_] -> Tgt
    end,
    {ok, Target}.

lookup_call(Target, Request) ->
    lookup_call(Target, Request, []).

lookup_call(Target, Request, SearchList) ->
    case gen_server:call({adb_vnode_server, Target}, Request) of
        {error, Msg} -> 
            case adb_dist_server:find_next_node() of
                {ok, Next} when Next =:= node() -> {error, Msg};
                {ok, Next}                      -> 
                    case lists:search(fun(X) -> X =:= Next end, SearchList) of
                        {value, _} -> {error, Msg};
                        false      -> lookup_call(Next, Request, [node()|SearchList])
                    end
            end;
        Res           -> Res
    end.

bulk_lookup(_, [], Responses) -> Responses;
bulk_lookup(Database, [{Keys, Target, VNode}| Rest], Responses) ->
    Request = {process_request, {VNode, {bulk_lookup, Database, Keys}}},
    Response = gen_server:call({adb_vnode_server, Target, replicate}, Request),
    bulk_lookup(Database, Rest, [Response|Responses]).

get_hash_func() ->
    {ok, PartitionConfig} = adb_dist_store:get_config(partition),
    {consistent, HashFunc} = PartitionConfig,
    HashFunc.