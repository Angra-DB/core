-module(consistent_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, update/3, delete/2]).

-define(HASH_SPACES, [{md5, 16}, {sha, 20}, {sha256, 32}, {sha384, 48}, {sha512, 64}]).

%%=============================================================================
%% gen_partition callbacks
%%=============================================================================

create_db(Database) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {create_db, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, Targets, write) of
        {success, _Res}                        -> {ok, Database};
        {failed, {badmatch, {error, eexist}}}  -> {error, already_exists};
        {failed, Response}                     -> {error, Response}
    end.

connect(Database) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {connect, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, Targets, read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response}
    end.

save(Database, {Key, HashFunc}, Doc) ->
    {ok, {HashKey, Target, VNode}} = map_key(HashFunc, Key),
    Request = {process_request, {VNode, {save_key, Database, {crypto:bytes_to_integer(HashKey), Doc}}}},
    gen_server:call({adb_vnode_server, Target, replicate}, Request).

lookup(Database, {Key, HashFunc}) ->
    {ok, {HashKey, Target, VNode}} = map_key(HashFunc, Key),
    Request = {process_request, {VNode, {lookup, Database, crypto:bytes_to_integer(HashKey)}}},
    lookup_call(Target, Request).

update(Database, {Key, HashFunc}, Doc) ->
    {ok, {HashKey, Target, VNode}} = map_key(HashFunc, Key),
    Request = {process_request, {VNode, {update, Database, {crypto:bytes_to_integer(HashKey), Doc}}}},
    gen_server:call({adb_vnode_server, Target, replicate}, Request).

delete(Database, {Key, HashFunc}) ->
    {ok, {HashKey, Target, VNode}} = map_key(HashFunc, Key),
    Request = {process_request, {VNode, {delete, Database, crypto:bytes_to_integer(HashKey)}}},
    gen_server:call({adb_vnode_server, Target, replicate}, Request).

%%==============================================================================
%% Internal functions
%%==============================================================================

map_key(HashFunc, Key) ->
    {ok, HashSize} = validate_hash(HashFunc),
    HashKey = crypto:bytes_to_integer(crypto:hash(HashFunc, Key)),
    {ok, {PartSize, TargetVNode}} = find_target_vnode(HashSize, HashKey),
    {ok, Target} = find_target(PartSize, TargetVNode, HashKey),
    {ok, {HashKey, Target, TargetVNode}}.

validate_hash(HashFunc) ->
    case proplists:lookup(HashFunc, ?HASH_SPACES) of
        none     -> {error, invalid_hash};
        HashSize -> math:pow(2, HashSize),
                    {ok, HashSize}
    end.

find_target_vnode(HashSize, HashKey) when is_integer(HashSize) ->
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    PartSize = HashSize / VNodes,
    [VNodeId|_] = lists:filter(fun(X) -> HashKey =< (PartSize * X) end, lists:seq(1, VNodes)), %% Forces to always return the first Id that satify the predicate.
    {ok, VNodeId}.

find_target(PartSize, TargetVNode, HashKey) ->
    {ok, RingInfo} = adb_dist_store:get_ring_info(),
    {ok, SortedRingInfo} = adb_utils:sort_ring_info(RingInfo),
    StartPoint = PartSize * TargetVNode,
    [{Target, _}|_] = lists:filter(fun({_, {Num, Den}}) -> HashKey =< (StartPoint + ((Num * PartSize) / Den)) end, SortedRingInfo),
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

