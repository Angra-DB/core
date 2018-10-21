-module(consistent_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, update/3, delete/2]).

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
    HashKey = crypto:hash(HashFunc, Key),
    {Target, VNode} = get_target_node(HashKey),
    Request = {process_request, {VNode, {save, Database, {crypto:bytes_to_integer(HashKey), Doc}}}},
    gen_server:call({adb_vnode_server, Target}, Request).

lookup(Database, {Key, HashFunc}) ->
    HashKey = crypto:hash(HashFunc, Key),
    {Target, VNode} = get_target_node(HashKey),
    Request = {process_request, {VNode, {lookup, Database, crypto:bytes_to_integer(HashKey)}}},
    gen_server:call({adb_vnode_server, Target}, Request).

update(Database, {Key, HashFunc}, Doc) ->
    HashKey = crypto:hash(HashFunc, Key),
    {Target, VNode} = get_target_node(HashKey),
    Request = {process_request, {VNode, {update, Database, {crypto:bytes_to_integer(HashKey), Doc}}}},
    gen_server:call({adb_vnode_server, Target}, Request).

delete(Database, {Key, HashFunc}) ->
    HashKey = crypto:hash(HashFunc, Key),
    {Target, VNode} = get_target_node(HashKey),
    Request = {process_request, {VNode, {delete, Database, crypto:bytes_to_integer(HashKey)}}},
    gen_server:call({adb_vnode_server, Target}, Request).

%%==============================================================================
%% Internal functions
%%==============================================================================

get_target_node(_HashKey) ->
    %% TODO
    ok.

