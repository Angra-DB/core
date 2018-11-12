-module(full_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, update/3, delete/2]).

%%=============================================================================
%% gen_partition callbacks
%%=============================================================================

create_db(Database) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {create_db, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), write) of
        {success, _Res}                        -> {ok, Database};
        {failed, {badmatch, {error, eexist}}}  -> {error, already_exists};
        {failed, Response}                     -> {error, Response};
        {error, Reason}                        -> {error, Reason}
    end.

connect(Database) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {connect, Database, Database}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

save(Database, Key, Doc) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {save_key, Database, {Key, Doc}}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

lookup(Database, Key) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {lookup, Database, Key}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

update(Database, Key, Doc) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {update, Database, {Key, Doc}}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

delete(Database, Key) ->
    Targets = [node()|nodes()],
    Request = {process_request, {all, {delete, Database, Key}}},
    ResponseStats = gen_partition:multi_call(Targets, adb_vnode_server, Request),
    case gen_partition:validate_response(ResponseStats, length(Targets), read) of
        {success, Response} -> {ok, Response};
        {failed, Response}  -> {error, Response};
        {error, Reason}     -> {error, Reason}
    end.

%%==============================================================================
%% Internal functions
%%==============================================================================