-module(gen_partition).

%% API
-export([start/2, forward_request/3, multi_call/3, validate_response/3]).

%% Behavior Specs
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{create_db, 1}, {connect, 1}, {save, 3}, {lookup, 2}, {update, 3}, {delete, 2}].

%%=============================================================================
%% API functions
%%=============================================================================

start(_Child, _Args) -> ok.

%% create_db operation.
forward_request(create_db, Database, Child) ->
    Child:create_db(Database);

%% connect_db operation.
forward_request(connect, Database, Child) ->
    Child:connect(Database);

%% save operation.
forward_request(save, {Database, Doc}, Child) ->
    %% This command will be converted to a 'save_key' command, generating here a key
    %% for the document.
    Key = adb_utils:gen_key(),
    forward_request(save_key, {Database, {Key, Doc}}, Child);

%% save_key operation.
forward_request(save_key, {Database, {Key, Doc}}, {Child, HashFunc}) ->
    Child:save(Database, {Key, HashFunc}, Doc);
forward_request(save_key, {Database, {Key, Doc}}, Child) ->
    Child:save(Database, Key, Doc);

%% lookup operation.
forward_request(lookup, {Database, Key}, {Child, HashFunc}) ->
    Child:lookup(Database, {Key, HashFunc});
forward_request(lookup, {Database, Key}, Child) ->
    Child:lookup(Database, Key);

%% update operation.
forward_request(update, {Database, {Key, Doc}}, {Child, HashFunc}) ->
    Child:update(Database, {Key, HashFunc}, Doc);
forward_request(update, {Database, {Key, Doc}}, Child) ->
    Child:update(Database, Key, Doc);

%% delete operation.
forward_request(delete, {Database, Key}, {Child, HashFunc}) ->
    Child:delete(Database, {Key, HashFunc});
forward_request(delete, {Database, Key}, Child) ->
    Child:delete(Database, Key).

multi_call(Nodes, Server, Request) ->
    multi_call(Nodes, Server, Request, []).

%%==============================================================================
%% Internal functions
%%==============================================================================
multi_call([], _, _, Responses) ->
    generate_stats(Responses);
multi_call(Nodes, Server, Request, Responses) when is_list(Nodes) ->
    [Target|Rest] = Nodes,
    Res = case gen_server:call({Server, Target}, Request) of
        ok       -> {ok, []};
        error    -> {error, []};
        Response -> Response
    end,
    multi_call(Rest, Server, Request, [Res|Responses]).

generate_stats(Responses) ->
    SuccessRes = proplists:get_all_values(ok, Responses),
    FailedRes = proplists:get_all_values(error, Responses),
    [{success, 
        {length(SuccessRes), adb_utils:unique_list(SuccessRes)}}, 
    {failed, 
        {length(FailedRes), adb_utils:unique_list(FailedRes)}}].

validate_response(Stats, Targets, Type) ->
    {ok, Quorum} = case Type of
        write -> adb_dist_store:get_config(write_quorum);
        read  -> adb_dist_store:get_config(read_quorum)
    end,
    {SuccessRequests, Responses} = proplists:get_value(success, Stats),
    if 
        (Quorum > length(Targets)) and 
            (SuccessRequests == length(Targets)) -> {success, choose_response(Responses)};
        SuccessRequests >= Quorum                -> {success, choose_response(Responses)};
        true                                     -> {_, FailRes} = proplists:get_value(failed, Stats),
                                                    {failed, choose_response(FailRes)}
    end.

choose_response(ResList) when is_list(ResList) ->
    %% Strategy used: Always return the first response.
    case ResList of
        []           -> [];
        [Response]   -> Response;
        [Response|_] -> Response
    end.