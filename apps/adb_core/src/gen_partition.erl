%% ----------------------------------------------------------------------------
%% @author Ismael Medeiros <ismael.medeiros96@gmail.com>
%%
%% @doc Behavior for defining partition type, defined by configuration 
%% "partition" on the .app.src file.
%%
%% @end
%% ----------------------------------------------------------------------------
-module(gen_partition).

%% API functions.
-export([start/2, forward_request/3, multi_call/3, validate_response/3]).

%% Private functions (only exported on testing mode).
-ifdef(TEST).
-export([multi_call/4, generate_stats/1, choose_response/1]).
-endif.

%%=============================================================================
%% Data type definitions
%%=============================================================================

%% @type database_name() = string(). The database name.
-type database_name() :: string().

%% @type key() = string(). The document's key.
-type key() :: string().

%% @type document() = string(). The document.
-type document() :: string().

%% @type child_module() = atom(). The child module.
-type child_module() :: atom().

%% @type hash_func() = atom(). The hash function to be used on consistent
%% partition.
-type hash_func() :: atom().

%% @type child_response() = success_response() | failed_response(). The valid
%% child response.
-type child_response() :: success_response() | failed_response().

%% @type success_response() = {ok, any()}. The success response.
-type success_response() :: {ok, any()}.

%% @type failed_response() = {ok, any()}. The failed response.
-type failed_response() :: {error, any()}.

%% @type success_stats() = {success, {integer(), [any()]}}. The success
%% responses statistics.
-type success_stats() :: {success, {integer(), [any()]}}.

%% @type failed_stats() = {failed, {integer(), [any()]}}. The failed responses
%% statistics.
-type failed_stats() :: {failed, {integer(), [any()]}}.

%% @type response_stats() = [success_stats() | failed_stats()]. The responses
%% statistics.
-type response_stats() :: [success_stats() | failed_stats()].

%%=============================================================================
%% Behavior callbacks
%%=============================================================================

-callback create_db(database_name())                                             -> child_response().
-callback connect(database_name())                                               -> child_response().
-callback save(database_name(), {key(), hash_func()}, {integer(), document()})   -> child_response();
              (database_name(), key(), {integer(), document()})                  -> child_response().
-callback lookup(database_name(), {key(), hash_func()})                          -> child_response();
                (database_name(), key())                                         -> child_response().
-callback bulk_lookup(database_name(), {[key()], hash_func()})                   -> child_response();
                     (database_name(), [key()])                                  -> child_response().
-callback update(database_name(), {key(), hash_func()}, {integer(), document()}) -> child_response();
                (database_name(), key(), {integer(), document()})                -> child_response().
-callback delete(database_name(), {key(), hash_func()})                          -> child_response();
                (database_name(), key())                                         -> child_response().
-callback query_term(database_name(), any())                                     -> child_response().
-callback query(database_name(), any())                                          -> child_response().

%%=============================================================================
%% API functions
%%=============================================================================

%% @doc Starts gen_partition behavior. Note: This method should be modified,
%% when needed to set an initial configuration to the child.
%% 
%% @spec start(ChildModule, Arguments) -> ok
%%       ChildModule = child_module()
%%       Arguments = [any()]
%% 
%% @param Child. An atom identifying the child module that implements this
%%        behavior.
%% @param Args. Some arguments to be passed to Args.
%%
%% @returns The "ok" response.
%%
%% @end
-spec start(child_module(), [any()]) -> ok.
start(_Child, _Args) -> ok.

%% @doc Forwards some operation to the chosen child to be executed.
%% 
%% @spec forward_request(Operation, Arguments, ChildSpec) -> Response
%%       Operation = create_db | connect | save | save_key | lookup | bulk_lookup | update | delete | query_term | query
%%       Arguments = DatabaseName | {DatabaseName, Key} | {DatabaseName, Document} | {DatabaseName, {Key, Document}}
%%       Key = string(),
%%       Document = string(),
%%       DatabaseName = string()
%%       ChildSpec = atom() | {atom(), atom()}
%%       Response = success_response() | failed_response()
%% 
%% @param Database. The name of the database to be created.
%% @param Child. An atom identifying the child module that implements this
%%        behavior.
%%
%% @returns The response of the operation.
%%
%% @end
-spec forward_request(create_db, database_name(), {child_module(), hash_func()})      -> child_response();
    (create_db, database_name(), child_module())                                      -> child_response();
    (connect, database_name(), {child_module(), hash_func()})                         -> child_response();
    (connect, database_name(), child_module())                                        -> child_response();
    (save, {database_name(), integer(), document()}, {child_module(), hash_func()})              -> child_response();
    (save, {database_name(), integer(), document()}, child_module())                             -> child_response();
    (save_key, {database_name(), {key(), integer(), document()}}, {child_module(), hash_func()}) -> child_response();
    (save_key, {database_name(), {key(), integer(), document()}}, child_module())                -> child_response();
    (lookup, {database_name(), key()}, {child_module(), hash_func()})                 -> child_response();
    (lookup, {database_name(), key()}, child_module())                                -> child_response();
    (bulk_lookup, {database_name(), [key()]}, {child_module(), hash_func()})          -> child_response();
    (bulk_lookup, {database_name(), [key()]}, child_module())                         -> child_response();
    (update, {database_name(), {key(), integer(), document()}}, {child_module(), hash_func()})   -> child_response();
    (update, {database_name(), {key(), integer(), document()}}, child_module())                  -> child_response();
    (delete, {database_name(), key()}, {child_module(), hash_func()})                 -> child_response();
    (delete, {database_name(), key()}, child_module())                                -> child_response();
    (query_term, {database_name(), any()}, {child_module(), hash_func()})             -> child_response();
    (quary_term, {database_name(), any()}, child_module())                            -> child_response();
    (query, {database_name(), any()}, {child_module(), hash_func()})                  -> child_response();
    (quary, {database_name(), any()}, child_module())                                 -> child_response().

forward_request(create_db, Database, {Child, _HashFunc}) ->
    Child:create_db(Database);
forward_request(create_db, Database, Child) ->
    Child:create_db(Database);
forward_request(connect, Database, {Child, _HashFunc}) ->
    Child:connect(Database);
forward_request(connect, Database, Child) ->
    Child:connect(Database);
forward_request(save, {Database, {Size, Doc}}, {Child, HashFunc}) ->
    %% This command will be converted to a 'save_key' command, generating here
    %% a key for the document.
    Key = adb_utils:gen_key(),
    forward_request(save_key, {Database, {Key, Size, Doc}}, {Child, HashFunc});
forward_request(save, {Database, {Size, Doc}}, Child) ->
    %% This command will be converted to a 'save_key' command, generating here
    %% a key for the document.
    Key = adb_utils:gen_key(),
    forward_request(save_key, {Database, {Key, Size, Doc}}, Child);
forward_request(save_key, {Database, {Key, Size, Doc}}, {Child, HashFunc}) ->
    Child:save(Database, {Key, HashFunc}, {Size, Doc});
forward_request(save_key, {Database, {Key, Size, Doc}}, Child) ->
    Child:save(Database, Key, {Size, Doc});
forward_request(lookup, {Database, Key}, {Child, HashFunc}) ->
    Child:lookup(Database, {Key, HashFunc});
forward_request(lookup, {Database, Key}, Child) ->
    Child:lookup(Database, Key);
forward_request(bulk_lookup, {Database, Keys}, {Child, HashFunc}) ->
    Child:bulk_lookup(Database, {Keys, HashFunc});
forward_request(bulk_lookup, {Database, Keys}, Child) ->
    Child:bulk_lookup(Database, Keys);
forward_request(update, {Database, {Key, Size, Doc}}, {Child, HashFunc}) ->
    Child:update(Database, {Key, HashFunc}, {Size, Doc});
forward_request(update, {Database, {Key, Size, Doc}}, Child) ->
    Child:update(Database, Key, {Size, Doc});
forward_request(delete, {Database, Key}, {Child, HashFunc}) ->
    Child:delete(Database, {Key, HashFunc});
forward_request(delete, {Database, Key}, Child) ->
    Child:delete(Database, Key);
forward_request(query_term, {Database, Term}, {Child, _HashFunc}) ->
    Child:query_term(Database, Term);
forward_request(query_term, {Database, Term}, Child) ->
    Child:query_term(Database, Term);
forward_request(query, {Database, Query}, {Child, _HashFunc}) ->
    Child:query(Database, Query);
forward_request(query, {Database, Query}, Child) ->
    Child:query(Database, Query).

%% @doc Makes multiple calls to provided nodes.
%%
%% @spec multi_call(TargetNodes, ServerModule, Request) -> ResponseStats
%%       TargetNodes = [Node]
%%       Node = atom()
%%       ServerModule = atom()
%%       Request = any()
%%       ResponseStats = response_stats()
%% 
%% @param Nodes. This list of nodes to be contacted.
%% @param Server. The server module to be contacted.
%% @param Request. The request to be sent to server.
%%
%% @returns The statistics of the responses.
%%
%% @end
-spec multi_call([atom()], atom(), any()) -> response_stats().
multi_call(Nodes, Server, Request) ->
    multi_call(Nodes, Server, Request, []).


%% @doc Validates the responses according to Quorum configuration, located on
%% .app.src file. After validation, the function will, and return the response
%% selected by function choose_response().
%%
%% @spec validate_response(Stats, NumberTargets, Type) -> ChooseResponse
%%       Stats = [SuccessResList | FailureResList]
%%       SuccessResList = {success, {NumberRes, [SuccessResponse]}}
%%       FailureResList = {failed, {NumberRes, [FailureResponse]}}
%%       NumberRes = integer()
%%       SuccessResponse = any()
%%       FailureResponse = any()
%%       NumberTargets = integer()
%%       Type = write | read
%%       ChooseResponse = {atom(), any()}
%%
%% @param Stats. Data structure containing all success and failure responses.
%% @param NTargets. The number of contacted nodes, that will be used to
%%                  validate the responses.
%% @param Type. The type of operation ("read" or "write").
%% 
%% @returns The success response, if the operation was considered a success, or
%%          the failed response, if the operation was considered a failure.
%%
%% @end
-spec validate_response(response_stats(), integer(), write | read) -> 
    {success, any()} | {failed, any()} | {error, invalid_stats}.
validate_response(Stats, NTargets, Type) ->
    {ok, Quorum} = case Type of
        write -> adb_dist_store:get_config(write_quorum);
        read  -> adb_dist_store:get_config(read_quorum)
    end,
    {SuccessRequests, SResponses} = proplists:get_value(success, Stats),
    {FailRequests, FResponses} = proplists:get_value(failed, Stats),
    if 
        (SuccessRequests + FailRequests) > NTargets ->
            {error, invalid_stats};
        (Quorum > NTargets) and (SuccessRequests == NTargets) -> 
            {success, choose_response(SResponses)};
        SuccessRequests >= Quorum -> 
            {success, choose_response(SResponses)};
        true -> 
            {failed, choose_response(FResponses)}
    end.

%%==============================================================================
%% Internal functions
%%==============================================================================

%% @private
%% @doc Makes multiple calls to provided nodes. This method serve as helper to
%% multi_call/3 method by performing a tail recursion.
%%
%% @end
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

%% @private
%% @doc Generates statitics from responses.
%%
%% @spec generate_stats(Responses) -> ResponseStats
%%       Responses = [child_response()]
%%       ResponseStats = response_stats()
%% 
%% @param Responses. The list of responses from multiple calls to a list of
%% target nodes.
%%
%% @returns The statistics of the responses.
%%
%% @end
-spec generate_stats([child_response()]) -> response_stats().
generate_stats(Responses) ->
    SuccessRes = proplists:get_all_values(ok, Responses),
    FailedRes = proplists:get_all_values(error, Responses),
    [{success, 
        {length(SuccessRes), adb_utils:unique_list(SuccessRes)}}, 
    {failed, 
        {length(FailedRes), adb_utils:unique_list(FailedRes)}}].

%% @private
%% @doc Uses some strategy to choose the response in a list of responses. The
%% strategy used is always choose the first answer of the list, otherwise, in
%% case of empty list of responses, the function returns a empty list.

%% @spec choose_response(Responses) -> Response
%%       Response = [Response]
%%       Response = any()
%% 
%% @param Responses. The list of responses.
%%
%% @returns The select response.
%%
%% @end
-spec choose_response([])       -> none;
                     ([any()])  -> any().
choose_response([])           -> none;
choose_response([Response])   -> Response;
choose_response([Response|_]) -> Response.