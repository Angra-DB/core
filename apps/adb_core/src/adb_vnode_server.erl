-module(adb_vnode_server).
-behavior(gen_server).

%% API
-export([start_link/0, get_count/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% ============================================================================
%% API functions
%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:cast(?SERVER, stop).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init(_Args) ->
    lager:info("Initializing adb_vnode server.~n"),
    {ok, [], 0}.

handle_call({process_request, {Target, Args}}, _From, State) ->
    Response = case Target of
        all -> send_to_all_vnodes({process_request, Args});
        Id  -> gen_server:call(get_vnode_name(Id), {process_request, Args})
    end,
    {reply, Response, State};
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

send_to_all_vnodes(Request) ->
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    Responses = send_to_all_vnodes(Request, VNodes),
    SuccessRes = proplists:get_all_values(ok, Responses),
    FailedRes = proplists:get_all_values(error, Responses),
    {Result, Res} = if 
        length(SuccessRes) =:= VNodes -> {ok, adb_utils:unique_list(SuccessRes)};
        true                          -> {error, adb_utils:unique_list(FailedRes)}
    end,
    {Result, choose_response(Res)}.

send_to_all_vnodes(Request, VNodes) ->
    send_to_all_vnodes(Request, [], VNodes).

send_to_all_vnodes(_, Responses, 0) -> Responses;
send_to_all_vnodes(Request, Responses, Acc) ->
    Res = case gen_server:call(get_vnode_name(Acc), Request) of 
        ok                -> {ok, []};
        {ok, Response}    -> {ok, Response};
        {error, Response} -> {error, Response};
        db_does_not_exist -> {ok, db_does_not_exist};
        Response          -> {ok, Response}
    end,
    send_to_all_vnodes(Request, [Res|Responses], Acc-1).

get_vnode_name(VirtualId) ->
    list_to_atom(atom_to_list(adb_persistence_) ++ integer_to_list(VirtualId)).

choose_response(ResList) when is_list(ResList) ->
    %% Strategy used: Always return the first response.
    case ResList of
        []           -> [];
        [Response]   -> Response;
        [Response|_] -> Response
    end.