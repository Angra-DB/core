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
        Id  -> Target = adb_utils:get_vnode_name(Id),
               Request = {process_request, Args},
               gen_server:call(Target, Request)
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
    Responses = collect_responses(VNodes, Request),
    SuccessRes = proplists:get_all_values(ok, Responses),
    FailedRes = proplists:get_all_values(error, Responses),
    generate_final_response(SuccessRes, FailedRes, length(SuccessRes) =:= VNodes).

collect_responses(LastIndex, Request) ->
    collect_responses(LastIndex, Request, []).

collect_responses(0, _Request, Responses) -> Responses;
collect_responses(Index, Request, Responses) ->
    {ok, Target} = adb_utils:get_vnode_name(Index),
    Response = gen_server:call(Target, Request),
    collect_responses(Index-1, Request, [Response|Responses]).

generate_final_response(SuccessRes, _, true) ->
    Successes = adb_utils:unique_list(SuccessRes),
    Response = choose_response(Successes),
    {ok, Response};
generate_final_response(_, FailedRes, false) ->
    Fails = adb_utils:unique_list(FailedRes),
    Response = choose_response(Fails),
    {error, Response}.

%% Strategy used: Always return the first response.
-spec choose_response([])     -> [];
                     (list()) -> term().
choose_response([]) -> [];
choose_response([Response|_]) -> Response.