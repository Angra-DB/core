%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. mar 2018 02:23
%%%-------------------------------------------------------------------
-module(writer).
-author("ftfnunes").

-behaviour(gen_server).

%% API
-export([start_link/2,
  create_db/2,
  connect/2,
  save/4,
  update/4,
  delete/3]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {db_name, vnode_id}).

%%%===================================================================
%%% API
%%%===================================================================

create_db(DbName, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {create_db}).
connect(DbName, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {connect}).
save(DbName, Value, Key, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {save, {Value, Key}}, 60000).
update(DbName, Value, Key, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {update, {Value, Key}}).
delete(DbName, Key, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {delete, {Key}}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName, VNodeId) ->
  gen_server:start_link({local, get_process_name(DbName, VNodeId)}, ?MODULE, [DbName, VNodeId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([DbName, VNodeId]) ->
  {ok, #state{db_name = DbName, vnode_id = VNodeId}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state {db_name = DbName, vnode_id = VNodeId}) ->
  case Request of
    {create_db} ->
      Reply = adbtree:create_db(DbName);
    {connect} ->
      Reply = adbtree:connect(DbName);
    {save, {Value, Key}} ->
      Reply =
        case adbtree:save(DbName, Value, Key) of
          {ok, {key, NewKey}, {ver, Version}} ->
            indexer:save(DbName, Value, NewKey, Version, VNodeId),
            {ok, {key, NewKey}, {ver, Version}};
          Else ->
            Else
        end;
    {update, {Value, Key}} ->
      Reply = adbtree:update(DbName, Value, Key),
      {ok, NewVersion} = Reply,
      indexer:update(DbName, Value, Key, NewVersion, VNodeId);
    {delete, {Key}} ->
      Reply = adbtree:delete(DbName, Key),
      indexer:delete(DbName, Key, VNodeId)
  end,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_name(DbName) ->
  list_to_atom(DbName++"_writer").

get_process_name(DbName, VNodeId) ->
	adb_utils:get_vnode_process(format_name(DbName), VNodeId).