%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. mai 2018 21:29
%%%-------------------------------------------------------------------
-module(indexer).
-author("ftfnunes").

-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([start_link/3]).
-export([save/5]).
-export([update/5]).
-export([delete/3]).
-export([query_term/3]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {db_name, index, index_max_size}).

%%%===================================================================
%%% API
%%%===================================================================

save(DbName, Value, Key, Version, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {save, {Value, Key, Version}}, 60000).

update(DbName, Value, Key, Version, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {update, {Value, Key, Version}}).

delete(DbName, Key, VNodeId) ->
  gen_server:call(get_process_name(DbName, VNodeId), {delete, {Key}}).

query_term(DbName, Term, VNodeId) ->
  Name = get_process_name(DbName, VNodeId),
  CallResult = gen_server:call(Name, {query, Term}),
  CallResult.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName, VNodeId) ->
  start_link(DbName, VNodeId, []).

start_link(DbName, VNodeId, Args) ->
  IndexMaxSize = proplists:get_value(max_index_size, Args),
  gen_server:start_link({local, get_process_name(DbName, VNodeId)}, ?MODULE, [DbName, IndexMaxSize, VNodeId], []).

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
init([DbName, IndexMaxSize, VNodeId]) ->
  adbindexer:start_table(DbName),
  adbindexer:start_deletion_table(DbName),
  {ok, Pid} = reader_sup:start_child(DbName, VNodeId),
  {ok, #state{ db_name =  DbName, index = start_index(Pid, DbName), index_max_size = IndexMaxSize }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state {db_name = DbName, index = Index}) ->
  case Request of
    {query, Term} ->
      Reply = adbindexer:find(Term, Index, DbName, fun adbindexer:hash/1),
      NewIndex = Index;
    {save, {Value, Key, Version}} ->
      io:format("Saving document: ~s~n", [Value]),
      { ok, Tokens } = token_parser:receive_json(Value),
      AddedIndex = adbindexer:update_mem_index(Tokens, Key, Version, Index, DbName),
      Reply = ok,
      NewIndex = flush_if_full(AddedIndex, State);
    {update, {Value, Key, Version}} ->
      { ok, Tokens } = token_parser:receive_json(Value),
      lager:info("Saving document: ~p, with tokens ~p", [Value, Tokens]),
      AddedIndex = adbindexer:update_mem_index(Tokens, Key, Version, Index, DbName),
      Reply = ok,
      NewIndex = flush_if_full(AddedIndex, State);
    {delete, {Key}} ->
      adbindexer:add_deleted_doc(Key, DbName),
      Reply = ok,
      NewIndex = Index
  end,
  {reply, Reply, State#state{index = NewIndex}}.

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
terminate(_Reason, #state {index = Index, db_name = DbName}) ->
  save_index(Index, DbName),
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
  list_to_atom(DbName++"_indexer").

flush_if_full(AddedIndex, #state {db_name = DbName, index_max_size = MaxSize}) ->

  Size = erts_debug:flat_size(AddedIndex),
  lager:info("Index size: ~p", [Size]),
  case Size > MaxSize of
    true ->
      lager:info("~n~nFlushing Index~n~n"),
      Result = save_index(AddedIndex, DbName),
      lager:info("~n~nFlush Succeeded~n~n"),
      Result;
    _ ->
      AddedIndex
  end.

save_index(Index, DbName) ->
  IndexName = DbName++"Index.adbi",
  case file:open(IndexName, [read, binary]) of
    {error, enoent} ->
      adbindexer:save_index(Index, DbName, fun adbindexer:hash/1),
      [];
    {ok, Fp} ->
      file:close(Fp),
      adbindexer:update_index(Index, DbName, fun adbindexer:hash/1),
      []
  end.

start_index(Pid, DbName) ->
  Keys = adbindexer:recover_keys(DbName),
  start_index(Pid, Keys, [], DbName).

start_index(_, [], Index, _) ->
  Index;
start_index(Pid, [K | Keys], Index, DbName) ->
  case reader:lookup(Pid, K) of
    {ok, Version, Doc} ->
      { ok, Tokens } = token_parser:receive_json(list_to_binary(Doc)),
      NewIndex = adbindexer:update_mem_index(Tokens, K, Version, Index, DbName),
      start_index(Pid, Keys, NewIndex, DbName);
    not_found ->
      start_index(Pid, Keys, Index, DbName)
  end.

get_process_name(DbName, VNodeId) ->
	adb_utils:get_vnode_process(format_name(DbName), VNodeId).