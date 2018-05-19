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
-export([start_link/1]).
-export([start_link/2]).

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

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName) ->
  lager:info("Args in indexer", []),
  start_link(DbName, []).

start_link(DbName, Args) ->
  IndexMaxSize = proplists:get_value(max_index_size, Args),
  lager:info("Args in indexer ~p", [IndexMaxSize]),
  gen_server:start_link({local, format_name(DbName)}, ?MODULE, [DbName, IndexMaxSize], []).

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
init([DbName, IndexMaxSize]) ->
  adbindexer:start_table(DbName),
  adbindexer:start_deletion_table(DbName),
  {ok, #state{ db_name =  DbName, index = [], index_max_size = IndexMaxSize }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state {db_name = DbName, index = Index}) ->
  case Request of
    {save, {Value, Key, Version}} ->
      { ok, Tokens } = token_parser:receive_json(Value),
      AddedIndex = adbindexer:update_mem_index(Tokens, Key, Version, Index, DbName),
      NewIndex = flush_if_full(AddedIndex);
    {update, {Value, Key, Version}} ->
      { ok, Tokens } = token_parser:receive_json(Value),
      AddedIndex = adbindexer:update_mem_index(Tokens, Key, Version, Index, DbName),
      NewIndex = flush_if_full(AddedIndex);
    {delete, {Key}} ->
      adbindexer:add_deleted_doc(Key, DbName),
      NewIndex = Index
  end,
  {reply, ok, State#state{index = NewIndex}}.

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
  list_to_atom(DbName++"_indexer").

flush_if_full(AddedIndex) ->
  erlang:error(not_implemented).