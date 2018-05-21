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
-export([start_link/1,
  create_db/1,
  connect/1,
  save/3,
  update/3,
  delete/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {db_name}).

%%%===================================================================
%%% API
%%%===================================================================

create_db(DbName) ->
  gen_server:call(format_name(DbName), {create_db}).
connect(DbName) ->
  gen_server:call(format_name(DbName), {connect}).
save(DbName, Value, Key) ->
  gen_server:call(format_name(DbName), {save, {Value, Key}}).
update(DbName, Value, Key) ->
  gen_server:call(format_name(DbName), {update, {Value, Key}}).
delete(DbName, Key) ->
  gen_server:call(format_name(DbName), {delete, {Key}}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName) ->
  gen_server:start_link({local, format_name(DbName)}, ?MODULE, [DbName], []).

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
init([DbName]) ->
  {ok, #state{db_name = DbName}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state {db_name = DbName}) ->
  case Request of
    {create_db} ->
      Reply = adbtree:create_db(DbName);
    {connect} ->
      Reply = adbtree:connect(DbName);
    {save, {Value, Key}} ->
      Reply = adbtree:save(DbName, Value, Key),
      {ok, {key, NewKey}, {ver, Version}} = Reply,
      indexer:save(DbName, Value, NewKey, Version);
    {update, {Value, Key}} ->
      Reply = adbtree:update(DbName, Value, Key),
      {ok, NewVersion} = Reply,
      indexer:update(DbName, Value, Key, NewVersion);
    {delete, {Key}} ->
      Reply = adbtree:delete(DbName, Key),
      indexer:delete(DbName, Key)
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