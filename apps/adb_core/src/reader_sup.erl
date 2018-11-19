%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. mar 2018 01:57
%%%-------------------------------------------------------------------
-module(reader_sup).
-author("ftfnunes").

-behaviour(supervisor).

%% API
-export([start_link/2,
  start_child/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_child(DbName, VNodeId) ->
  supervisor:start_child(get_process_name(DbName, VNodeId), []).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName, VNodeId) ->
  supervisor:start_link({local, get_process_name(DbName, VNodeId)}, ?MODULE, [DbName, VNodeId]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
init([DbName]) ->
  RestartStrategy = simple_one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = permanent,
  Shutdown = 2000,
  Type = worker,

  AChild = {reader, {reader, start_link, [DbName]},
    Restart, Shutdown, Type, [reader]},

  {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_sup_name(DbName) ->
  list_to_atom(DbName++"_reader_sup").

get_process_name(DbName, VNodeId) ->
	adb_utils:get_vnode_process(format_sup_name(DbName), VNodeId).