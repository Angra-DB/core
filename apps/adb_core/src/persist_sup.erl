%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. mar 2018 16:42
%%%-------------------------------------------------------------------
-module(persist_sup).
-author("ftfnunes").

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([spawn_if_exists/2]).
-export([start_child/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

spawn_if_exists(DbName, VNodeId) ->
	SupName = adb_utils:get_vnode_process(?MODULE, VNodeId)
  case adbtree:exists(DbName) of
    true ->
      case supervisor:start_child(SupName, children_spec(DbName, VNodeId)) of
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        Error -> Error
      end;
    false ->
      db_does_not_exist
  end.

start_child(DbName, VNodeId) ->
	SupName = adb_utils:get_vnode_process(?MODULE, VNodeId)
  supervisor:start_child(SupName, children_spec(DbName)).

children_spec(DbName, VNodeId) ->
  Restart = permanent,
  Shutdown = 2000,
  Type = supervisor,

  {DbName, {database_sup, start_link, [DbName, VNodeId]},
    Restart, Shutdown, Type, [database_sup]}.

%%--------------------------------------------------------------------
%% Starts the supervisor
%%--------------------------------------------------------------------
start_link(Persistence, Id) ->
  SupName = adb_utils:get_vnode_process(?MODULE, Id),
  supervisor:start_link({local, SupName}, ?MODULE, [Persistence, Id]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%--------------------------------------------------------------------
init([Persistence, Id]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,
  PersistServer = #{id       => adb_utils:get_vnode_process(adb_persistence, Id), 
						  start    => {adb_persistence, start_link, [Persistence, Id]},
						  restart  => permanent, 
						  shutdown => brutal_kill, 
						  type     => worker, 
						  modules  => [adb_persistence]},
  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
  {ok, {SupFlags, [PersistServer]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
