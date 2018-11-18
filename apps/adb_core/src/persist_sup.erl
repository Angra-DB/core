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

spawn_if_exists(DbName, Args) ->
  case adbtree:exists(DbName) of
    true ->
      case supervisor:start_child(?SERVER, children_spec(DbName, Args)) of
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        Error -> Error
      end;
    false ->
      db_does_not_exist
  end.

start_child(DbName, Args) ->
  supervisor:start_child(?SERVER, children_spec(DbName, Args)).

children_spec(DbName, Args) ->
  Restart = permanent,
  Shutdown = 2000,
  Type = supervisor,

  {DbName, {database_sup, start_link, [DbName, Args]},
    Restart, Shutdown, Type, [database_sup]}.

%%--------------------------------------------------------------------
%% Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%--------------------------------------------------------------------
init([]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
