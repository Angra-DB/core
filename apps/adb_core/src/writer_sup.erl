%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. mar 2018 02:08
%%%-------------------------------------------------------------------
-module(writer_sup).
-author("ftfnunes").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
start_link(DbName, Args) ->
  supervisor:start_link({local, list_to_atom(DbName++"_writer_sup")}, ?MODULE, [DbName, Args]).

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
init([DbName, Args]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = permanent,
  Shutdown = 2000,
  Type = worker,

  WriterWorker = {DbName++"writer", {writer, start_link, [DbName]},
    Restart, Shutdown, Type, [writer]},
  IndexerWorker = {DbName++"indexer", {indexer, start_link, [DbName, Args]},
    Restart, Shutdown, Type, [indexer]},

  {ok, {SupFlags, [WriterWorker, IndexerWorker]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
