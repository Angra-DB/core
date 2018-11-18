%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. mar 2018 01:41
%%%-------------------------------------------------------------------
-module(database_sup).
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
  supervisor:start_link({local, list_to_atom(DbName)}, ?MODULE, [DbName, Args]).

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

  Restart = temporary,
  Shutdown = 2000,
  Type = supervisor,

  lager:info("DbName = ~p", [DbName]),

  ReaderChild = {DbName++"_reader", {reader_sup, start_link, [DbName]},
    Restart, Shutdown, Type, [reader_sup]},
  WriterChild = {DbName++"_writer", {writer_sup, start_link, [DbName, Args]},
    Restart, Shutdown, Type, [writer_sup]},
  ManagementChild = {DbName++"_man", {man_sup, start_link, [DbName]},
    Restart, Shutdown, Type, [man_sup]},
  QueryChild = {DbName++"_query", {query_sup, start_link, [DbName]},
    Restart, Shutdown, Type, [query_sup]},

  lager:info("Initializing database ~p", [DbName]),
  {ok, {SupFlags, [ReaderChild, WriterChild, ManagementChild, QueryChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
