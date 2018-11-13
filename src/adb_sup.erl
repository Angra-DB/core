%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. mar 2018 03:34
%%%-------------------------------------------------------------------
-module(adb_sup).
-author("ftfnunes").

-behaviour(supervisor).

%% API
-export([start_link/1]).

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
start_link(Args) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, [Args]).

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
init([Args]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = permanent,
  Shutdown = 2000,
  Type = supervisor,

  ServerSup = {server_sup, {server_sup, start_link, [Args]},
    Restart, Shutdown, Type, [server_sup]},
  PersistSup = {persist_sup, {persist_sup, start_link, []},
    Restart, Shutdown, Type, [persist_sup]},

  % The order of these child modules (PersistSup, ServerSup) matter!! ServerSup needs PersistSup to be up-and-running while initializing.
  {ok, {SupFlags, [PersistSup, ServerSup]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
