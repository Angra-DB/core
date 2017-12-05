-module(adbtree_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/1, connect/1, save/3, lookup/2, update/3, delete/2]).

% -record(state, {pid_async=0}).

setup([DBName]) ->
    {ok, Tree} = adbtree:start(DBName),
    Tree.

spawn_async() ->
  % lager:info("[BEGIN] spawn_async:~n"),

  case whereis(adbtreeasync) of
    undefined ->
      adbtree_async:start(),
      % lager:info("[end] spawn_async:~n"),
      spawn_async();
    _ ->
      % lager:info("[end] spawn_async ok:~n"),
      ok
  end.
  % case #state.pid_async of
  %   0 ->
  %     PID=adbtree_async:start(),
  %     #state.pid_async=PID,
  %     lager:info("[END] spawn_async call start- #state.pid_async: ~p ~n",[#state.pid_async]),
  %     ok;
  %   _ ->
  %     lager:info("[END] spawn_async dont call start- #state.pid_async: ~p ~n",[#state.pid_async]),
  %     ok
  % end.

connect(DB) ->
  spawn_async(),
  adbtreeasync ! {connect, self(), DB},
  receive
    Response ->
      Response
  end.

createDB(DB) ->
  spawn_async(),
  adbtreeasync ! {create_db, self(), DB},
  receive
    Response ->
      Response
  end.

teardown(Tree) ->
  spawn_async(),
  adbtreeasync ! finished,
  adbtree:close(Tree).

save(Tree, Key, Value) ->
    spawn_async(),
    adbtreeasync ! {save, self(), Tree, Key, Value},
    receive
      Response ->
        Response
    end.

lookup(Tree, Key) ->
  spawn_async(),
  adbtreeasync ! {lookup, self(), Tree, Key},
  receive
    Response ->
      Response
  end.

delete(Tree, Key) ->
  spawn_async(),
  adbtreeasync ! {delete, self(), Tree, Key},
  receive
    Response ->
      Response
  end.

update(Tree, Key, Value) ->
  spawn_async(),
  adbtreeasync ! {update, self(), Tree, Key, Value},
  receive
    Response ->
      Response
  end.
