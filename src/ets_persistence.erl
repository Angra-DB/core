-module(ets_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, save/3, lookup/2, update/3, delete/2]).

setup([DBName]) ->
  list_to_atom(DBName).

teardown(_) ->
  ok.

save(Tree, Key, Value) ->
    ets:insert(Tree, {Key, Value}),
    Key.

lookup(Tree, Key) ->
    ets:lookup(Tree, Key).

delete(Tree, Key) ->
    ets:delete(Tree, Key).

update(Tree, Key, Value) ->
    delete(Tree, Key),
    save(Tree, Key,Value).
