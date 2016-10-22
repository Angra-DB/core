-module(ets_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, save/3, lookup/2, update/3, delete/2]).

setup(_) ->
  ok.

teardown(_) ->
  ok.

save(_, Key, Value) ->
    ets:insert(docs, {Key, Value}),
    Key.

lookup(_, Key) ->
    ets:lookup(docs, Key).

delete(_, Key) ->
    ets:delete(docs, Key).

update(Args, Key, Value) ->
    delete(Args, Key),
    save(Args, Key,Value).
