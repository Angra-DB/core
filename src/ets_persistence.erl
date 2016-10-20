-module(ets_persistence).

-behaviour(gen_persistence).

-export([save/2, lookup/1, update/2, delete/1]).

save(Key, Value) ->
    ets:insert(docs, {Key, Value}),
    Key.

lookup(Key) -> 
    ets:lookup(docs, Key).

delete(Key) ->
    ets:delete(docs, Key).

update(Key, Value) ->
    delete(Key),
    save(Key,Value).
