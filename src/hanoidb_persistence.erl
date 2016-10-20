-module(hanoidb_persistence).

-behaviour(gen_persistence).

-export([save/2, lookup/1, update/2, delete/1]).

save(Key, Value) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    hanoidb:put(Tree, list_to_binary(Key), list_to_binary(Value)), 
    hanoidb:close(Tree),
    Key.

lookup(Key) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    Result = hanoidb:get(Tree, list_to_binary(Key)),
    hanoidb:close(Tree),
    Result.

delete(Key) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    Result = hanoidb:delete(Tree, list_to_binary(Key)),
    hanoidb:close(Tree),
    Result.

update(Key, Value) ->
    delete(Key),
    save(Key,Value).
