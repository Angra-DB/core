-module(hanoidb_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/2, connect/2, save/3, lookup/2, update/3, delete/2]).

setup([DBName]) ->
    {ok, Tree} = hanoidb:open_link(DBName),
    Tree.

connect(DB, _) ->
    ok. 

createDB(DB, _) ->
    ok. 

teardown(Tree) ->
    hanoidb:close(Tree).

save(Tree, Key, Value) ->
    hanoidb:put(Tree, list_to_binary(Key), list_to_binary(Value)), 
    Key.

lookup(Tree, Key) ->
  try
    {ok, Result} = hanoidb:get(Tree, list_to_binary(Key)),
    binary_to_list(Result)
  catch
    _Class:Err -> not_found	
  end.

delete(Tree, Key) ->
    hanoidb:delete(Tree, list_to_binary(Key)).

update(Tree, Key, Value) ->
    delete(Tree, Key),
    save(Tree, Key, Value).
