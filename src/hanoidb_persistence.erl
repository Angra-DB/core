-module(hanoidb_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/1, connect/1, save/3, lookup/2, update/3, delete/2]).

setup([DBName]) ->
    {ok, Tree} = hanoidb:open_link(DBName),
    Tree.

connect(DB) ->
    {ok, Tree} = hanoidb:open(DB),
    ok.

createDB(DB) ->
    {ok, Tree} = hanoidb:open(DB),
    ok.

teardown(Tree) ->
    hanoidb:close(Tree).

save(Tree, Key, Value) ->
    {ok, Tree0} = hanoidb:open(Tree),
    ok = hanoidb:put(Tree0, list_to_binary(Key), list_to_binary(Value)),
    hanoidb:close(Tree0),
    Key.

lookup(Tree, Key) ->
  try
    {ok, Tree0} = hanoidb:open(Tree),
    {ok, Result} = hanoidb:get(Tree0, list_to_binary(Key)),
    hanoidb:close(Tree0),
    binary_to_list(Result)
  catch
    _Class:Err -> not_found
  end.

delete(Tree, Key) ->
  {ok, Tree0} = hanoidb:open(Tree),
  hanoidb:delete(Tree0, list_to_binary(Key)).

update(Tree, Key, Value) ->
    delete(Tree, Key),
    {ok, Tree0} = hanoidb:open(Tree),
    ok = hanoidb:put(Tree0, list_to_binary(Key), list_to_binary(Value)),
    hanoidb:close(Tree0),
    ok.
