-module(adbtree_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/1, connect/1, save/3, lookup/2, update/3, delete/2]).

setup([DBName]) ->
    {ok, Tree} = adbtree:start(DBName),
    Tree.

connect(DB) ->
    case adbtree:start(DB) of
        {ok, Tree} ->
            adbtree:close(Tree),
            ok;
        {error, enoent} ->
            db_does_not_exist
    end. 

createDB(DB) ->
    adbtree:create_db(DB). 

teardown(Tree) ->
    adbtree:close(Tree).

save(Tree, Key, Value) ->
    {ok, Btree} = adbtree:start(atom_to_list(Tree)),
    adbtree:save(Btree, list_to_binary(Value), list_to_integer(Key, 16)),
    adbtree:close(Btree),
    Key.

lookup(Tree, Key) ->
  try
    {ok, Btree} = adbtree:start(atom_to_list(Tree)),
    {ok, _Version, Doc} = adbtree:lookup(Btree, list_to_integer(Key, 16)),
    adbtree:close(Btree),
    binary_to_list(Doc)
  catch
    _Class:_Err -> not_found	
  end.

delete(Tree, Key) ->
    {ok, Btree} = adbtree:start(atom_to_list(Tree)),
    adbtree:delete(Btree, list_to_integer(Key, 16)),
    adbtree:close(Btree).

update(Tree, Key, Value) ->
    {ok, Btree} = adbtree:start(atom_to_list(Tree)),
    adbtree:update(Btree, list_to_binary(Value), list_to_integer(Key, 16)),
    adbtree:close(Btree),
    ok.

