-module(adbtree_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/1, connect/1, save/3, lookup/2, update/3, delete/2]).

setup([DBName]) ->
    {ok, Tree} = adbtree:start(DBName),
    Tree.

connect(DB) ->
    case adbtree:connect(DB) of
        {ok, Tree} ->
            ok;
        {error, enoent} ->
            db_does_not_exist
    end. 

createDB(DB) ->
    adbtree:create_db(DB). 

teardown(Tree) ->
    adbtree:close(Tree).

save(Tree, Key, Value) ->
    adbtree:save(atom_to_list(Tree), list_to_binary(Value), list_to_integer(Key, 16)),
    Key.

lookup(Tree, Key) ->
  try
    {ok, _Version, Doc} = adbtree:lookup(atom_to_list(Tree), list_to_integer(Key, 16)),
    binary_to_list(Doc)
  catch
    _Class:_Err -> not_found	
  end.

delete(Tree, Key) ->
    adbtree:delete(atom_to_list(Tree), list_to_integer(Key, 16)).

update(Tree, Key, Value) ->
    adbtree:update(atom_to_list(Tree), list_to_binary(Value), list_to_integer(Key, 16)),
    ok.

