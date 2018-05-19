-module(ets_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/2, connect/2, save/3, lookup/2, update/3, delete/2]).

setup([]) -> 
  none;

setup([DBName]) ->
  list_to_atom(DBName).

teardown(_) ->
  ok.

createDB(DB, _) ->
    DBName = list_to_atom(DB),
    _NDB = ets:new(DBName, [set, public, named_table]),
    {ok}.

connect(DB, _) ->
    DBName = list_to_atom(DB),
    case ets:info(DBName) of
       undefined -> db_does_not_exist;
       _ -> ok
    end. 

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
