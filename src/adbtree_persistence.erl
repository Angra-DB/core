-module(adbtree_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/2, connect/2, save/3, lookup/2, update/3, delete/2, query_term/2]).

setup([DbName]) ->
    {ok, Tree} = adbtree:start(DbName),
    Tree.

connect(DbName, Args) ->
    persist_sup:spawn_if_exists(DbName, Args).

createDB(DbName, Args) ->
    {ok, _} = persist_sup:start_child(DbName, Args),
    writer:create_db(DbName).

teardown(_) ->
    ok.

save(DbName, Key, Value) ->
    {ok, {key, NewKey}, {ver, _Version}} = writer:save(atom_to_list(DbName), list_to_binary(Value), list_to_integer(Key, 16)),
    integer_to_list(NewKey, 16).

lookup(DbName, Key) ->
    {ok, Pid} = reader_sup:start_child(atom_to_list(DbName)),
    case reader:lookup(Pid, list_to_integer(Key, 16)) of
        {ok, _Version, Doc} ->
            {ok, Doc};
        Response ->
            Response
    end.

delete(DbName, Key) ->
    writer:delete(atom_to_list(DbName), list_to_integer(Key, 16)).

update(DbName, Key, Value) ->
    writer:update(atom_to_list(DbName), list_to_binary(Value), list_to_integer(Key, 16)).

query_term(DbName, Term) ->
    indexer:query_term(atom_to_list(DbName), Term).

