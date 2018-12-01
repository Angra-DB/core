-module(adbtree_persistence).

-behaviour(gen_persistence).

-export([setup/1, teardown/1, createDB/2, connect/2, save/4, lookup/3, update/4, delete/3, query_term/3, query/3, bulk_lookup/3]).

setup([DbName]) ->
    {ok, Tree} = adbtree:start(DbName),
    Tree.

connect(DbName, VNodeId) ->
    persist_sup:spawn_if_exists(DbName, VNodeId).

createDB(DbName, VNodeId) ->
    {ok, _} = persist_sup:start_child(DbName, VNodeId),
    writer:create_db(DbName, VNodeId).

teardown(_) ->
    ok.

save(DbName, Key, Value, VNodeId) ->
    case writer:save(atom_to_list(DbName), list_to_binary(Value), list_to_integer(Key, 16), VNodeId) of
        {ok, {key, NewKey}, {ver, _Version}} ->
            integer_to_list(NewKey, 16);
        Else ->
            lager:info("~p", [Else]),
            Else
    end.
    

lookup(DbName, Key, VNodeId) ->
    {ok, Pid} = reader_sup:start_child(atom_to_list(DbName), VNodeId),
    case reader:lookup(Pid, list_to_integer(Key, 16)) of
        {ok, _Version, Doc} ->
            {ok, Doc};
        Response ->
            Response
    end.

bulk_lookup(DbName, Keys, VNodeId) ->
    {ok, Pid} = reader_sup:start_child(atom_to_list(DbName), VNodeId),
    {ok, bulk_lookup_(Pid, Keys, VNodeId)}.

bulk_lookup_(_, [], _) ->
    [];

bulk_lookup_(ReaderPid, [K | Keys], VNodeId) ->
    case reader:lookup(ReaderPid, list_to_integer(K, 16)) of
        {ok, _Version, Doc} ->
            [Doc | bulk_lookup_(ReaderPid, Keys, VNodeId)];
        not_found ->
            lager:info("Key not found: ~p", [K]),
            bulk_lookup_(ReaderPid, Keys, VNodeId)
    end.

delete(DbName, Key, VNodeId) ->
    writer:delete(atom_to_list(DbName), list_to_integer(Key, 16), VNodeId).

update(DbName, Key, Value, VNodeId) ->
    writer:update(atom_to_list(DbName), list_to_binary(Value), list_to_integer(Key, 16), VNodeId).

query_term(DbName, Term, VNodeId) ->
    Response = indexer:query_term(DbName, Term, VNodeId),
    lager:info("Reponse = ~p", [Response]),
    Response.

query(DbName, Query, VNodeId) ->
  Response = query_server:process_query(atom_to_list(DbName), Query, VNodeId),
  lager:info("Reponse = ~p", [Response]),
  Response.

