-module(adbpersistence).
-include("adbpersistence.hrl").
-import(adbtree).
-import(adbindexer).
-compile(export_all).

initialize_db(DbName) ->
    initialize_index(DbName).

initialize_index(DbName) ->
    adbindexer:start_table(DbName),
    {ok, Fp} = file:open(DbName++"Versions.adbi", [read, write, binary]),
    restore_index(Fp, []).

restore_index(Fp, DbName, Index) ->
    case file:read(Fp, ?SizeOfDocKey+?SizeOfVersion) of
		{ok, <<DocKey:?SizeOfDocKey/unit:8, DocVersion:?SizeOfVersion/unit:8>>} ->
            {ok, Document, {ver, CurrentVersion}} = adbtree:lookup(DbName, DocKey),
            NewIndex = adbindexer:update_mem_index(tokenize(Document), DocKey, CurrentVersion, Index, DbName),
            restore_index(Fp, DbName, NewIndex);
		eof ->
            Index
	end.

create_doc(Document, Key, DbName, MemIndex) ->
    DocumentBin = list_to_binary(Document),
    Result = adbtree:save(DbName, Document, Key),
    {ok, _, {ver, DocVersion}} = Result,
    NewIndex = adbindexer:update_mem_index(tokenize(Document), Key, DocVersion, MemIndex, DbName),
    {Result, NewIndex}.

% TODO: update, search for a term, search by key and delete.
