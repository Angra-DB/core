-module(adbindexer).
-include("adbindexer.hrl").
-compile(export_all).

% InvertedIndex -> index that is being built

create_index(TokenList, DocKey, DocVersion, DbName) ->
	add_doc_version(DocKey, DocVersion, DbName),
	create_index(TokenList, DocKey, DocVersion).

create_index([], _DocKey, _DocVersion) ->
	[];

create_index([_Token = #token{ word = Word, docPos = DocPos } | TokenList], DocKey, DocVersion) ->
	Index = insert_token(Word, #posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion}, []),
	update_mem_index(TokenList, DocKey, DocVersion, Index);

create_index([_Token = #token_ext{ word = Word, docPos = DocPos, fieldStart = FieldStart, fieldEnd = FieldEnd } | TokenList], DocKey, DocVersion) ->
	Index = insert_token(Word, #posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd}, []),
	update_mem_index(TokenList, DocKey, DocVersion, Index).

update_mem_index(TokenList, DocKey, DocVersion, Index, DbName) ->
	add_doc_version(DocKey, DocVersion, DbName),
	update_mem_index(TokenList, DocKey, DocVersion, Index).

update_mem_index([], _DocKey, _DocVersion, Index) ->
	Index;

update_mem_index([_Token = #token{ word = Word, docPos = DocPos }  | TokenList], DocKey, DocVersion, Index) ->
	NewIndex = insert_token(Word, #posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion}, Index),
	update_mem_index(TokenList, DocKey, DocVersion, NewIndex);

update_mem_index([_Token = #token_ext{ word = Word, docPos = DocPos, fieldStart = FieldStart, fieldEnd = FieldEnd }  | TokenList], DocKey, DocVersion, Index) ->
	NewIndex = insert_token(Word, #posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd}, Index),
	update_mem_index(TokenList, DocKey, DocVersion, NewIndex).

insert_token(Word, Posting, Index) ->
	case find_term(Index, Word) of
		not_found ->
			case Posting of
				#posting{} ->
					insert_term(Index, #term{word = Word, normalPostings = 1, extPostings = 0, postings = [Posting], nextTerm = 0});
				#posting_ext{} ->
					insert_term(Index, #term{word = Word, normalPostings = 0 , extPostings = 1, postings = [Posting], nextTerm = 0})
			end;
		Term ->
			NewTerm = insert_posting(Term, Posting),
			update_term(Index, NewTerm)
	end.

find_term(Index, Word) ->
	Length = length(Index),
	Middle = (Length + 1) div 2, %% saves us hassle with odd/even indexes

	case Middle of
	0 -> not_found; %% empty list -> item not found
	_ ->
		Term = lists:nth(Middle, Index),

		case Term#term.word of
			Word -> Term; %% yay, found it!
			_ -> case Term#term.word > Word of
			       true  -> find_term(lists:sublist(Index, Length - Middle), Word); %% LT, search on left side
			       false -> find_term(lists:nthtail(Middle, Index), Word)           %% GT, search on right side
			     end
		end
  	end.

insert_posting(Term = #term{postings = Postings, normalPostings = NormalPostings, extPostings = ExtPostings}, NewPosting) ->
	NewPostings = insert_posting(Postings, NewPosting),
	case NewPosting of
		#posting{} ->
			Term#term{postings = NewPostings, normalPostings = NormalPostings + 1};
		#posting_ext{} ->
			Term#term{postings = NewPostings, extPostings = ExtPostings + 1}
	end;


insert_posting([], NewPosting) ->
	[NewPosting];

insert_posting([P | Postings], NewPosting) ->
	CurDocKey = extract_key_from_record(P),
	NewDocKey = extract_key_from_record(NewPosting),
	case CurDocKey < NewDocKey of
		true ->
			NewPostings = insert_posting(Postings, NewPosting),
			[P | NewPostings];
		false ->
			[NewPosting | [P | Postings]]
	end.


extract_key_from_record(#posting_ext{docKey = DocKey}) ->
	DocKey;
extract_key_from_record(#posting{docKey = DocKey}) ->
	DocKey.

extract_version_from_record(#posting_ext{docVersion = DocVersion}) ->
	DocVersion;
extract_version_from_record(#posting{docVersion = DocVersion}) ->
	DocVersion.

update_term([], _NewTerm) ->
	[];

update_term([T | Index], NewTerm) ->
	NewWord = NewTerm#term.word,
	case (T#term.word) of
		NewWord -> [NewTerm | Index];
		_ -> [T | update_term(Index, NewTerm)]
	end.

insert_term([], NewTerm) ->
	[NewTerm];

insert_term([T | Index], NewTerm) ->
	case T#term.word < NewTerm#term.word of
		true ->
			NewIndex = insert_term(Index, NewTerm),
			[T | NewIndex];
		false ->
			[NewTerm | [T | Index]]
	end.

% ###### Term utils
word_to_bin(Word) ->
	WordList = Word ++ lists:duplicate(?SizeOfWord - length(Word), 0),
	list_to_binary(WordList).

bin_to_word(WordBin) ->
	lists:takewhile(fun(X) -> X /= 0 end, binary_to_list(WordBin)).

calculate_hash(Word, HashFunction) ->
	(HashFunction(Word) rem ?HashSize) + 1. %nth is base 1

hash_table_get(Word, HashTable, HashFunction) ->
	Hash = calculate_hash(Word, HashFunction),
	{lists:nth(Hash, HashTable), Hash}.

hash_table_insert(Pointer, Hash, HashTable) ->
	lists:sublist(HashTable, Hash - 1) ++ [Pointer] ++ lists:nthtail(Hash, HashTable).

read_term(Fp, DbName) ->
	case file:read(Fp, ?SizeOfWord + 2*?SizeOfCount + ?SizeOfPointer) of
		{ok, <<WordBin:?SizeOfWord/binary-unit:8, NormalPostings:?SizeOfCount/unit:8, ExtPostings:?SizeOfCount/unit:8, NextTerm:?SizeOfPointer/unit:8>>} ->
			Word = bin_to_word(WordBin),

			% lager:info("Reading ~p normal postings and ~p extended postings of word ~p", [NormalPostings, ExtPostings, WordBin]),
			{ok, <<PostingsBin/binary>>} =  file:read(Fp, (NormalPostings*?SizeOfPosting) + (ExtPostings*?SizeOfExtPosting)),
			{Postings, {NormalCounter, ExtCounter}} = bin_to_postings(PostingsBin, {NormalPostings, ExtPostings}, DbName),
			{ok, #term{word = Word, normalPostings = NormalCounter, extPostings = ExtCounter, nextTerm = NextTerm, postings = Postings}};
		R ->
			R
	end.

save_term(Term, Fp, HashTable, HashFunction, DbName) ->
	{ok, Pointer} = file:position(Fp, cur),
	{NewNextTerm, Hash} = hash_table_get(Term#term.word, HashTable, HashFunction),
	NewHashTable = hash_table_insert(Pointer, Hash, HashTable),
	% lager:info("Saving term ~p", [Term]),
	NewTermBin = term_to_bin(Term#term{nextTerm = NewNextTerm}, DbName),
	file:write(Fp, NewTermBin),
	NewHashTable.

save_term(MemTerm, DocTerm, Fp, HashTable, HashFunction, DbName) ->
	{NewPostings, {NormalCounter, ExtCounter}} = merge_postings(DocTerm#term.postings, MemTerm#term.postings, {DocTerm#term.normalPostings, DocTerm#term.extPostings}, {MemTerm#term.normalPostings, MemTerm#term.extPostings}),
	save_term(MemTerm#term{ normalPostings = NormalCounter, extPostings = ExtCounter, postings = NewPostings}, Fp, HashTable, HashFunction, DbName).

term_to_bin(#term{word = Word, normalPostings = NormalPostings, extPostings = ExtPostings, nextTerm = NextTerm, postings = Postings}, DbName) ->
	{PostingsBin, {NormalCounter, ExtCounter}} = postings_to_bin(Postings, {NormalPostings, ExtPostings}, DbName),
	WordBin = word_to_bin(Word),
	<<WordBin/binary, NormalCounter:?SizeOfCount/unit:8, ExtCounter:?SizeOfCount/unit:8, NextTerm:?SizeOfPointer/unit:8, PostingsBin/binary>>.


% #######

save_index(Index, DBName, HashFunction) ->
	IndexName = DBName++"Index.adbi",
	{ok, Fp} = file:open(IndexName, [read, write, binary]),
	HashTableBin = <<0:(?HashSize)/unit:64>>,
	Header = <<HashTableBin/binary>>,
	file:write(Fp, Header),
	HashTable = lists:duplicate(?HashSize, 0),
	NewHashTable = write_index(Index, HashTable, Fp, HashFunction, DBName),
	% lager:info("written"),
	NewHashTableBin = hashtable_to_bin(NewHashTable),
	% lager:info("hashtableBin ~p", [NewHashTableBin]),
	NewHeader = <<NewHashTableBin/binary>>,
	file:position(Fp, bof),
	file:write(Fp, NewHeader),
	file:close(Fp),
	clear_versions(DBName),
	clear_deletions(DBName).

write_index([], HashTable, _Fp, _HashFunction, _) ->
	HashTable;

write_index([Term | Index], HashTable, Fp, HashFunction, DbName) ->
	NewHashTable = save_term(Term, Fp, HashTable, HashFunction, DbName),
	write_index(Index, NewHashTable, Fp, HashFunction, DbName).

update_index(Index, DBName, HashFunction) ->
	IndexName = DBName++"Index.adbi",
	FpOld = case file:open(IndexName, [read, write, binary, {read_ahead, 1000000}]) of
		{ok, FpO} ->
			FpO;
		FpOldError ->
			lager:error("Unexpected error while opening old file ~p", [FpOldError]),
			throw(FpOldError)
	end,
	FpNew = case file:open("."++IndexName, [write, binary, {delayed_write, 1000000, 5}]) of
		{ok, FpN} ->
			FpN;
		FpNewError ->
			lager:error("Unexpected error while opening new file  ~p", [FpNewError]),
			throw(FpNewError)
	end,	
	HashTableBin = <<0:(?HashSize)/unit:64>>,
	Header = <<HashTableBin/binary>>,
	file:write(FpNew, Header),
	file:position(FpOld, {bof, ?HashSize*?SizeOfPointer}),
	HashTable = lists:duplicate(?HashSize, 0),
	NewHashTable = merge_index(Index, HashTable, FpOld, FpNew, HashFunction, DBName),
	NewHashTableBin = hashtable_to_bin(NewHashTable),
	NewHeader = <<NewHashTableBin/binary>>,
	file:position(FpNew, bof),
	file:write(FpNew, NewHeader),
	ok = file:close(FpOld),
	ok = file:close(FpNew),
	ok = file:delete(IndexName),
	ok = file:rename("."++IndexName, IndexName),
	clear_versions(DBName),
	clear_deletions(DBName).

merge_index([], DocTerm, HashTable, FpOld, FpNew, HashFunction, DbName) ->
	NewHashTable = save_term(DocTerm, FpNew, HashTable, HashFunction, DbName),
	merge_index([], NewHashTable, FpOld, FpNew, HashFunction, DbName);

merge_index([MemTerm | Index], DocTerm, HashTable, FpOld, FpNew, HashFunction, DbName) ->
	case MemTerm#term.word < DocTerm#term.word of
		true ->
			NewHashTable = save_term(MemTerm, FpNew, HashTable, HashFunction, DbName),
			merge_index(Index, DocTerm, NewHashTable, FpOld, FpNew, HashFunction, DbName);
		false when MemTerm#term.word > DocTerm#term.word ->
			NewHashTable = save_term(DocTerm, FpNew, HashTable, HashFunction, DbName),
			merge_index([MemTerm | Index], NewHashTable, FpOld, FpNew, HashFunction, DbName);
		false ->
			NewHashTable = save_term(MemTerm, DocTerm, FpNew, HashTable, HashFunction, DbName),
			merge_index(Index, NewHashTable, FpOld, FpNew, HashFunction, DbName)
	end.

merge_index(MemIndex, HashTable, FpOld, FpNew, HashFunction, DbName) ->
	case read_term(FpOld, DbName) of
		{ok, DocTerm} ->
			% lager:info("Read term ~p", [DocTerm]),
			merge_index(MemIndex, DocTerm, HashTable, FpOld, FpNew, HashFunction, DbName);
		eof ->
			merge_index(MemIndex, HashTable, FpNew, HashFunction, DbName)
	end.

merge_index([], HashTable, _, _, _) ->
	HashTable;

merge_index([MemTerm | Index], HashTable, FpNew, HashFunction, DbName) ->
	NewHashTable = save_term(MemTerm, FpNew, HashTable, HashFunction, DbName),
	merge_index(Index, NewHashTable, FpNew, HashFunction, DbName).

hashtable_to_bin([]) ->
	<<>>;

hashtable_to_bin([P | HashTable]) ->
	<<P:1/unit:64, (hashtable_to_bin(HashTable))/binary>>.

postings_to_bin([], Counters, _) ->
    {<<>>, Counters};

postings_to_bin([Posting | Postings], Counters, DbName) ->
	DocKey = extract_key_from_record(Posting),
	DocVersion = extract_version_from_record(Posting),
	Result = {lookup_doc_version(DocKey, DbName), lookup_deleted_doc(DocKey, DbName)},
	if
		Result =:= {{DocKey, DocVersion}, not_found}; Result =:= {not_found, not_found} ->
			case Posting of
				#posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion} ->
					{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters, DbName),
					{<<?Normal:1/unit:8, DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, PostingsBin/binary>>, UpdatedCounters};
				#posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd} ->
					{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters, DbName),
					{<<?Extended:1/unit:8, DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, FieldStart:?SizeOfDocPos/unit:8, FieldEnd:?SizeOfDocPos/unit:8, PostingsBin/binary>>, UpdatedCounters}
			end;
		true ->
			{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters, DbName),
			{PostingsBin, update_counters(Posting, UpdatedCounters, decrease)}
	end.

bin_to_postings(<<>>, Counters, _) ->
	{[], Counters};

bin_to_postings(PostingsBin, Counters, DbName) ->
	<<Type:1/unit:8, PostingBin/binary>> = PostingsBin,
	case Type of
		?Normal ->
			<<DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, NewPostingsBin/binary>> = PostingBin,
			Posting = #posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion};
		?Extended ->
			<<DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, FieldStart:?SizeOfDocPos/unit:8, FieldEnd:?SizeOfDocPos/unit:8, NewPostingsBin/binary>> = PostingBin,
			Posting = #posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd}
	end,
	{Postings, UpdatedCounters} = bin_to_postings(NewPostingsBin, Counters, DbName),
	insert_if_valid(Posting, DocKey, DocVersion, UpdatedCounters, Postings, DbName).

insert_if_valid(Posting, DocKey, DocVersion, Counters, PostingsList, DbName) ->
	case {lookup_doc_version(DocKey, DbName), lookup_deleted_doc(DocKey, DbName)} of
		{{DocKey, DocVersion}, not_found} ->
			{[Posting | PostingsList], Counters};
		{not_found, not_found} ->
			{[Posting | PostingsList], Counters};
		_ ->
			{PostingsList, update_counters(Posting, Counters, decrease)}
	end.

update_counters(#posting{}, {NormalCount, ExtCount}) ->
	{NormalCount+1, ExtCount};
update_counters(#posting_ext{}, {NormalCount, ExtCount}) ->
	{NormalCount, ExtCount+1}.

update_counters(#posting{}, {NormalCount, ExtCount}, decrease) ->
	{NormalCount-1, ExtCount};
update_counters(#posting_ext{}, {NormalCount, ExtCount}, decrease) ->
	{NormalCount, ExtCount-1};
update_counters({NormalCounter, ExtCounter}, {NormalDecrease, ExtDecrease}, decrease) ->
	{NormalCounter - NormalDecrease, ExtCounter - ExtDecrease}.

merge_postings([], [], {0, 0}, {0, 0}) ->
	{[], {0, 0}};

merge_postings([], MemPostings, {0, 0}, MemCounters) ->
	{MemPostings, MemCounters};

merge_postings(DocPostings, [], DocCounters, {0, 0}) ->
	{DocPostings, DocCounters};

merge_postings([P1 | DocPostings], [P2 | MemPostings], DocCounters, MemCounters) ->
	MemPostingKey = extract_key_from_record(P2),
	DocPostingKey = extract_key_from_record(P1),
	case DocPostingKey < MemPostingKey of
		true ->
			DecreasedDocCounters = update_counters(P1, DocCounters, decrease),
			{NewPostings, Counters} = merge_postings(DocPostings, [P2 | MemPostings], DecreasedDocCounters, MemCounters),
			UpdatedCounters = update_counters(P1, Counters),
			{[P1 | NewPostings], UpdatedCounters};
		false when DocPostingKey > MemPostingKey ->
			DecreasedMemCounters = update_counters(P2, MemCounters, decrease),
			{NewPostings, Counters} = merge_postings([P1 | DocPostings], MemPostings, DocCounters, DecreasedMemCounters),
			UpdatedCounters = update_counters(P2, Counters),
			{[P2 | NewPostings], UpdatedCounters};
		false ->
			{SkippedList, SkippedCounters} = skip_list(DocPostings, DocPostingKey),
			DecreasedDocCounters = update_counters(DocCounters, SkippedCounters, decrease),
			DecreasedMemCounters = update_counters(P2, MemCounters, decrease),
			{NewPostings, Counters} = merge_postings(SkippedList, MemPostings, DecreasedDocCounters, DecreasedMemCounters),
			UpdatedCounters = update_counters(P2, Counters),
			{[P2 | NewPostings], UpdatedCounters}
	end.

skip_list([], _DocKey) ->
	{[], {0, 0}};

skip_list([P | Postings], DocKey) ->
	PostingDocKey = extract_key_from_record(P),
	case PostingDocKey of
		DocKey ->
			{SkippedList, SkippedCounters} = skip_list(Postings, DocKey),
			Counters = update_counters(P, SkippedCounters),
			{SkippedList, Counters};
		_ ->
			{[P | Postings], {0, 0}}
	end.

find(Word, Index, DBName, HashFunction) ->
	IndexName = DBName++"Index.adbi",
	{ok, Fp} = file:open(IndexName, [read, write, binary]),

	MemTerm =
		case find_term(Index, Word) of
			not_found ->
				#term{postings = [], word=Word, normalPostings=0, extPostings=0, nextTerm=0};
			Term ->
				Term
		end,
	DocTerm = find_doc_term(Fp, HashFunction, Word, DBName),
	{Postings, _} = merge_postings(DocTerm#term.postings, MemTerm#term.postings, {DocTerm#term.normalPostings, DocTerm#term.extPostings}, {MemTerm#term.normalPostings, MemTerm#term.extPostings}),
	map_postings(Postings, DBName).

find_doc_term(Fp, HashFunction, Word, DbName) ->
	Hash = (HashFunction(Word) rem ?HashSize) + 1,
	{ok, _} = file:position(Fp, bof),
	case file:read(Fp, ?HashSize*?SizeOfPointer) of
		{ok, <<HashTableBin/binary>>} ->
			HashTable = bin_to_hashtable(HashTableBin),
			TermPointer = lists:nth(Hash, HashTable),
			find_term(Fp, TermPointer, Word, DbName);
		eof ->
			#term{postings=[], normalPostings=0, extPostings=0}
	end.

find_term(_Fp, 0, _Word, _) ->
	#term{postings=[], normalPostings=0, extPostings=0};

find_term(Fp, TermPointer, Word, DbName) ->
	{ok, _} = file:position(Fp, {bof, TermPointer}),
	{ok, TermWordBin} = file:read(Fp, ?SizeOfWord),
	TermWord = lists:takewhile(fun(X) -> X /= 0 end, binary_to_list(TermWordBin)),
	{ok, <<NormalPostings:?SizeOfCount/unit:8, ExtPostings:?SizeOfCount/unit:8, NextTerm:?SizeOfPointer/unit:8>>} = file:read(Fp, 2*?SizeOfCount + ? SizeOfPointer),
	case Word of
		TermWord ->
			{ok, PostingsBin} = file:read(Fp, (NormalPostings*?SizeOfPosting) + (ExtPostings*?SizeOfExtPosting)),
			{Postings, {NormalCounter, ExtCounter}} = bin_to_postings(PostingsBin, {NormalPostings, ExtPostings}, DbName),
			#term{word = Word, normalPostings = NormalCounter, extPostings = ExtCounter, postings = Postings};
		_ ->
			find_term(Fp, NextTerm, Word, DbName)
	end.

map_postings([], _) ->
	[];

map_postings([#posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion} | Postings], DbName) ->
	case {lookup_doc_version(DocKey, DbName), lookup_deleted_doc(DocKey, DbName)} of
		{{DocKey, DocVersion}, not_found} ->
			[{{docKey, integer_to_list(DocKey, 16)}, {docPos, DocPos}, {docVersion, DocVersion}} | map_postings(Postings, DbName)];
		{not_found, not_found} ->
			[{{docKey, integer_to_list(DocKey, 16)}, {docPos, DocPos}, {docVersion, DocVersion}} | map_postings(Postings, DbName)];
		_ ->
			map_postings(Postings, DbName)
	end;

map_postings([#posting_ext{docKey = DocKey, docPos = DocPos, fieldStart = FieldStart, fieldEnd = FieldEnd, docVersion = DocVersion} | Postings], DbName) ->
	case {lookup_doc_version(DocKey, DbName), lookup_deleted_doc(DocKey, DbName)} of
		{{DocKey, DocVersion}, not_found} ->
			[{{docKey, integer_to_list(DocKey, 16)}, {docPos, DocPos}, {fieldStart, FieldStart}, {fieldEnd, FieldEnd}, {docVersion, DocVersion}} | map_postings(Postings, DbName)];
		{not_found, not_found} ->
			[{{docKey, integer_to_list(DocKey, 16)}, {docPos, DocPos}, {fieldStart, FieldStart}, {fieldEnd, FieldEnd}, {docVersion, DocVersion}} | map_postings(Postings, DbName)];
		_ ->
			map_postings(Postings, DbName)
	end.

bin_to_hashtable(<<>>) ->
	[];

bin_to_hashtable(HashTableBin) ->
	<<Pointer:?SizeOfPointer/unit:8, NewHashTableBin/binary>> = HashTableBin,
	[Pointer | bin_to_hashtable(NewHashTableBin)].

hash(Word) ->
	Bin = crypto:hash(md4, Word),
	<<_:8/unit:8, Hash:8/unit:8>> = Bin,
	Hash.

% Version Control

start_table(DbName) ->
	TableName = format_versions_table_name(DbName),
	ets:new(format_versions_table_name(DbName), [set, protected, named_table]),
	{ok, Fp} = file:open(DbName++"Versions.adb", [read, write, binary]),
	Result = initialize_table(Fp, TableName),
	file:close(Fp),
	Result.

initialize_table(Fp, TableName) ->
	{ok, Position} = file:position(Fp, cur),
	case file:read(Fp, ?SizeOfDocKey+?SizeOfVersion) of
		{ok, <<DocKey:?SizeOfDocKey/unit:8, DocVersion:?SizeOfVersion/unit:8>>} ->
			ets:insert(TableName, {DocKey, DocVersion, Position}),
			initialize_table(Fp, TableName);
		eof ->
			ok
	end.

append_doc_version(DocKey, DocVersion, Fp) ->
	{ok, Pointer} = file:position(Fp, eof),
	file:write(Fp, <<DocKey:?SizeOfDocKey/unit:8, DocVersion:?SizeOfVersion/unit:8>>),
	Pointer.

insert_doc_version(DocKey, DocVersion, Pointer, Fp) ->
	{ok, _} = file:position(Fp, {bof, Pointer}),
	file:write(Fp, <<DocKey:?SizeOfDocKey/unit:8, DocVersion:?SizeOfVersion/unit:8>>).

add_doc_version(DocKey, DocVersion, DbName) ->
	TableName = format_versions_table_name(DbName),
	{ok, Fp} = file:open(DbName++"Versions.adb", [read, write, binary]),
	case ets:lookup(TableName, DocKey) of
		[] ->
			Pointer = append_doc_version(DocKey, DocVersion, Fp);
		[{DocKey, _, Pointer}] ->
			insert_doc_version(DocKey, DocVersion, Pointer, Fp)
	end,
	file:close(Fp),
	ets:insert(TableName, {DocKey, DocVersion, Pointer}).

lookup_doc_version(DocKey, DbName) ->
	case ets:lookup(format_versions_table_name(DbName), DocKey) of
		[] ->
			not_found;
		[{DocKey, Version, _}] ->
			{DocKey, Version}
	end.

recover_keys(DbName) ->
	TableName = format_versions_table_name(DbName),
	FirstKey = ets:first(TableName),
	recover_keys(FirstKey, [FirstKey], TableName).

recover_keys('$end_of_table', ['$end_of_table' | Acc], _) ->
	Acc;
recover_keys(Current, Acc, TableName) ->
	NextKey = ets:next(TableName, Current),
	recover_keys(NextKey, [NextKey | Acc], TableName).

clear_versions(DbName) ->
	ets:delete_all_objects(format_versions_table_name(DbName)),
	file:delete(DbName++"Versions.adb").

format_versions_table_name(DbName) ->
	list_to_atom(DbName ++ "_doc_versions").

%Deletion

start_deletion_table(DbName) ->
	TableName = format_deletions_table_name(DbName),
	ets:new(TableName, [set, protected, named_table]),
	{ok, Fp} = file:open(DbName++"Deletions.adb", [read, write, binary]),
	Result = initialize_deletion_table(Fp, TableName),
	file:close(Fp),
	Result.

initialize_deletion_table(Fp, TableName) ->
	{ok, Position} = file:position(Fp, cur),
	case file:read(Fp, ?SizeOfDocKey) of
		{ok, <<DocKey:?SizeOfDocKey/unit:8>>} ->
			ets:insert(TableName, {DocKey, Position}),
			initialize_deletion_table(Fp, TableName);
		eof ->
			ok
	end.

append_deleted_doc(DocKey, Fp) ->
	{ok, Pointer} = file:position(Fp, eof),
	file:write(Fp, <<DocKey:?SizeOfDocKey/unit:8>>),
	Pointer.

insert_deleted_doc(DocKey, Pointer, Fp) ->
	{ok, _} = file:position(Fp, {bof, Pointer}),
	file:write(Fp, <<DocKey:?SizeOfDocKey/unit:8>>).

add_deleted_doc(DocKey, DbName) ->
	TableName = format_deletions_table_name(DbName),
	{ok, Fp} = file:open(DbName++"Deletions.adb", [read, write, binary]),
	case ets:lookup(TableName, DocKey) of
		[] ->
			Pointer = append_deleted_doc(DocKey, Fp);
		[{DocKey, Pointer}] ->
			insert_deleted_doc(DocKey, Pointer, Fp)
	end,
	file:close(Fp),
	ets:insert(TableName, {DocKey, Pointer}).

lookup_deleted_doc(DocKey, DbName) ->
	case ets:lookup(format_deletions_table_name(DbName), DocKey) of
		[] ->
			not_found;
		[{DocKey, _}] ->
			{DocKey}
	end.

clear_deletions(DbName) ->
	ets:delete_all_objects(format_deletions_table_name(DbName)),
	file:delete(DbName++"Deletions.adb").

format_deletions_table_name(DbName) ->
	list_to_atom(DbName ++ "_doc_deletions").