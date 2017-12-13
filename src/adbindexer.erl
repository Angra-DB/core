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

read_term(Fp) ->
	case file:read(Fp, ?SizeOfWord + 2*?SizeOfCount + ?SizeOfPointer) of
		{ok, <<WordBin:?SizeOfWord/binary-unit:8, NormalPostings:?SizeOfCount/unit:8, ExtPostings:?SizeOfCount/unit:8, NextTerm:?SizeOfPointer/unit:8>>} ->
			Word = bin_to_word(WordBin),
			{ok, <<PostingsBin/binary>>} =  file:read(Fp, (NormalPostings*?SizeOfPosting) + (ExtPostings*?SizeOfExtPosting)),
			{Postings, {NormalCounter, ExtCounter}} = bin_to_postings(PostingsBin, {NormalPostings, ExtPostings}),
			{ok, #term{word = Word, normalPostings = NormalCounter, extPostings = ExtCounter, nextTerm = NextTerm, postings = Postings}};
		R ->
			R
	end.

save_term(Term, Fp, HashTable, HashFunction) ->
	{ok, Pointer} = file:position(Fp, cur),
	{NewNextTerm, Hash} = hash_table_get(Term#term.word, HashTable, HashFunction),
	NewHashTable = hash_table_insert(Pointer, Hash, HashTable),
	NewTermBin = term_to_bin(Term#term{nextTerm = NewNextTerm}),
	file:write(Fp, NewTermBin),
	NewHashTable.

save_term(MemTerm, DocTerm, Fp, HashTable, HashFunction) ->
	{NewPostings, {NormalCounter, ExtCounter}} = merge_postings(DocTerm#term.postings, MemTerm#term.postings, {DocTerm#term.normalPostings, DocTerm#term.extPostings}, {MemTerm#term.normalPostings, MemTerm#term.extPostings}),
	save_term(MemTerm#term{ normalPostings = NormalCounter, extPostings = ExtCounter, postings = NewPostings}, Fp, HashTable, HashFunction).

term_to_bin(#term{word = Word, normalPostings = NormalPostings, extPostings = ExtPostings, nextTerm = NextTerm, postings = Postings}) ->
	{PostingsBin, {NormalCounter, ExtCounter}} = postings_to_bin(Postings, {NormalPostings, ExtPostings}),
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
	NewHashTable = write_index(Index, HashTable, Fp, HashFunction),
	NewHashTableBin = hashtable_to_bin(NewHashTable),
	NewHeader = <<NewHashTableBin/binary>>,
	file:position(Fp, bof),
	file:write(Fp, NewHeader).

write_index([], HashTable, _Fp, _HashFunction) ->
	HashTable;

write_index([Term | Index], HashTable, Fp, HashFunction) ->
	NewHashTable = save_term(Term, Fp, HashTable, HashFunction),
	write_index(Index, NewHashTable, Fp, HashFunction).

update_index(Index, DBName, HashFunction) ->
	IndexName = DBName++"Index.adbi",
	{ok, FpOld} = file:open(IndexName, [read, write, binary]),
	{ok, FpNew} = file:open("."++IndexName, [read, write, binary]),
	HashTableBin = <<0:(?HashSize)/unit:64>>,
	Header = <<HashTableBin/binary>>,
	file:write(FpNew, Header),
	file:position(FpOld, {bof, ?HashSize*?SizeOfPointer}),
	HashTable = lists:duplicate(?HashSize, 0),
	NewHashTable = merge_index(Index, HashTable, FpOld, FpNew, HashFunction),
	NewHashTableBin = hashtable_to_bin(NewHashTable),
	NewHeader = <<NewHashTableBin/binary>>,
	file:position(FpNew, bof),
	file:write(FpNew, NewHeader),
	file:close(FpOld),
	file:close(FpNew),
	file:delete(IndexName),
	file:rename("."++IndexName, IndexName).

merge_index([], DocTerm, HashTable, FpOld, FpNew, HashFunction) ->
	NewHashTable = save_term(DocTerm, FpNew, HashTable, HashFunction),
	merge_index([], NewHashTable, FpOld, FpNew, HashFunction);

merge_index([MemTerm | Index], DocTerm, HashTable, FpOld, FpNew, HashFunction) ->
	case MemTerm#term.word < DocTerm#term.word of
		true ->
			NewHashTable = save_term(MemTerm, FpNew, HashTable, HashFunction),
			merge_index(Index, DocTerm, NewHashTable, FpOld, FpNew, HashFunction);
		false when MemTerm#term.word > DocTerm#term.word ->
			NewHashTable = save_term(DocTerm, FpNew, HashTable, HashFunction),
			merge_index([MemTerm | Index], NewHashTable, FpOld, FpNew, HashFunction);
		false ->
			NewHashTable = save_term(MemTerm, DocTerm, FpNew, HashTable, HashFunction),
			merge_index(Index, NewHashTable, FpOld, FpNew, HashFunction)
	end.

merge_index(MemIndex, HashTable, FpOld, FpNew, HashFunction) ->
	case read_term(FpOld) of
		{ok, DocTerm} ->
			merge_index(MemIndex, DocTerm, HashTable, FpOld, FpNew, HashFunction);
		eof ->
			merge_index(MemIndex, HashTable, FpNew, HashFunction)
	end.

merge_index([], HashTable, _, _) ->
	HashTable;

merge_index([MemTerm | Index], HashTable, FpNew, HashFunction) ->
	NewHashTable = save_term(MemTerm, FpNew, HashTable, HashFunction),
	merge_index(Index, NewHashTable, FpNew, HashFunction).

hashtable_to_bin([]) ->
	<<>>;

hashtable_to_bin([P | HashTable]) ->
	<<P:1/unit:64, (hashtable_to_bin(HashTable))/binary>>.

postings_to_bin([], Counters) ->
    {<<>>, Counters};

postings_to_bin([Posting | Postings], Counters) ->
	DocKey = extract_key_from_record(Posting),
	DocVersion = extract_version_from_record(Posting),
	Result = lookup_doc_version(DocKey),
	if
		Result =:= {DocKey, DocVersion}; Result =:= not_found ->
			case Posting of
				#posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion} ->
					{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters),
					{<<?Normal:1/unit:8, DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, PostingsBin/binary>>, UpdatedCounters};
				#posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd} ->
					{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters),
					{<<?Extended:1/unit:8, DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, FieldStart:?SizeOfDocPos/unit:8, FieldEnd:?SizeOfDocPos/unit:8, PostingsBin/binary>>, UpdatedCounters}
			end;
		true ->
			{PostingsBin, UpdatedCounters} = postings_to_bin(Postings, Counters),
			{PostingsBin, update_counters(Posting, UpdatedCounters, decrease)}
	end.

bin_to_postings(<<>>, Counters) ->
	{[], Counters};

bin_to_postings(PostingsBin, Counters) ->
	<<Type:1/unit:8, PostingBin/binary>> = PostingsBin,
	case Type of
		?Normal ->
			<<DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, NewPostingsBin/binary>> = PostingBin,
			Posting = #posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion};
		?Extended ->
			<<DocKey:?SizeOfDocKey/unit:8, DocPos:?SizeOfDocPos/unit:8, DocVersion:?SizeOfVersion/unit:8, FieldStart:?SizeOfDocPos/unit:8, FieldEnd:?SizeOfDocPos/unit:8, NewPostingsBin/binary>> = PostingBin,
			Posting = #posting_ext{docKey = DocKey, docPos = DocPos, docVersion = DocVersion, fieldStart = FieldStart, fieldEnd = FieldEnd}
	end,
	{Postings, UpdatedCounters} = bin_to_postings(NewPostingsBin, Counters),
	insert_if_current_version(Posting, DocKey, DocVersion, UpdatedCounters, Postings).

insert_if_current_version(Posting, DocKey, DocVersion, Counters, PostingsList) ->
	{DocKey, CurrentVersion} = lookup_doc_version(DocKey),
	case CurrentVersion of
		DocVersion ->
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
			io:format("DocKey ~w~nMemKey ~w~n", [DocPostingKey, MemPostingKey]),
			{SkippedList, SkippedCounters} = skip_list(DocPostings, DocPostingKey),
			DecreasedDocCounters = update_counters(DocCounters, SkippedCounters, decrease),
			DecreasedMemCounters = update_counters(P2, MemCounters, decrease),
			{NewPostings, {NormalPostings, ExtPostings}} = merge_postings(SkippedList, MemPostings, DecreasedDocCounters, DecreasedMemCounters),
			UpdatedCounters = update_counters(P2, NormalPostings, ExtPostings),
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
	DocTerm = find_doc_term(Fp, HashFunction, Word),
	{Postings, _} = merge_postings(DocTerm#term.postings, MemTerm#term.postings, {DocTerm#term.normalPostings, DocTerm#term.normalPostings}, {MemTerm#term.normalPostings, MemTerm#term.extPostings}),
	map_postings(Postings).

find_doc_term(Fp, HashFunction, Word) ->
	Hash = (HashFunction(Word) rem ?HashSize) + 1,
	{ok, _} = file:position(Fp, bof),
	case file:read(Fp, ?HashSize*?SizeOfPointer) of
		{ok, <<HashTableBin/binary>>} ->
			HashTable = bin_to_hashtable(HashTableBin),
			TermPointer = lists:nth(Hash, HashTable),
			find_term(Fp, TermPointer, Word);
		eof ->
			#term{postings=[], normalPostings=0, extPostings=0}
	end.

find_term(_Fp, 0, _Word) ->
	#term{postings=[], normalPostings=0, extPostings=0};

find_term(Fp, TermPointer, Word) ->
	{ok, _} = file:position(Fp, {bof, TermPointer}),
	{ok, TermWordBin} = file:read(Fp, ?SizeOfWord),
	TermWord = lists:takewhile(fun(X) -> X /= 0 end, binary_to_list(TermWordBin)),
	{ok, <<NormalPostings:?SizeOfCount/unit:8, ExtPostings:?SizeOfCount/unit:8, NextTerm:?SizeOfPointer/unit:8>>} = file:read(Fp, 2*?SizeOfCount + ? SizeOfPointer),
	case Word of
		TermWord ->
			{ok, PostingsBin} = file:read(Fp, (NormalPostings*?SizeOfPosting) + (ExtPostings*?SizeOfExtPosting)),
			{Postings, {NormalCounter, ExtCounter}} = bin_to_postings(PostingsBin, {NormalPostings, ExtPostings}),
			#term{word = Word, normalPostings = NormalCounter, extPostings = ExtCounter, postings = Postings};
		_ ->
			find_term(Fp, NextTerm, Word)
	end.

map_postings([]) ->
	[];

map_postings([#posting{docKey = DocKey, docPos = DocPos, docVersion = DocVersion} | Postings]) ->
	case lookup_doc_version(DocKey) of
		{DocKey, DocVersion} ->
			[{{docKey, DocKey}, {docPos, DocPos}, {docVersion, DocVersion}} | map_postings(Postings)];
		_ ->
			map_postings(Postings)
	end;

map_postings([#posting_ext{docKey = DocKey, docPos = DocPos, fieldStart = FieldStart, fieldEnd = FieldEnd, docVersion = DocVersion} | Postings]) ->
	case lookup_doc_version(DocKey) of
		{DocKey, DocVersion} ->
			[{{docKey, DocKey}, {docPos, DocPos}, {fieldStart, FieldStart}, {fieldEnd, FieldEnd}, {docVersion, DocVersion}} | map_postings(Postings)];
		_ ->
			map_postings(Postings)
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
	ets:new(doc_versions, [set, protected, named_table]),
	{ok, Fp} = file:open(DbName++"Versions.adbi", [read, write, binary]),
	initialize_table(Fp).

initialize_table(Fp) ->
	{ok, Position} = file:position(Fp, cur),
	case file:read(Fp, ?SizeOfDocKey+?SizeOfVersion) of
		{ok, <<DocKey:?SizeOfDocKey/unit:8, DocVersion:?SizeOfVersion/unit:8>>} ->
			ets:insert(doc_versions, {DocKey, DocVersion, Position}),
			initialize_table(Fp);
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
	{ok, Fp} = file:open(DbName++"Versions.adb", [read, write, binary]),
	case ets:lookup(doc_versions, DocKey) of
		[] ->
			Pointer = append_doc_version(DocKey, DocVersion, Fp);
		[{DocKey, _, Pointer}] ->
			insert_doc_version(DocKey, DocVersion, Pointer, Fp)
	end,
	file:close(Fp),
	ets:insert(doc_versions, {DocKey, DocVersion, Pointer}).

lookup_doc_version(DocKey) ->
	case ets:lookup(doc_versions, DocKey) of
		[] ->
			not_found;
		[{DocKey, Version, _}] ->
			{DocKey, Version}
	end.
