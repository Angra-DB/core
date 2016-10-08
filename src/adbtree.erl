-module(adbtree).
-include("adbtree.hrl").
-compile(export_all).

%	Função que lê um documento do arquivo de documentos. Recebe como parâmetro: *parâmetros* . Retorna: *retorno*.
%	A function that reads a document from the document's file. Receives as attribute: *attribute*. Returns: *return value*.
read_doc(PosInBytes, Settings) -> 
	{ok, Fp} = file:open((Settings#dbsettings.dbname ++ "Docs.adb"), [read, binary]),
	{ok, _NP} = file:position(Fp, {bof, PosInBytes}),
	SizeSize = Settings#dbsettings.sizeinbytes,
	{ok, <<DocSize:SizeSize/unit:8>>} = file:read(Fp, Settings#dbsettings.sizeinbytes),
	{ok, Version} = file:read(Fp, Settings#dbsettings.sizeversion),
	{ok, Doc} = file:read(Fp, DocSize),
	file:close(Fp),
	{ok, Version, Doc}.


%	Função que atualiza um documento no arquivo de documentos. Recebe como parâmetro: *parâmetros* . Retorna: *retorno*.
%	A function that updates a document in the document's file. Receives as attribute: *attribute*. Returns: *return value*.
save_doc(PosLastDoc, LastVersion, Doc, Settings) ->
	{ok, Fp} = file:open((Settings#dbsettings.dbname ++ "Docs.adb"), [write, append]),
	{ok, Pos} = file:position(Fp, eof),
	RawDocSize = byte_size(Doc),
	Size = Settings#dbsettings.sizeinbytes,
	SizeBin = <<RawDocSize:Size/unit:8>>, % The Size is multiplied by 8 to obtain the value in bits.
	SVersion = Settings#dbsettings.sizeversion,
	Version = <<(LastVersion+1):SVersion/unit:8>>, % Version is a 2 bytes integer, refering to the times that the doc was changed. "1" means first version. The Size is multiplied by 8 to obtain the value in bits.
	PointerLastDoc = <<PosLastDoc:?SizeOfPointer/unit:8>>,
	file:write(Fp, <<SizeBin/binary, Version/binary, Doc/binary, PointerLastDoc/binary>>),
	file:close(Fp),
	{ok, Pos, (LastVersion+1)}.
	% On create, remember to pass "-1" as PosLastDoc and "0" as LastVersion.
	
%Add exception handling later...
%Name is the name of the Database, without any extensions.
%Header is formated as: NameOfDB/40bytes, SizeOfSizeOfDocs/8bytes,SyzeOfVersion/8bytes,BtreeOrder/2bytes, PointerForRoot/8Bytes.
create_db(Name, SizeOfSizeOfDoc, SizeOfVersion, BtreeOrder) -> 
	NameIndex = Name++"Index.adb",
	{ok, Fp} = file:open(NameIndex, [exclusive, binary, write]),
	Settings = #dbsettings{dbname=Name, sizeinbytes=SizeOfSizeOfDoc, sizeversion=SizeOfVersion},
	Btree = #btree{order=BtreeOrder},
	write_header(Fp, Settings, Btree, ?SizeOfHeader),
	Keys = lists:duplicate(BtreeOrder, 0),
	Versions = lists:duplicate(BtreeOrder, 0),
	Pointers = lists:duplicate(BtreeOrder, 0), %A pointer for each key plus the pointer for the next leaf.
	write_leaf(Fp, Settings, Btree, #leaf{keys=Keys, versions=Versions, docPointers=Pointers, leafPointer=0}),
	file:close(Fp),
	{ok, Settings}.

%insert new key-value pair
insert(Doc, Key, DBName) ->
	NameIndex = DBName++"Index.adb",
	{ok, Fp} = file:open(NameIndex, [read, write, binary]),
	{Settings, Btree} = get_header(Fp),
	{ok, PosDoc, Version} = save_doc(-1, 0, Doc, Settings),
	{ok, NewRoot} = insert(Fp, Btree, Settings, Key, PosDoc, Version),
	write_header(Fp, Settings, Btree, NewRoot),
	file:close(Fp),
	{ok}.
insert(Fp, Btree, Settings, Key, PosDoc, Version) ->
	case btree_insert(Fp, Btree, Settings, Key, PosDoc, Version) of
		{ok, ChildP} ->
			{ok, NewRoot};
		{ok, LChildP, RChildP, NewValue} ->
			{ok, NewRoot} = write_node(Fp, Settings, Btree, #node{keys=[NewValue], nodePointers=[LChildP, RChildP]})
	end.		

btree_insert(Fp, Btree = #btree{curNode = PNode}, Settings, Key, PosDoc, Version) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> -> 
			Leaf = read_leaf(Fp, Settings, Btree),
			case leaf_insert(Leaf, Btree#btree.order, Key, PosDoc, Version) of
				{ok, NewLeaf} -> 
					{ok, NewPos} = write_leaf(Fp, Settings, Btree, NewLeaf);
				{ok, NewLeafL, NewLeafR, NewValue} ->
					{ok, NewPosL} = write_leaf(Fp, Settings, Btree, NewLeafL),
					{ok, NewPosR} = write_leaf(Fp, Settings, Btree, NewLeafR),
					{ok, NewPosL, NewPosR, NewValue};
				Error -> 
					Error
			end;
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNode = find_next_node(Node, Key),
			case btree_insert(Fp, Btree#btree{curNode = NextNode}, Settings, Key, PosDoc, Version) of
				{ok, ChildP} -> 
					update_reference(Node, Key, ChildP, Btree#btree.order),
					{ok, NewPos} = write_node(Fp, Settings, Btree, ChildP);
				{ok, RChildP, LChildP, NewValue} ->
					case node_insert(Node, RChildP, LChildP, NewValue) of
						{ok, NewNode} ->
							{ok, NewPos} = write_node(Fp, Settings, Btree, NewNode); 
						{ok, NewNodeL, NewNodeR, NewValue} ->
							{ok, NewPosL} = write_node(Fp, Settings, Btree, NewNodeL),
							{ok, NewPosR} = write_node(Fp, Settings, Btree, NewNodeR),
							{ok, NewPosL, NewPosR, NewValue};
						Error ->
							Error
					end;
				Error -> 
					Error
			end
	end.

%Operations with files, nodes and leaves and header



% Discover the child node which a given key belong.
find_next_node(#node{keys = Keys, nodePointers = Pointers}, Key) ->
	find_next_node(Keys, Pointers, Key).

find_next_node([], [], _Key) ->
	{error, notNode};
find_next_node([],[Pointer], _Key)
	Pointer;
find_next_node([CurKey | Keys], [Pointer | Pointers], NewKey) ->
	if
		NewK < CurKey ->
			Pointer;
		true ->
			find_next_node(Keys, Pointers, Tam-1, NewKey)
	end
	
%header must be in end of file
write_header(Fp, #dbsettings{dbname=Name, sizeversion=SizeOfVersion, sizeinbytes=SizeOfSizeOfDoc}, #btree{order=BtreeOrder}, RootPointer) ->
	NameBin = list_to_binary(lists:duplicate((40 - length(Name)), 0)++Name),
	Header = <<NameBin/binary, SizeOfSizeOfDoc:?SizeOfSize/unit:8, SizeOfVersion:?SizeOfSize/unit:8, BtreeOrder:?OrderSize/unit:8, RootPointer:?SizeOfPointer/unit:8>>,
	file:write(Fp, Header).

%Given a file pointer, return the DBSettings and Btree
get_header(Fp) ->
	file:position(Fp, bof),
	{ok, Header} = file:read(Fp, ?SizeOfHeader),
	<<Name:40/binary, SizeOfSize:?SizeOfSize/unit:8, SizeOfVersion:?SizeOfSize/unit:8, BtreeOrder:?OrderSize/unit:8, RootPointer:?SizeOfPointer/unit:8>> = Header,
	Settings = #dbsettings{dbname=lists:dropwhile(fun(X) -> X == 0 end, binary:bin_to_list(Name)), sizeinbytes=SizeOfSize, sizeversion=SizeOfVersion},
	Btree = #btree{order = BtreeOrder, curNode = RootPointer},
	{Settings, Btree}.

leaf_to_bin(#leaf{keys=Keys, docPointers=DocPointers, leafPointer=LeafPointer, versions=Versions}, SizeOfVersion, BtreeOrder) ->
	KeysExt = complete_list(Keys, BtreeOrder),
	KeysBin = gen_bin(KeysExt, ?KeySize),
	DocPointersExt = complete_list(DocPointers, BtreeOrder),
	DocPointersBin = gen_bin(DocPointersExt, ?SizeOfPointer),
	VersionsExt = complete_list(Versions, BtreeOrder),
	VersionsBin = gen_bin(VersionsExt, SizeOfVersion),
	<<KeysBin/binary, VersionsBin/binary, DocPointersBin/binary, LeafPointer:?SizeOfPointer/unit:8>>.

bin_to_leaf(LeafBin, SizeOfVersion, BtreeOrder) ->
	{KeysExt, VersionsBin} = list_from_bin(LeafBin, BtreeOrder, ?KeySize),
	Keys = compact_list(KeysExt),
	{VersionsExt, PointersBin} = list_from_bin(VersionsBin, BtreeOrder, SizeOfVersion),
	Versions = compact_list(VersionsExt),
	{DocPointersExt, LeafPointerBin} = list_from_bin(PointersBin, BtreeOrder, ?SizeOfPointer),
	DocPointers = compact_list(DocPointersExt),
	<<LeafPointer:?SizeOfPointer/unit:8>> = LeafPointerBin,
	#leaf{keys = Keys, versions = Versions, docPointers = DocPointers, leafPointer = LeafPointer}.

read_leaf(Fp, #dbsettings{sizeversion = SizeOfVersion}, #btree{order=BtreeOrder}) ->
	{ok, LeafBin} = file:read(Fp, size_of_leaf(SizeOfVersion, BtreeOrder)),
	bin_to_leaf(LeafBin, SizeOfVersion, BtreeOrder).
	

write_leaf(Fp, #dbsettings{sizeversion=SizeOfVersion}, #btree{order=BtreeOrder}, Leaf) ->
	{ok, NewPos} = file:position(Fp, eof),
	LeafBin = leaf_to_bin(Leaf, SizeOfVersion, BtreeOrder),
	file:write(Fp, <<?Leaf:1/unit:8, LeafBin/binary>>),
	{ok, NewPos}.

node_to_bin(#node{keys=Keys, nodePointers=Pointers}, BtreeOrder) ->
	KeysExt = complete_list(Keys, BtreeOrder),
	KeysBin = gen_bin(KeysExt, ?KeySize),
	PointersExt = complete_list(Pointers, BtreeOrder+1),
	PointersBin = gen_bin(PointersExt, ?SizeOfPointer),
	<<KeysBin/binary, PointersBin/binary>>.

bin_to_node(NodeBin, BtreeOrder) ->
	{KeysExt, PointersBin} = list_from_bin(NodeBin, BtreeOrder, ?KeySize),
	Keys = compact_list(KeysExt),
	{DocPointersExt, <<>>} = list_from_bin(NodeBin, BtreeOrder+1, ?SizeOfPointer),
	DocPointers = compact_list(DocPointers),
	#node{keys = Keys, docPointers= DocPointers}.

read_node(Fp, #btree{BtreeOrder}) ->
	{ok, NodeBin} = file:read(Fp, size_of_node(BtreeOrder)),
	bin_to_node(NodeBin, BtreeOrder).

write_node(Fp, #btree{order=BtreeOrder}, Node) ->
	{ok, NewPos} = file:position(Fp, eof),
	NodeBin = node_to_bin(Node, BtreeOrder),
	file:write(Fp, <<?Node:1/unit:8, NodeBin/binary>>),
	{ok, newPos}.

% Ponteiro a esquerda da chave aponta para o no com elementos menores que a chave, enquato que o ponteiro a direita aponta para
% o no com

leaf_insert(Leaf, BtreeOrder, Key, PosDoc, Version) -> 
	case insert_ord(Leaf#leaf.keys, Key) of
		{ok, NewKeys, Index} ->	
			{ok, NewDocPointers} = insert_list(Leaf#leaf.docPointers, PosDoc, Index),
			{ok, NewVersions} = insert_list(Leaf#leaf.versions, Version, Index),
			NewLeaf = Leaf#leaf{keys=NewKeys, docPointers = NewDocPointers, versions = NewVersions}
			if
				length(NewLeaf#leaf.keys) > (BtreeOrder) ->
					{KeysL, KeysR} = lists:split(NewLeaf#leaf.keys, (BtreeOrder-1) div 2),
					{DocPointersL, DocPointersR} = lists:split(NewLeaf#leaf.docPointers, (BtreeOrder-1) div 2),
					{VersionsL, VersionsR} = lists:split(NewLeaf#leaf.docPointers, (BtreeOrder-1) div 2),
					NewLeafL = #leaf{versions = VersionsL, docPointers = DocPointersL, keys = KeysL, leafPointer = 0},
					NewLeafR = #leaf{versions = VersionsR, docPointers = DocPointersR, keys = KeysR, leafPointer = 0},
					[NewValue|_L] = KeysR,
					{ok, NewLeafL, NewLeafR, NewValue};
				true ->
					NewLeaf
			end;
		Error ->
			Error
	end


%Utility Functions


gen_bin([X|[]], ElementSize) ->
	<<X:ElementSize/unit:8>>;

gen_bin([X|L], ElementSize) -> 
	Rest = gen_bin(L, ElementSize),
	<<X:ElementSize/unit:8, Rest/binary>>.

complete_list(L, Tam) ->
	L ++ lists:duplicate(Tam-length(L), 0).

list_from_bin(Bin, 1, Size) ->
	<<Element:Size/unit:8, Rest/binary>> = Bin,
	{[Element], Rest};

list_from_bin(Bin, N, Size) ->
	<<Element:Size/unit:8, Rest/binary>> = Bin,
	{L, Suffix} = list_from_bin(Rest, N-1, Size),
	{[Element | L], Suffix}.

size_of_leaf(SizeOfVersion, BtreeOrder) ->
	BtreeOrder*SizeOfVersion+BtreeOrder*?KeySize+BtreeOrder*?SizeOfPointer+?SizeOfPointer. %Node identifier

size_of_node(BtreeOrder) ->
	BtreeOrder*?KeySize + (BtreeOrder+1)*?SizeOfPointer.

compact_list(L) ->
	reverse(dropwhile(fun(X) -> X == 0 end, reverse(L))).