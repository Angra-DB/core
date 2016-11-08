-module(adbtree).
-include("adbtree.hrl").
%-export([start/1, save/3,lookup/2,update/3,delete/2, create_db/4, create_db/3, create_db/2, create_db/1]).
-compile(export_all).

start(DBName) ->
	case Fp = file:open(DBName++"Index.adb", [read]) of
		T = {error, enoent} ->
			T;
		_ ->
			file:close(Fp),
			Reader = spawn(adbtree, reader, [DBName]),
			Modifier = spawn(adbtree, modifier, [DBName]),
			Pid = spawn(adbtree, btree_device, [Modifier, Reader]),
			{ok, Pid}
	end.

btree_device(Modifier, Reader) ->
	link(Modifier),
	link(Reader),
	receive
		{Sender, {lookup, Key}} ->
			Reader ! {Sender, {lookup, Key}},
			btree_device(Modifier, Reader);
		{Sender, T} ->
			Modifier ! {Sender, T},
			btree_device(Modifier, Reader)
	end.

reader(DBName) ->
	receive
		{Sender, {lookup, Key}} ->
			Sender ! btree_lookup(Key, DBName),
			reader(DBName)
	end.

modifier(DBName) ->
	receive
		{Sender, {update, Doc, Key}} ->
			Sender ! btree_update(Doc, Key, DBName),
			modifier(DBName);
		{Sender, {insert, Doc, Key}} ->
			Sender ! insert(Doc, Key, DBName),
			modifier(DBName);
		{Sender, {delete, Key}} ->
			Sender ! btree_delete(Key, DBName),
			modifier(DBName);
		{Sender, _} ->
			Sender ! invalid_operation;
		T -> 
			io:print("~p", [T])
	end.

save(AdbtreeDevice, Doc, Key) ->
	AdbtreeDevice ! {self(), {insert, Doc, Key}},
	receive
		Answ ->
			Answ
	end.


lookup(AdbtreeDevice, Key) ->
	AdbtreeDevice ! {self(), {lookup, Key}},
	receive
		Answ ->
			Answ
	end.

update(AdbtreeDevice, Doc, Key) ->
	AdbtreeDevice ! {self(), {update, Doc, Key}},
	receive
		Answ ->
			Answ
	end.

delete(AdbtreeDevice, Key) ->
	AdbtreeDevice ! {self(), {delete, Key}},
	receive
		Answ ->
			Answ
	end.

close(AdbtreeDevice) ->
	AdbtreeDevice ! exit(normal).


%	Função que lê um documento do arquivo de documentos. Recebe como parâmetro: *parâmetros* . Retorna: *retorno*.
%	A function that reads a document from the document's file. Receives as attribute: *attribute*. Returns: *return value*.
read_doc(PosInBytes, Settings) -> 
	{ok, Fp} = file:open((Settings#dbsettings.dbname ++ "Docs.adb"), [read, binary]),
	{ok, _NP} = file:position(Fp, {bof, PosInBytes}),
	SizeSize = Settings#dbsettings.sizeinbytes,
	SizeOfVersion = Settings#dbsettings.sizeversion,
	{ok, <<DocSize:SizeSize/unit:8>>} = file:read(Fp, Settings#dbsettings.sizeinbytes),
	{ok, <<Version:SizeOfVersion/unit:8>>} = file:read(Fp, Settings#dbsettings.sizeversion),
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
	try
		NameIndex = Name++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [exclusive, binary, write]),
		Settings = #dbsettings{dbname=Name, sizeinbytes=SizeOfSizeOfDoc, sizeversion=SizeOfVersion},
		Btree = #btree{order=BtreeOrder},
		write_header(Fp, Settings, Btree, ?SizeOfHeader),
		Keys = lists:duplicate(BtreeOrder-1, -1),
		Versions = lists:duplicate(BtreeOrder-1, -1),
		Pointers = lists:duplicate(BtreeOrder-1, -1), %A pointer for each key plus the pointer for the next leaf.
		write_leaf(Fp, Settings, Btree, #leaf{keys=Keys, versions=Versions, docPointers=Pointers, leafPointer=-1}),
		file:close(Fp),
		{ok, Settings}
	catch
		error:Error -> 
			{error, Error}
	end.

create_db(Name) ->
	create_db(Name, ?DefaultMaxSize, ?DefaultVerSize, ?DefaultOrder).

create_db(Name, MaxSize) ->
	create_db(Name, MaxSize, ?DefaultVerSize, ?DefaultOrder).

create_db(Name, MaxSize, BtreeOrder) ->
	create_db(Name, MaxSize, ?DefaultVerSize, BtreeOrder).

%insert new key-value pair
insert(Doc, Key, DBName) ->
	try
		NameIndex = DBName++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [read, write, binary]),
		{Settings, Btree} = get_header(Fp),
		{ok, PosDoc, Version} = save_doc(-1, 0, Doc, Settings),
		{ok, NewRoot} = insert(Fp, Btree, Settings, Key, PosDoc, Version),
		write_header(Fp, Settings, Btree, NewRoot),
		file:close(Fp)
	catch
		error:Error ->
			{error, Error};
		exit:Error ->
			{exit, Error}
	end.
insert(Fp, Btree, Settings, Key, PosDoc, Version) ->
	try btree_insert(Fp, Btree, Settings, Key, PosDoc, Version) of
		{ok, ChildP} ->
			{ok, ChildP};
		{ok, LChildP, RChildP, NewValue} ->
			write_node(Fp, Btree, #node{keys=[NewValue], nodePointers=[LChildP, RChildP]})
	catch
		error:Error ->
			error(Error);
		exit:Error ->
			exit(Error)
	end.		

btree_insert(Fp, Btree = #btree{curNode = PNode}, Settings, Key, PosDoc, Version) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> -> 
			Leaf = read_leaf(Fp, Settings, Btree),
			try leaf_insert(Leaf, Btree#btree.order, Key, PosDoc, Version) of
				{ok, NewLeaf} -> 
					write_leaf(Fp, Settings, Btree, NewLeaf);
				{ok, NewLeafL, NewLeafR, NewValue} ->
					{ok, NewPosL} = write_leaf(Fp, Settings, Btree, NewLeafL),
					{ok, NewPosR} = write_leaf(Fp, Settings, Btree, NewLeafR),
					{ok, NewPosL, NewPosR, NewValue}
			catch
				error:Error -> 
					error(Error)
			end;
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNode = find_next_node(Node, Key),
			try btree_insert(Fp, Btree#btree{curNode = NextNode}, Settings, Key, PosDoc, Version) of
				{ok, ChildP} -> 
					NewNode = update_reference(Node, Key, ChildP),
					write_node(Fp, Btree, NewNode);
				{ok, LChildP, RChildP, NewValue} ->
					case node_insert(Node, Btree#btree.order, LChildP, RChildP, NewValue) of
						{ok, NewNode} ->
							write_node(Fp, Btree, NewNode); 
						{ok, NewNodeL, NewNodeR, PromotedValue} ->
							{ok, NewPosL} = write_node(Fp, Btree, NewNodeL),
							{ok, NewPosR} = write_node(Fp, Btree, NewNodeR),
							{ok, NewPosL, NewPosR, PromotedValue}
					end
			catch
				error:Error -> 
					error(Error)
			end;
		_V -> 
			error(invalidNodeId)
	end.


%delete the document identified with key
btree_delete(Key, DBName) ->
	try
		NameIndex = DBName++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [read, write, binary]),
		{Settings, Btree} = get_header(Fp),
		{ok, NewRoot} = delete(Fp, Btree, Settings, Key),
		write_header(Fp, Settings, Btree, NewRoot),
		file:close(Fp)
	catch
		error:Error ->
			{error, Error}
	end.

delete(Fp, Btree = #btree{order=BtreeOrder, curNode=PNode}, Settings, Key) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> ->
			Leaf = read_leaf(Fp, Settings, Btree),
			{_, NewLeaf} = delete_from_leaf(Leaf, Key, BtreeOrder),
			write_leaf(Fp, Settings, Btree, NewLeaf);
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNodeP = find_next_node(Node, Key),
			case btree_delete(Fp, Btree#btree{curNode=NextNodeP}, Settings, Key) of
				{ok, ChildNode} ->
					if
						is_record(ChildNode, node) ->
							{ok, PChildNode} = write_node(Fp, Btree, ChildNode);
						true ->
							{ok, PChildNode} = write_leaf(Fp, Settings, Btree, ChildNode)
					end,
					NewNode = update_reference(Node, Key, PChildNode),
					write_node(Fp, Btree, NewNode);
				{lacking, ChildNode} ->
					case find_brother(Fp, Node, Key, Settings, Btree) of
						{Side, Brother, DivKey} when length(Brother#node.keys) == (BtreeOrder-1) div 2 ->
							NewChildNode = merge(ChildNode, Brother, DivKey, Side),
							{ok, PChildNode} = write_node(Fp, Btree, NewChildNode),
							case delete_key_and_update(Node, DivKey, PChildNode, Btree#btree.order) of
								{_, NewNode} when length(NewNode#node.keys) == 0 ->
									{ok, PChildNode};
								{_, NewNode} ->
									write_node(Fp, Btree, NewNode)
							end;
						{Side, Brother, DivKey} when length(Brother#leaf.keys) == (BtreeOrder-1) div 2 ->
							NewChildNode = merge(ChildNode, Brother, DivKey, Side),
							{ok, PChildNode} = write_leaf(Fp, Settings, Btree, NewChildNode),
							case delete_key_and_update(Node, DivKey, PChildNode, Btree#btree.order) of
								{_, NewNode} when length(NewNode#node.keys) == 0 ->
									{ok, PChildNode};
								{_, NewNode} ->
									write_node(Fp, Btree, NewNode)
							end;
						{Side, Brother, DivKey} ->
							{NewChildNode, NewBrother, NewDivKey} = borrow(ChildNode, Brother, DivKey, Side),
							if
								is_record(NewChildNode, node) ->
									{ok, PChildNode} = write_node(Fp, Btree, NewChildNode),
									{ok, PBrother} = write_node(Fp, Btree, NewBrother);
								true ->
									{ok, PChildNode} = write_leaf(Fp, Settings, Btree, NewChildNode),
									{ok, PBrother} = write_leaf(Fp, Settings, Btree, NewBrother)
							end,
							{ok, NewNode} = update_key_and_references(Node, PChildNode, PBrother, NewDivKey, DivKey, Side),
							write_node(Fp, Btree, NewNode)
					end
			end				
	end.


btree_delete(Fp, Btree = #btree{curNode = PNode, order=BtreeOrder}, Settings, Key) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> ->
			Leaf = read_leaf(Fp, Settings, Btree),
			delete_from_leaf(Leaf, Key, BtreeOrder);
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNodeP = find_next_node(Node, Key),
			case btree_delete(Fp, Btree#btree{curNode=NextNodeP}, Settings, Key) of
				{ok, ChildNode} ->
					if
						is_record(ChildNode, node) ->
							{ok, PChildNode} = write_node(Fp, Btree, ChildNode);
						true ->
							{ok, PChildNode} = write_leaf(Fp, Settings, Btree, ChildNode)
					end,
					NewNode = update_reference(Node, Key, PChildNode),
					{ok, NewNode};
				{lacking, ChildNode} -> 
					case find_brother(Fp, Node, Key, Settings, Btree) of
						{Side, Brother, DivKey} when length(Brother#node.keys) == (BtreeOrder-1) div 2 ->
							NewChildNode = merge(ChildNode, Brother, DivKey, Side),
							{ok, PChildNode} = write_node(Fp, Btree, NewChildNode),
							delete_key_and_update(Node, DivKey, PChildNode, Btree#btree.order);
						
						{Side, Brother, DivKey} when length(Brother#leaf.keys) == (BtreeOrder-1) div 2 ->
							NewChildNode = merge(ChildNode, Brother, DivKey, Side),
							{ok, PChildNode} = write_leaf(Fp, Settings, Btree, NewChildNode),
							delete_key_and_update(Node, DivKey, PChildNode, Btree#btree.order);				
						{Side, Brother, DivKey} ->
							{NewChildNode, NewBrother, NewDivKey} = borrow(ChildNode, Brother, DivKey, Side),
							if
								is_record(NewChildNode, node) ->
									{ok, PChildNode} = write_node(Fp, Btree, NewChildNode),
									{ok, PBrother} = write_node(Fp, Btree, NewBrother);
								true ->
									{ok, PChildNode} = write_leaf(Fp, Settings, Btree, NewChildNode),
									{ok, PBrother} = write_leaf(Fp, Settings, Btree, NewBrother)
							end,
							update_key_and_references(Node, PChildNode, PBrother, NewDivKey, DivKey, Side)
					end 

			end
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	Area where the lookup and update functions will take place. Now functional.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	

btree_lookup(Key, DBName) ->
	try
		NameIndex = DBName++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [read, binary]),
		{Settings, Btree} = get_header(Fp),
		TargetDoc = btree_lookup(Fp, Settings, Btree, Key),
		Doc = read_doc(TargetDoc, Settings),
		file:close(Fp),
		Doc
	catch
		error:Error ->
			{error, Error}
	end.

btree_lookup(Fp, Settings, Btree = #btree{curNode = PNode}, Key) -> 
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNode = find_next_node(Node, Key),
			try
				Doc = btree_lookup(Fp, Settings, Btree#btree{curNode = NextNode}, Key),
				Doc
			catch
				error:Error ->
					error(Error)
			end;
		<<?Leaf:1/unit:8>> ->
			Leaf = read_leaf(Fp, Settings, Btree),
			TDoc = doc_pointer(Key, Leaf#leaf.keys, Leaf#leaf.docPointers),
			TDoc;
		_V ->
			error(invalidNodeId)
	end.

doc_pointer(Key, [LKey|LKeys], [PDoc|LDocPointers]) ->
	if 
		Key == LKey ->
			PDoc;
		true ->
			doc_pointer(Key, LKeys, LDocPointers)
	end;
doc_pointer(_, [], []) ->
	error(notInDB).




btree_update(Doc, Key, DBName) -> 
	try
		NameIndex = DBName++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [read, write, binary]),
		{Settings, Btree} = get_header(Fp),
		btree_update(Fp, Settings, Btree, Key, Doc, root),
		file:close(Fp),
		ok
	catch
		error:Error ->
			{error, Error}
	end.

btree_update(Fp, Settings, Btree = #btree{curNode = PNode}, Key, Doc, IsRoot) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			NextNode = find_next_node(Node, Key),
			try
				SonPointer = btree_update(Fp, Settings, Btree#btree{curNode = NextNode}, Key, Doc, noRoot),
				NewNode = update_reference(Node, Key, SonPointer),
				{ok, NewPosNode} = write_node(Fp, Btree, NewNode),
				if
					IsRoot == root ->
						% Atualiza header
						write_header(Fp, Settings, Btree, NewPosNode),
						ok;
					true ->
						NewPosNode
				end
			catch
				error:Error ->
					error(Error)
			end;
		<<?Leaf:1/unit:8>> ->
			Leaf = read_leaf(Fp, Settings, Btree),
			{PosDoc, Version} = get_doc_pointer_version(Key, Leaf#leaf.keys , Leaf#leaf.docPointers , Leaf#leaf.versions),
			{ok, NewPos, NewVersion} = save_doc(PosDoc, Version, Doc, Settings),
			LPos = update_list(NewPos, PosDoc, Leaf#leaf.docPointers),
			LVersions = update_list(NewVersion, Version, Leaf#leaf.versions),
			NewLeaf = Leaf#leaf{docPointers = LPos, versions = LVersions},
			{ok, NewLeafPos} = write_leaf(Fp, Settings, Btree, NewLeaf),
			NewLeafPos;
		_V ->
			error(invalidNodeId)
	end.

get_doc_pointer_version(Key, [LKey|LKeys], [PosDoc|Docs], [Version|Versions]) ->
	if 
		Key == LKey ->
			{PosDoc, Version};
		true ->
			get_doc_pointer_version(Key, LKeys, Docs, Versions)
	end;
get_doc_pointer_version(_, [], [], []) ->
	notInDB.

update_list(New, Old, [Pos|LPos]) ->
	if
		Old == Pos ->
			[New|LPos];
		true ->
			[Pos|update_list(New, Old, LPos)]
	end;
update_list(_, _, []) ->
	error.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	End of the area for lookup and update functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	

%Operations with files, nodes and leaves and header

update_key_and_references([Key|Keys], [Pointer, Brother | Pointers], PChildNode, PBrother, DivKey, OldKey, Side) ->
	if 
		(OldKey == Key) and (Side == right) ->
			{[DivKey|Keys], [PChildNode, PBrother|Pointers]};
		(OldKey == Key) and (Side == left) and (Pointers == []) ->
			{[DivKey], [PBrother, PChildNode]};
		true ->
			{TempKeys, TempPointers} = update_key_and_references(Keys, [Brother|Pointers], PChildNode, PBrother, DivKey, OldKey, Side),
			{[Key|TempKeys], [Pointer|TempPointers]}
	end.

update_key_and_references(#node{keys=Keys, nodePointers=Pointers}, PChildNode, PBrother, DivKey, OldKey, Side) ->
	{NewKeys, NewPointers} = update_key_and_references(Keys, Pointers, PChildNode, PBrother, DivKey, OldKey, Side),
	{ok, #node{keys=NewKeys, nodePointers=NewPointers}}.

borrow(#node{keys=Keys, nodePointers=Pointers}, #node{keys=[NewDiv|BKeys], nodePointers=[BorrowedChild|BPointers]}, DivKey, right) ->
	{#node{keys=Keys++[DivKey], nodePointers=Pointers++[BorrowedChild]}, #node{keys=BKeys, nodePointers=BPointers}, NewDiv};

borrow(#node{keys=Keys, nodePointers=Pointers}, #node{keys=BKeys, nodePointers=BPointers}, DivKey, left) ->
	{#node{keys=[DivKey]++Keys, nodePointers=[lists:last(BPointers)]++Pointers}, #node{keys=lists:droplast(BKeys), nodePointers=lists:droplast(BPointers)}, lists:last(BKeys)};

borrow(#leaf{keys=Keys, docPointers=Pointers, versions=Versions}, #leaf{keys=[BorrowedKey, NewDiv|BKeys], docPointers=[BorrowedPointer|BPointers], versions=[BorrowedVersion|BVersions]}, _DivKey, right) -> 
	{#leaf{keys=Keys++[BorrowedKey], versions=Versions++[BorrowedVersion], docPointers=Pointers++[BorrowedPointer], leafPointer=-1},
	#leaf{keys=[NewDiv|BKeys], versions=BVersions, docPointers=BPointers, leafPointer=-1}, NewDiv};

borrow(#leaf{keys=Keys, docPointers=Pointers, versions=Versions}, #leaf{keys=BKeys, docPointers=BPointers, versions=BVersions}, _DivKey, left) -> 
	{#leaf{keys=[lists:last(BKeys)]++Keys, versions=[lists:last(BVersions)]++Versions, docPointers=[lists:last(BPointers)]++Pointers, leafPointer=-1},
	#leaf{keys=lists:droplast(BKeys), versions=lists:droplast(BVersions), docPointers=lists:droplast(BPointers), leafPointer=-1}, lists:last(BKeys)}.

find_brother([], [_Pointer], _Key) ->
	{left};

find_brother([CurKey|Keys], [Pointer, RBrother|Pointers], Key) ->
	if
		Key < CurKey ->
			{right, RBrother, CurKey};
		true ->
			case find_brother(Keys, [RBrother|Pointers], Key) of
				{left} ->
					{left, Pointer, CurKey};
				T ->
					T
			end
	end.

find_brother(Fp, #node{keys=Keys, nodePointers=Pointers}, Key, Settings, Btree) ->
	{Side, PBrother, DivKey} = find_brother(Keys, Pointers, Key),
	file:position(Fp, {bof, PBrother}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> ->
			Brother = read_leaf(Fp, Settings, Btree);
		<<?Node:1/unit:8>> ->
			Brother = read_node(Fp, Btree)
	end,
	{Side, Brother, DivKey}.

delete_key_and_update([Key|Keys], [Pointer, Brother|Pointers], DivKey, PChildNode) ->
	if
		DivKey == Key ->
			{Keys, [PChildNode|Pointers]};
		true ->
			{TempKeys, TempPointers} = delete_key_and_update(Keys, [Brother|Pointers], DivKey, PChildNode),
			{[Key|TempKeys], [Pointer|TempPointers]}
	end;

delete_key_and_update(#node{keys=Keys, nodePointers = Pointers}, DivKey, PChildNode, BtreeOrder) ->
	{NewKeys, NewPointers} = delete_key_and_update(Keys, Pointers, DivKey, PChildNode),
	if
		length(NewKeys) < (BtreeOrder-1) div 2 ->
			{lacking, #node{keys=NewKeys, nodePointers = NewPointers}};
		true ->
			{ok, #node{keys=NewKeys, nodePointers = NewPointers}}
	end.

delete_from_leaf([], _, _, _) ->
	error(keyUnused);
delete_from_leaf([CurKey|Keys], [Pointer|Pointers], [Version|Versions], Key) ->
	if
		CurKey == Key ->
			{Keys,Pointers,Versions};
		true ->
			{TempKeys, TempPointers, TempVersions} = delete_from_leaf(Keys, Pointers, Versions, Key),
			{[CurKey|TempKeys],[Pointer|TempPointers],[Version|TempVersions]}
	end.

delete_from_leaf(Leaf = #leaf{keys=Keys, docPointers=Pointers, versions=Versions}, Key, BtreeOrder) ->
	{NewKeys, NewPointers, NewVersions} = delete_from_leaf(Keys, Pointers, Versions, Key),
	if
		length(NewKeys) < ((BtreeOrder-1) div 2) ->
			{lacking, Leaf#leaf{keys=NewKeys, docPointers=NewPointers, versions=NewVersions}};
		true ->
			{ok, Leaf#leaf{keys=NewKeys, docPointers=NewPointers, versions=NewVersions}}
	end.

merge(#leaf{versions=Versions1, keys = Keys1, docPointers=Pointers1}, #leaf{versions=Versions2, keys = Keys2, docPointers=Pointers2}, _, right) ->
	#leaf{versions=Versions1++Versions2, keys=Keys1++Keys2, docPointers = Pointers1++Pointers2, leafPointer = -1};
merge(#leaf{versions=Versions1, keys = Keys1, docPointers=Pointers1}, #leaf{versions=Versions2, keys = Keys2, docPointers=Pointers2}, _, left) ->
	#leaf{versions=Versions2++Versions1, keys=Keys2++Keys1, docPointers = Pointers2++Pointers1, leafPointer = -1};


merge(#node{keys=Keys1, nodePointers=Pointers1}, #node{keys=Keys2, nodePointers=Pointers2}, DivKey, right) ->
	#node{keys=Keys1++[DivKey|Keys2], nodePointers=Pointers1++Pointers2};
merge(#node{keys=Keys1, nodePointers=Pointers1}, #node{keys=Keys2, nodePointers=Pointers2}, DivKey, left) ->
	#node{keys=Keys2++[DivKey|Keys1], nodePointers=Pointers2++Pointers1}.



pretty_printer(Name) ->
	try
		NameIndex = Name++"Index.adb",
		{ok, Fp} = file:open(NameIndex, [read, write, binary]),
		{Settings, Btree} = get_header(Fp),
		pretty_printer(Fp, Settings, Btree),
		file:close(Fp)
	catch
		error:Error ->
			{caught, Error, erlang:get_stacktrace()};
		exit:Error ->
			{caught, exit, Error, erlang:get_stacktrace()}
	end.

pretty_printer(Fp, Settings, Btree = #btree{curNode = PNode}) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> -> 
			Leaf = read_leaf(Fp, Settings, Btree),
			print_leaf(Leaf);
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			print_node(Node),
			lists:foreach(fun(X) -> pretty_printer(Fp, Settings, Btree#btree{curNode = X}) end, Node#node.nodePointers);
		_V -> 
			error(invalidNodeId)
	end.


print_leaf(#leaf{keys = Keys, docPointers = Pointers, versions = Versions}) ->
	io:format("L ", []),
	print_leaf(Keys, Pointers, Versions).

print_leaf([], [], []) -> 
	io:format("~n",[]);

print_leaf([Key|Keys],[Pointer|Pointers], [Version|Versions]) ->
	io:format("*~p|~p|v~p|", [Pointer, Key, Version]),
	print_leaf(Keys, Pointers, Versions).

print_node(#node{keys = Keys, nodePointers = Pointers}) ->
	io:format("N ", []),
	print_node(Keys, Pointers).

print_node([], [Pointer]) ->
	io:format("*~p~n", [Pointer]);

print_node([Key|Keys],[Pointer|Pointers]) ->
	io:format("*~p|~p|", [Pointer, Key]),
	print_node(Keys, Pointers).



update_reference(#node{keys = Keys, nodePointers = Pointers}, Key, ChildP) ->
	NewPointers = update_reference(Keys, Pointers, Key, ChildP),
	#node{keys = Keys, nodePointers = NewPointers}.

update_reference([], [_Pointer], _, NewPointer) ->
	[NewPointer];
update_reference([CurKey | Keys], [Pointer|Pointers], NewKey, NewPointer) ->
	if
		NewKey < CurKey ->
			[NewPointer | Pointers];
		true -> 
			[Pointer | update_reference(Keys, Pointers, NewKey, NewPointer)]  
	end.

% Discover the child node which a given key belong.
find_next_node(#node{keys = Keys, nodePointers = Pointers}, Key) ->
	find_next_node(Keys, Pointers, Key).

find_next_node([], [], _Key) ->
	erlang:error(notNode);
find_next_node([], [Pointer], _Key) -> 
	Pointer;
find_next_node([CurKey | Keys], [Pointer | Pointers], NewKey) ->
	if
		NewKey < CurKey ->
			Pointer;
		true ->
			find_next_node(Keys, Pointers, NewKey)
	end.
	
%header must be in begin of file
write_header(Fp, #dbsettings{dbname=Name, sizeversion=SizeOfVersion, sizeinbytes=SizeOfSizeOfDoc}, #btree{order=BtreeOrder}, RootPointer) ->
	file:position(Fp, bof),
	NameBin = list_to_binary(lists:duplicate((40 - length(Name)), 0)++Name),
	Header = <<NameBin/binary, SizeOfSizeOfDoc:?SizeOfSize/unit:8, SizeOfVersion:?SizeOfSize/unit:8, BtreeOrder:?OrderSize/unit:8, RootPointer:?SizeOfPointer/unit:8>>,
	file:write(Fp, Header).

%Given a file pointer, return the DBSettings and Btree
get_header(Fp) ->
	file:position(Fp, bof),
	case file:read(Fp, ?SizeOfHeader) of
		{ok, Header} ->
			<<Name:40/binary, SizeOfSize:?SizeOfSize/unit:8, SizeOfVersion:?SizeOfSize/unit:8, BtreeOrder:?OrderSize/unit:8, RootPointer:?SizeOfPointer/unit:8>> = Header,
			Settings = #dbsettings{dbname=lists:dropwhile(fun(X) -> X == 0 end, binary:bin_to_list(Name)), sizeinbytes=SizeOfSize, sizeversion=SizeOfVersion},
			Btree = #btree{order = BtreeOrder, curNode = RootPointer},
			{Settings, Btree};
		eof ->
			error(invalidDB)
	end.


leaf_to_bin(#leaf{keys=Keys, docPointers=DocPointers, leafPointer=LeafPointer, versions=Versions}, SizeOfVersion, BtreeOrder) ->
	KeysExt = complete_list(Keys, BtreeOrder-1),
	KeysBin = gen_bin(KeysExt, ?KeySize),
	DocPointersExt = complete_list(DocPointers, BtreeOrder-1),
	DocPointersBin = gen_bin(DocPointersExt, ?SizeOfPointer),
	VersionsExt = complete_list(Versions, BtreeOrder-1),
	VersionsBin = gen_bin(VersionsExt, SizeOfVersion),
	<<KeysBin/binary, VersionsBin/binary, DocPointersBin/binary, LeafPointer:?SizeOfPointer/unit:8>>.

bin_to_leaf(LeafBin, SizeOfVersion, BtreeOrder) ->
	{KeysExt, VersionsBin} = list_from_bin(LeafBin, BtreeOrder-1, ?KeySize),
	Keys = compact_list(KeysExt),
	{VersionsExt, PointersBin} = list_from_bin(VersionsBin, BtreeOrder-1, SizeOfVersion),
	Versions = compact_list(VersionsExt),
	{DocPointersExt, LeafPointerBin} = list_from_bin(PointersBin, BtreeOrder-1, ?SizeOfPointer),
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
	KeysExt = complete_list(Keys, BtreeOrder-1),
	KeysBin = gen_bin(KeysExt, ?KeySize),
	PointersExt = complete_list(Pointers, BtreeOrder),
	PointersBin = gen_bin(PointersExt, ?SizeOfPointer),
	<<KeysBin/binary, PointersBin/binary>>.

bin_to_node(NodeBin, BtreeOrder) ->
	{KeysExt, PointersBin} = list_from_bin(NodeBin, BtreeOrder-1, ?KeySize),
	Keys = compact_list(KeysExt),
	{PointersExt, <<>>} = list_from_bin(PointersBin, BtreeOrder, ?SizeOfPointer),
	Pointers = compact_list(PointersExt),
	#node{keys = Keys, nodePointers= Pointers}.

read_node(Fp, #btree{order = BtreeOrder}) ->
	{ok, NodeBin} = file:read(Fp, size_of_node(BtreeOrder)),
	bin_to_node(NodeBin, BtreeOrder).

write_node(Fp, #btree{order=BtreeOrder}, Node) ->
	{ok, NewPos} = file:position(Fp, eof),
	NodeBin = node_to_bin(Node, BtreeOrder),
	file:write(Fp, <<?Node:1/unit:8, NodeBin/binary>>),
	{ok, NewPos}.


% Ponteiro a esquerda da chave aponta para o no com elementos menores que a chave, enquato que o ponteiro a direita aponta para
% o no com elementos maiores ou iguais a ele

leaf_insert(Leaf, BtreeOrder, Key, PosDoc, Version) -> 
	try insert_ord(Leaf#leaf.keys, Key) of
		{ok, NewKeys, Index} ->	
			NewDocPointers = insert_list(Leaf#leaf.docPointers, PosDoc, Index),
			NewVersions = insert_list(Leaf#leaf.versions, Version, Index),
			NewLeaf = Leaf#leaf{keys=NewKeys, docPointers = NewDocPointers, versions = NewVersions, leafPointer = -1},
			if
				length(NewLeaf#leaf.keys) > (BtreeOrder-1) ->
					{KeysL, KeysR} = lists:split((BtreeOrder) div 2, NewLeaf#leaf.keys),
					{DocPointersL, DocPointersR} = lists:split((BtreeOrder) div 2, NewLeaf#leaf.docPointers),
					{VersionsL, VersionsR} = lists:split((BtreeOrder) div 2, NewLeaf#leaf.versions),
					NewLeafL = #leaf{versions = VersionsL, docPointers = DocPointersL, keys = KeysL, leafPointer = -1},
					NewLeafR = #leaf{versions = VersionsR, docPointers = DocPointersR, keys = KeysR, leafPointer = -1},
					[NewValue|_L] = KeysR,
					{ok, NewLeafL, NewLeafR, NewValue};
				true ->
					{ok, NewLeaf}
			end
	catch
		error:Error ->
			error(Error)
	end.

node_insert_key([], [_Pointer], LChildP, RChildP, NewValue) ->
	{[NewValue], [LChildP, RChildP]};
node_insert_key([K|Keys], [P|Pointers], LChildP, RChildP, NewValue) ->
	if
		NewValue < K -> 
			{[NewValue, K | Keys], [LChildP, RChildP | Pointers]};
		true ->
			{NewKeys, NewPointers} = node_insert_key(Keys, Pointers, LChildP, RChildP, NewValue),
			{[K | NewKeys], [P | NewPointers]}
	end.

node_insert(#node{keys = Keys, nodePointers = Pointers}, LChildP, RChildP, NewValue) ->
	{NewKeys, NewPointers} = node_insert_key(Keys, Pointers, LChildP, RChildP, NewValue),
	#node{keys = NewKeys, nodePointers = NewPointers}.


node_insert(Node, BtreeOrder, LChildP, RChildP, Value) ->
	NewNode = node_insert(Node, LChildP, RChildP, Value),
	if
		length(NewNode#node.keys) > (BtreeOrder-1) ->
			{KeysL, [NewValue | KeysR]} = lists:split((BtreeOrder) div 2, NewNode#node.keys),
			{PointersL, PointersR} = lists:split((BtreeOrder+2) div 2, NewNode#node.nodePointers),
			NewNodeL = #node{keys = KeysL, nodePointers = PointersL},
			NewNodeR = #node{keys = KeysR, nodePointers = PointersR},
			{ok, NewNodeL, NewNodeR, NewValue};
		true ->
			{ok, NewNode}
	end.


%Utility Functions

insert_ord(L, V) ->
	try
		insert_ord(L, V, 0)
	catch
		error:Error ->
			error(Error)
	end.

insert_ord([], V, I) ->
	{ok, [V], I};
insert_ord([X|L], V, I) ->
	if
		X == V ->
			 error(keyInUse);
		V < X ->
			{ok, [V, X|L], I};
		true -> 
			try insert_ord(L, V, I+1) of
				{ok, NewL, Index} ->
					{ok, [X|NewL], Index}
			catch
				error:Error ->
					error(Error)
			end
	end.
insert_list([], V, _) ->
	[V];
insert_list(L, V, 0) ->
	[V|L];
insert_list([X|L], V, Index) ->
	[X|insert_list(L, V, Index-1)].

gen_bin([X|[]], ElementSize) ->
	<<X:ElementSize/unit:8>>;

gen_bin([X|L], ElementSize) -> 
	Rest = gen_bin(L, ElementSize),
	<<X:ElementSize/unit:8, Rest/binary>>.

complete_list(L, Tam) ->
	L ++ lists:duplicate(Tam-length(L), -1).

list_from_bin(Bin, 1, Size) ->
	<<Element:Size/signed-unit:8, Rest/binary>> = Bin,
	{[Element], Rest};

list_from_bin(Bin, N, Size) ->
	<<Element:Size/signed-unit:8, Rest/binary>> = Bin,
	{L, Suffix} = list_from_bin(Rest, N-1, Size),
	{[Element | L], Suffix}.

size_of_leaf(SizeOfVersion, BtreeOrder) ->
	(BtreeOrder-1)*SizeOfVersion+(BtreeOrder-1)*?KeySize+(BtreeOrder-1)*?SizeOfPointer+?SizeOfPointer. %Node identifier

size_of_node(BtreeOrder) ->
	(BtreeOrder-1)*?KeySize + (BtreeOrder)*?SizeOfPointer.

compact_list(L) ->
	lists:reverse(lists:dropwhile(fun(X) -> X == -1 end, lists:reverse(L))).