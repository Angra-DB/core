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
	NameBin = list_to_binary(lists:duplicate((40 - length(Name)), 0)++Name),
	Header = <<NameBin/binary, SizeOfSizeOfDoc:8/unit:8, SizeOfVersion:8/unit:8, BtreeOrder:?OrderSize/unit:8, ?SizeOfHeader:?SizeOfPointer/unit:8>>,
	SizeOfKeys = BtreeOrder*?KeySize,
	SizeOfPointers = (BtreeOrder+1)*?SizeOfPointer,
	Root = <<?Leaf:1/unit:8, 0:SizeOfKeys/unit:8, 0:SizeOfPointers/unit:8, 0:?SizeOfPointer/unit:8>>,
	file:write(Fp, <<Header/binary, Root/binary>>),
	file:close(Fp),
	{ok, Settings}.






