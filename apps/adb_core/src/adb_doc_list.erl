-module(adb_doc_list).
-include("adbtree.hrl").

-compile(export_all).

-import(adbtree, [get_header/1, read_leaf/3, read_node/2]).
-import(adb_utils, [get_database_name/2, get_vnode_name/1]).

-define(Size, 8).

generate_list(DBName, List) ->
    file:rename(get_file_name(DBName), get_replaced_file_name(DBName)),
    file:write_file(get_file_name(DBName), List),
    file:delete(get_replaced_file_name(DBName)),
    ok.

generate_list(DBName) ->
    VNodeName = get_vnode_name(1),
    RealDBName = get_database_name(DBName, VNodeName),
    NameIndex = RealDBName++"Index.adb",
    {ok, Fp} = file:open(NameIndex, [read, binary]),
    {Settings, Btree} = get_header(Fp),
    LeafPosList = generate_list(Fp, Btree, Settings),
    BinLeafList = list_to_bin(lists:flatten(LeafPosList)),
    file:write_file(get_file_name(DBName), BinLeafList),
    file:close(Fp),
    ok.

generate_list(Fp, Btree = #btree{curNode=PNode}, Settings) ->
	file:position(Fp, {bof, PNode}),
	{ok, Type} = file:read(Fp, 1),
	case Type of
		<<?Leaf:1/unit:8>> ->
			[PNode];		
		<<?Node:1/unit:8>> ->
			Node = read_node(Fp, Btree),
			generate_list(Node#node.nodePointers, Fp, Btree, Settings);
		_V ->
            io:format("Erro!~n"), 
			error(invalidNodeId)
    end.
    
generate_list([], _Fp, _Btree, _Settings)->
    {ok, []};
generate_list([PNode | NodePointers], Fp, Btree, Settings) ->
    {ok, NewPointers} = generate_list(NodePointers, Fp, Btree, Settings),
    {ok, NewPos} = generate_list(Fp, Btree#btree{curNode=PNode}, Settings),
    {ok, [NewPos | NewPointers]}.

insert(DBName, OldPointer, NewPointers) ->
    List = get_list(DBName),
    NewList = list_insert(List, OldPointer, NewPointers),
    generate_list(DBName, NewList).

list_insert([], _, Pointers) ->
    Pointers;
list_insert([OldPointer | Tail], OldPointer, Pointers) ->
    lists:concat([Tail, Pointers]);
list_insert([Head | Tail], OldPointer, Pointers) ->
    [Head | list_insert(Tail, OldPointer, Pointers)].


update(DBName, OldPointer, NewPointer) ->
    List = get_list(DBName),
    PointerPos = get_pointer_pos(List, OldPointer),
    replace_pointer(DBName, PointerPos, NewPointer).

delete(DBName, OldPointer) ->
    List = get_list(DBName),
    UpdatedList = remove_pointer(List, OldPointer),
    generate_list(DBName, UpdatedList).

get_list(DBName) ->
    {ok, Data} = file:read_file(get_file_name(DBName)),
    list_from_bin(Data).

get_file_name(DBName) ->
    string:concat(DBName, "_doc_list.adb").

get_replaced_file_name(DBName) ->
    string:concat(DBName, "_doc_list_rep.adb").

open_file(DBName, Modes) ->
    file:open(string:concat(DBName, "_doc_list.adb"), Modes).

list_from_bin(Data) ->
    Datasize = byte_size(Data),
    list_from_bin(Data, Datasize, 0).

list_from_bin(_, Datasize, Datasize) ->
    [];
list_from_bin(Data, Datasize, Index) ->
    <<Pointer:?Size/unit:8>> = binary_part(Data, Index, 8),
    [Pointer | list_from_bin(Data, Datasize, Index + 8)].

list_to_bin([]) ->
    [];
list_to_bin([Head | Tail]) ->
    [<<Head:?Size/unit:8>> | list_to_bin(Tail)].

get_pointer_pos(List, OldPointer) ->
    Pos = string:str(List, [OldPointer]),
    (Pos - 1)*8.

replace_pointer(DBName, PointerPos, NewPointer) ->
    NewPointerBin = <<NewPointer:?Size/unit:8>>,
    {ok, File} = open_file(DBName, [read, write]),
    file:position(File, PointerPos),
    file:write(File, NewPointerBin),
    file:close(File),
    ok.

remove_pointer([], _) ->
    [];
remove_pointer([OldPointer | Tail], OldPointer) ->
    Tail;
remove_pointer([Head | Tail], OldPointer) ->
    [Head | remove_pointer(Tail, OldPointer)].