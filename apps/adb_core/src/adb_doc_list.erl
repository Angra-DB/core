-module(adb_doc_list).

-compile(export_all).

-define(Size, 8).

generate_list(DBName, List) ->
    file:rename(get_file_name(DBName), get_replaced_file_name(DBName)),
    file:write_file(get_file_name(DBName), List),
    file:delete(get_replaced_file_name(DBName)),
    ok.

% generate_list(DBName) ->

%     file:write_file(get_file_name(DBName), List),
%     ok.

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


