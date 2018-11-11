-module(adb_mr_tests).
-compile(export_all).

map(Doc) ->
    word_count(Doc).


reduce([]) ->
    0;
reduce([Head | Tail]) ->
    Head + reduce(Tail).

merge(List) ->
    reduce(List).

word_count(Element) when is_atom(Element) ->
  1;
word_count(Element) when is_integer(Element) ->
  1;
word_count(Element) when is_float(Element) ->
  1;
word_count(<<Element/binary>>) ->
  ElementList = string:tokens(binary_to_list(Element), " "),
  length(ElementList);

word_count([]) ->
  0;
word_count([Head | Tail]) when is_atom(Head) ->
  1 + word_count(Tail);
word_count([Head | Tail]) when is_integer(Head) ->
  1 + word_count(Tail);
word_count([Head | Tail]) when is_float(Head) ->
  1 + word_count(Tail);
word_count([<<Head/binary>> | Tail]) ->
  HeadList = string:tokens(binary_to_list(Head), " "),
  length(HeadList) + word_count(Tail);

word_count([{<<Header/binary>>, Content} | Tail]) ->
  HeadList = string:tokens(binary_to_list(Header), " "),
  HeadLength = length(HeadList),
  ContentLength = word_count(Content),
  HeadLength + ContentLength + word_count(Tail).