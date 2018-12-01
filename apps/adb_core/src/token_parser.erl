-module(token_parser).
-compile(export_all).
-include("adbindexer.hrl").
-import(jsone, [decode/2]).



%
%
%
% Jsone parser turns strings into binaries ( "hello" -> <<"hello">>)
%
%
%



receive_json(Json_Orig) ->
  Json_proplist = decode(Json_Orig, [{object_format, proplist}]),
  Token_list = json_parser(Json_proplist, 0),
  {ok, Token_list}.

json_parser(Element, Count) when is_atom(Element) ->
  [#token{word=atom_to_list(Element), docPos=Count}];
json_parser(Element, Count) when is_integer(Element) ->
  [#token{word=integer_to_list(Element), docPos=Count}];
json_parser(Element, Count) when is_float(Element) ->
  [#token{word=float_to_list(Element, [{decimals, 8}, compact]), docPos=Count}];
json_parser(<<Element/binary>>, Count) ->
  ElementList = string:tokens(binary_to_list(Element), " "),
  tokenize_string(ElementList, Count);

json_parser([], _) ->
  [];
json_parser([Head | Tail], Count) when is_atom(Head) ->
  [#token{word=atom_to_list(Head), docPos=Count} | json_parser(Tail, Count+1)];
json_parser([Head | Tail], Count) when is_integer(Head) ->
  [#token{word=integer_to_list(Head), docPos=Count} | json_parser(Tail, Count+1)];
json_parser([Head | Tail], Count) when is_float(Head) ->
  [#token{word=float_to_list(Head, [{decimals, 8}, compact]), docPos=Count} | json_parser(Tail, Count+1)];
json_parser([<<Head/binary>> | Tail], Count) ->
  HeadList = string:tokens(binary_to_list(Head), " "),
  lists:append(tokenize_string(HeadList, Count), json_parser(Tail, Count+length(HeadList)));

json_parser([{<<Header/binary>>, Content} | Tail], Count) ->
  HeadList = string:tokens(binary_to_list(Header), " "),
  InitField = length(HeadList)+Count,
  HeaderTokenList = tokenize_head_list(HeadList, InitField, Count),
  ContentList = json_parser(Content, InitField),
  FinalHeaderTokens = lists:map(fun(Token) -> Token#token_ext{fieldEnd = InitField+length(ContentList)-1} end, HeaderTokenList),
  NextPos = InitField+length(ContentList),
  lists:append([FinalHeaderTokens, ContentList, json_parser(Tail, NextPos)]);

json_parser(_, _) ->
  throw(invalid_json).


tokenize_head_list([], _, _) ->
  [];
tokenize_head_list([Head | Tail], InitField, Count) ->
  [#token_ext{word=Head, docPos=Count, fieldStart=InitField, fieldEnd=-1} | tokenize_head_list(Tail, InitField, Count+1)].

tokenize_string([], _) ->
  [];
tokenize_string([Head | Tail], Count) -> 
  [#token{word=Head, docPos=Count} | tokenize_string(Tail, Count+1)].

