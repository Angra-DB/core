%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. ago 2018 11:18
%%%-------------------------------------------------------------------
-module(query_processor).
-author("ftfnunes").
-include("adbindexer.hrl").
-include("query_processor.hrl").

%% API
-compile(export_all).

process_query(Query, DbName, VNodeId) ->
  Statements = get_lines(Query),
  process_query(Statements, [], DbName).

process_query([], CurrentResult, _, _) ->
  CurrentResult;

process_query([S | Statements], CurrentResult, DbName, VNodeId) ->
  {Command, Args} = parse_statement(S),
  lager:info("Command = ~p, Args = ~p, CurrentResult = ~p", [Command, Args, CurrentResult]),
  process_command(list_to_atom(Command), Args, CurrentResult, DbName, Statements, VNodeId).

parse_statement(Line) ->
  [Command | Args] = split(Line, " "),
  {Command, Args}.


%% All process command functions should return a list of query_results

process_command('and', _Args, CurrentResult, DbName, Statements, VNodeId) ->
  RestResult = process_query(Statements, [], DbName, VNodeId),
  intersect(RestResult, CurrentResult);

process_command('or', _Args, CurrentResult, DbName, Statements, VNodeId) ->
  RestResult = process_query(Statements, [], DbName, VNodeId),
  join(RestResult, CurrentResult);

process_command(filter_field, FieldNames, CurrentResult, DbName, Statements, VNodeId) ->
  FieldResult = process_field(FieldNames, DbName, VNodeId),
  NewResult = case CurrentResult of
    [] -> filter_expression(FieldResult, none, fun is_in_interval/2, fun(I) -> I end);
    _ -> filter_expression(FieldResult, CurrentResult, fun is_in_interval/2, fun(I) -> I end)
  end,
  process_query(Statements, NewResult, DbName, VNodeId);

process_command(filter, Words, _, DbName, Statements, VNodeId) ->
  NewResult = process_expression(Words, DbName, VNodeId),
  process_query(Statements, NewResult, DbName, VNodeId).

process_expression([W | Words], DbName, VNodeId) ->
  CurrentExpressionResult = parse_query_term(W, DbName, VNodeId),
  MapFunction = fun(I = #doc_interval{ startPos = Start }) -> I#doc_interval{startPos = Start-1} end,
  case Words of
    [] -> filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentExpressionResult), none, fun is_sequence/2, MapFunction);
    _ ->
      TailExpressionResult = process_expression(Words, DbName, VNodeId),
      filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentExpressionResult), TailExpressionResult, fun is_sequence/2, MapFunction)
  end.

process_field([F | FieldNames], DbName, VNodeId) ->
  CurrentFieldResult = lists:map(fun convert_to_posting/1, indexer:query_term(DbName, F, VNodeId)),
  case FieldNames of
    [] -> filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentFieldResult), none, fun is_in_interval/2, fun(I) -> I end);
    _ ->
      InnerFieldResult = process_field(FieldNames, DbName, VNodeId),
      filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentFieldResult), InnerFieldResult, fun is_in_interval/2, fun(I) -> I end)
  end.


%% Result manipulation


filter_expression([], _, _, _) ->
  [];

filter_expression([CurrentExp | CurrentExps], none, FilterFunction, MapQueryResultFunction) ->
  {CurrentExpKey, CurrentExpPostings} = get_expression_item(CurrentExp),
  case filter_expression_positions(CurrentExpPostings, [], FilterFunction, MapQueryResultFunction) of
    [] ->
      filter_expression(CurrentExps, none, FilterFunction, MapQueryResultFunction);
    R -> [#query_result{ key = CurrentExpKey, positions = R } | filter_expression(CurrentExps, none, FilterFunction, MapQueryResultFunction)]
  end;

filter_expression(_, [], _, _) ->
  [];

filter_expression([C = CurrentExp | CurrentExps], [T = #query_result{ key = TailExpKey, positions = TailExpPositions } | TailExps], FilterFunction, MapQueryResultFunction) ->
  {CurrentExpKey, CurrentExpPostings} = get_expression_item(CurrentExp),
  case CurrentExpKey > TailExpKey of
    true ->
      filter_expression([C | CurrentExps], TailExps, FilterFunction, MapQueryResultFunction);
    false when CurrentExpKey < TailExpKey ->
      filter_expression(CurrentExps, [T | TailExps], FilterFunction, MapQueryResultFunction);
    false ->
      case filter_expression_positions(CurrentExpPostings, TailExpPositions, FilterFunction, MapQueryResultFunction) of
        [] -> 
          filter_expression(CurrentExps, TailExps, FilterFunction, MapQueryResultFunction);
        L ->
          [#query_result{ key = CurrentExpKey, positions = L } | filter_expression(CurrentExps, TailExps, FilterFunction, MapQueryResultFunction)]
      end
  end.

filter_expression_positions([], _, _, _) ->
  [];

filter_expression_positions(CurrentPostings, [], FilterFunction, _) ->
  Pred = fun(X) -> FilterFunction(none, X) end,
  [convert_to_result(P) || P <- CurrentPostings, Pred(P)];

filter_expression_positions([P | CurrentPostings], TailExpressionPositions, FilterFunction, MapQueryResultFunction) ->
  Pred = fun(X) -> FilterFunction(X, P) end,
  Map = [ MapQueryResultFunction(Interval) || Interval <- TailExpressionPositions, Pred(Interval) ],
  Map ++ filter_expression_positions(CurrentPostings, TailExpressionPositions, FilterFunction, MapQueryResultFunction).

is_in_interval(none, Posting) ->
  case Posting of
    #posting_ext{} -> true;
    #doc_interval{} -> true;
    _ -> false
  end;

is_in_interval(#doc_interval{ startPos = StartPos, endPos = EndPos }, #posting_ext{fieldStart = Start, fieldEnd = End}) ->
  (StartPos >= Start) and (EndPos =< End);

is_in_interval(#doc_interval{ startPos = StartPos, endPos = EndPos }, #doc_interval{startPos = Start, endPos = End}) ->
  (StartPos >= Start) and (EndPos =< End);

is_in_interval(_, _) ->
  lager:info("No match"),
  false.

is_sequence(none, Posting) ->
  case Posting of
    #posting{} -> true;
    #doc_interval{} -> true;
    _ -> false
  end;

is_sequence(#doc_interval{ startPos = StartPos }, #posting{docPos = DocPos}) ->
  DocPos == StartPos-1;

is_sequence(#doc_interval{ startPos = StartPos }, #doc_interval{endPos = EndPos}) ->
  EndPos == StartPos-1;

is_sequence(_, _) ->
  false.

get_expression_item(#query_result{ key = ExpKey, positions = ExpPositions }) ->
  {ExpKey, ExpPositions};

get_expression_item(E) ->
  E.

convert_to_posting(Tuple) ->
  PropList = tuple_to_list(Tuple),
  case length(PropList) of
      3 -> convert_to_posting(posting, PropList);
      5 -> convert_to_posting(posting_ext, PropList)
  end.

convert_to_posting(posting, PropList) ->
  list_to_tuple([posting|[proplists:get_value(X, PropList)
    || X <- record_info(fields, posting)]]);

convert_to_posting(posting_ext, PropList) ->
  list_to_tuple([posting_ext|[proplists:get_value(X, PropList)
    || X <- record_info(fields, posting_ext)]]).

group_by(F, L) -> lists:sort(fun compare_groups/2, dict:to_list(lists:foldr(fun({K,V}, D) -> dict:append(K, V, D) end , dict:new(), [ {F(X), X} || X <- L ]))).

parse_query_term(Word, DbName, VNodeId) ->
  lists:map(fun convert_to_posting/1, indexer:query_term(DbName, Word, VNodeId)).

convert_to_result(#posting{docPos = DocPos}) ->
  #doc_interval{ startPos = DocPos, endPos = DocPos };

convert_to_result(#posting_ext{fieldStart = Start, fieldEnd = End}) ->
  #doc_interval{ startPos = Start, endPos = End };

convert_to_result(R) ->
  R.

compare_groups({Key1, _}, {Key2, _}) ->
  Key1 =< Key2.

intersect([], _) ->
  [];

intersect(_, []) ->
  [];

intersect([Q1 = #query_result{key = Key1} | L1], [Q2 = #query_result{key = Key2} | L2]) ->
  case Key1 > Key2 of
      true -> intersect([Q1 | L1], L2);
      false when Key1 < Key2 -> intersect(L1, [Q2 | L2]);
      false -> [#query_result{key = Key1, positions = join_positions(Q1, Q2)} | intersect(L1, L2)]
  end.

join([], L2) ->
  L2;

join(L1, []) ->
  L1;

join([Q1 = #query_result{key = Key1} | L1], [Q2 = #query_result{key = Key2} | L2]) ->
  case Key1 > Key2 of
    true -> [Q2 | join([Q1 | L1], L2)];
    false when Key1 < Key2 -> [Q1 | join(L1, [Q2 | L2])];
    false -> [#query_result{key = Key1, positions = join_positions(Q1, Q2)} | join(L1, L2)]
  end.

join_positions(#query_result{positions = P1}, #query_result{positions = P2}) ->
  P1 ++ P2.

% String manipulation functions

get_lines(Query) ->
  split(Query, "/").

split(Str, Separators) ->
  Stripped = string:strip(Str),
  Lines = string:tokens(Stripped, Separators),
  remove_empty(Lines).

remove_empty(List) ->
  Pred = fun (X) -> X =/= "" end,
  lists:filter(Pred, List).