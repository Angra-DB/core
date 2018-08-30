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

process_query(Query, DbName) ->
  Statements = get_lines(Query),
  process_query(Statements, [], DbName).

process_query([], CurrentResult, DbName) ->
  CurrentResult;

process_query([S | Statements], CurrentResult, DbName) ->
  {Command, Args} = parse_statement(S),
  NewResult = process_command(Command, Args, CurrentResult, DbName),
  process_query(Statements, NewResult, DbName).

parse_statement(Line) ->
  [Command | Args] = split(Line, " "),
  {Command, Args}.


%% All process command functions should return a list of query_results

process_command(get_field, FieldNames, CurrentResult, DbName) ->
  process_field(FieldNames, DbName);

process_command(filter_expression, Words, CurrentResult, DbName) ->
  process_expression(Words, DbName).

process_expression([], DbName) ->
  [];

process_expression([W | Words], DbName) ->
  CurrentExpressionResult = parse_query_term(W, DbName),
  TailExpressionResult = process_expression(Words, DbName),
  MapFunction = fun(I = #doc_interval{ startPos = Start }) -> I#doc_interval{startPos = Start-1} end,
  filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentExpressionResult), TailExpressionResult, fun is_sequence/2, MapFunction).

process_field([F | FieldNames], DbName) ->
  CurrentFieldResult = lists:map(fun convert_to_posting/1, indexer:query_term(DbName, F)),
  InnerFieldResult = process_field(FieldNames, DbName),
  filter_expression(group_by(fun adbindexer:extract_key_from_record/1, CurrentFieldResult), InnerFieldResult, fun is_in_interval/2, fun(I) -> I end).

%% Result manipulation

filter_expression(_, [], _, _) ->
  [];

filter_expression([], _, _, _) ->
  [];

filter_expression([C = {CurrentExpKey, CurrentExpPostings} | CurrentExps], [T = #query_result{ key = TailExpKey, positions = TailExpPositions } | TailExps], FilterFunction, MapQueryResultFunction) ->
  case CurrentExpKey > TailExpKey of
    true ->
      filter_expression([C | CurrentExps], TailExps, FilterFunction, MapQueryResultFunction);
    false when CurrentExpKey < TailExpKey ->
      filter_expression(CurrentExps, [T | TailExps], FilterFunction, MapQueryResultFunction);
    false ->
      [#query_result{ key = CurrentExpKey, positions = filter_expression_positions(CurrentExpPostings, TailExpPositions, FilterFunction, MapQueryResultFunction) } | filter_expression(CurrentExps, TailExps, FilterFunction, MapQueryResultFunction)]
  end.

filter_expression_positions([], _, _, _) ->
  [];

filter_expression_positions(_, [], _, _) ->
  [];

filter_expression_positions([P | CurrentPostings], TailExpressionPositions, FilterFunction, MapQueryResultFunction) ->
  Pred = fun(X) -> FilterFunction(X, P) end,
  Map = [ MapQueryResultFunction(Interval) || Interval <- TailExpressionPositions, Pred(Interval) ],
  Map ++ filter_expression_positions(CurrentPostings, TailExpressionPositions, FilterFunction, MapQueryResultFunction);

filter_expression_positions([_ | CurrentPostings], TailExpressionPositions, FilterFunction, MapQueryResultFunction) ->
  filter_expression_positions(CurrentPostings, TailExpressionPositions, FilterFunction, MapQueryResultFunction).

is_in_interval(#doc_interval{ startPos = StartPos, endPos = EndPos }, #posting_ext{fieldStart = Start, fieldEnd = End}) ->
  (StartPos >= Start) and (EndPos =< End);

is_in_interval(_, _) ->
  false.

is_sequence(#doc_interval{ startPos = StartPos }, #posting{docPos = DocPos}) ->
  DocPos == StartPos-1;

is_sequence(_, _) ->
  false.

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

group_by(F, L) -> lists:foldr(fun({K,V}, D) -> dict:append(K, V, D) end , dict:new(), [ {F(X), X} || X <- L ]).

parse_query_term(Word, DbName) ->
  lists:map(fun convert_to_posting/1, indexer:query_term(DbName, Word)).

% String manipulation functions

get_lines(Query) ->
  split(Query, "~n").

split(Str, Separators) ->
  Stripped = string:strip(Str),
  Lines = string:tokens(Stripped, Separators),
  remove_empty(Lines).

remove_empty(List) ->
  Pred = fun (X) -> X =/= "" end,
  lists:filter(Pred, List).















%%
%%filter_inner_fields(_, []) ->
%%  [];
%%
%%filter_inner_fields([], _) ->
%%  [];
%%
%%filter_inner_fields([C = {CurrentFieldKey, CurrentFieldPostings} | CurrentFields], [I = #query_result{ key = InnerFieldKey, positions = InnerPositions } | InnerFields]) ->
%%  case CurrentFieldKey > InnerFieldKey of
%%      true ->
%%        filter_inner_fields([C | CurrentFields], InnerFields);
%%      false when CurrentFieldKey < InnerFieldKey ->
%%        filter_inner_fields(CurrentFields, [I | InnerFields]);
%%      false ->
%%        [#query_result{ key = CurrentFieldKey, positions = filter_inner_field_positions(CurrentFieldPostings, InnerPositions) } | filter_inner_fields(CurrentFields, InnerFields)]
%%  end.
%%
%%filter_inner_field_positions([], _) ->
%%  [];
%%
%%filter_inner_field_positions(_, []) ->
%%  [];
%%
%%filter_inner_field_positions([P = #posting_ext | CurrentFieldPostings], InnerFieldPostings, FilterFunction) ->
%%  Pred = fun(X) -> FilterFunction(X, P) end,
%%  lists:filter(Pred, InnerFieldPostings) ++ filter_inner_field_positions(CurrentFieldPostings, InnerFieldPostings);

%%filter_inner_field_positions([_ | CurrentFieldPostings], InnerFieldPostings) ->
%%  filter_inner_field_positions(CurrentFieldPostings, InnerFieldPostings).