%%%-------------------------------------------------------------------
%%% @author ftfnunes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. ago 2018 19:17
%%%-------------------------------------------------------------------
-author("ftfnunes").

-record(query_result, {key, positions}).
-record(doc_interval, {startPos, endPos}).
-record(doc_position, {docPos}).