-module(adb_server_tests).
-include_lib("eunit/include/eunit.hrl").

split_next_token_test_() -> 
  ParsedString = {"save", "{\"name\" : \"erlang\"}"},
  String1 = "save {\"name\" : \"erlang\"}",
  String2 = " save {\"name\" : \"erlang\"}",
  String3 = "  save {\"name\" : \"erlang\"}",
  String4 = "  save {\"name\" : \"erlang\"} ",
  String5 = "  save {\"name\" : \"erlang\"}  ",
  [ 
   ?_assert(ParsedString =:= adb_server:split_next_token(String1)),
   ?_assert(ParsedString =:= adb_server:split_next_token(String2)),
   ?_assert(ParsedString =:= adb_server:split_next_token(String3)),
   ?_assert(ParsedString =:= adb_server:split_next_token(String4)),
   ?_assert(ParsedString =:= adb_server:split_next_token(String5))
  ].