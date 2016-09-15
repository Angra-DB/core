-module(adb_server_tests).
-include_lib("eunit/include/eunit.hrl").

split_test_() -> 
  ParsedString = {"save", "{\"name\" : \"erlang\"}"},
  String1 = "save {\"name\" : \"erlang\"}",
  [ 
   ?_assert(ParsedString =:= adb_server:split(String1))
  ].
