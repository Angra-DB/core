-module(adb_server_tests).
-include_lib("eunit/include/eunit.hrl").

split_test_() -> 
  ParsedString = {"save", "{\"name\" : \"erlang\"}"},
  String1 = "save {\"name\" : \"erlang\"}",
  String2 = " save {\"name\" : \"erlang\"}",
  String3 = "  save {\"name\" : \"erlang\"}",
  String4 = "  save {\"name\" : \"erlang\"} ",
  String5 = "  save {\"name\" : \"erlang\"}  ",
  [ 
   ?_assert(ParsedString =:= adb_server:split(String1)),
   ?_assert(ParsedString =:= adb_server:split(String2)),
   ?_assert(ParsedString =:= adb_server:split(String3)),
   ?_assert(ParsedString =:= adb_server:split(String4)),
   ?_assert(ParsedString =:= adb_server:split(String5))
  ].
