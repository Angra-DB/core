-module(adb_server_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([check_split_next_token/1]).

all() -> [check_split_next_token].

init_per_suite(_Config) -> 
    [].

end_per_suite(_Config) ->
    ok.

check_split_next_token(_Config) ->
  ParsedString = {"save", "{\"name\" : \"erlang\"}"},
  String1 = "save {\"name\" : \"erlang\"}",
  String2 = " save {\"name\" : \"erlang\"}",
  String3 = "  save {\"name\" : \"erlang\"}",
  String4 = "  save {\"name\" : \"erlang\"} ",
  String5 = "  save {\"name\" : \"erlang\"}  ",
  ParsedString = adb_server:split_next_token(String1),
  ParsedString = adb_server:split_next_token(String2),
  ParsedString = adb_server:split_next_token(String3),
  ParsedString = adb_server:split_next_token(String4),
  ParsedString = adb_server:split_next_token(String5),
  ok.