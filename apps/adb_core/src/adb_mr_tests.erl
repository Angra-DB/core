-module(adb_mr_tests).
-compile(export_all).

map_count_words(Doc) ->
    ok.

map1([]) ->
    map2();
map1([Head | Tail]) ->
    Head + map1(Tail).

map2() ->
    25000.
