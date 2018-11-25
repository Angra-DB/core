-module(gen_partition_tests).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    ?assertEqual(ok, gen_partition:start(test_child_module, [])).

forward_request_test() ->
    ChildMock = mock_partition,
    Database = "database test",
    Key = "key test",
    Keys = ["key1", "key2", "key3"],
    HashFunc = hash_test,
    Doc = "doc test",
    Size = 8,
    Term = {foo, bar},
    Query = "Hello World",
    [?assertEqual({ok, Database}, gen_partition:forward_request(create_db, Database, {ChildMock, HashFunc})),
     ?assertEqual({ok, Database}, gen_partition:forward_request(create_db, Database, ChildMock)),
     ?assertEqual({ok, Database}, gen_partition:forward_request(connect, Database, {ChildMock, HashFunc})),
     ?assertEqual({ok, Database}, gen_partition:forward_request(connect, Database, ChildMock)),
     ?assertMatch({ok, {Database, _, HashFunc, Size, Doc}}, gen_partition:forward_request(save, {Database, {Size, Doc}}, {ChildMock, HashFunc})),
     ?assertMatch({ok, {Database, _, Size, Doc}}, gen_partition:forward_request(save, {Database, {Size, Doc}}, ChildMock)),
     ?assertEqual({ok, {Database, Key, HashFunc, Size, Doc}}, gen_partition:forward_request(save_key, {Database, {Key, Size, Doc}}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Key, Size, Doc}}, gen_partition:forward_request(save_key, {Database, {Key, Size, Doc}}, ChildMock)),
     ?assertEqual({ok, {Database, Key, HashFunc}}, gen_partition:forward_request(lookup, {Database, Key}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Key}}, gen_partition:forward_request(lookup, {Database, Key}, ChildMock)),
     ?assertEqual({ok, {Database, Keys, HashFunc}}, gen_partition:forward_request(bulk_lookup, {Database, Keys}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Keys}}, gen_partition:forward_request(bulk_lookup, {Database, Keys}, ChildMock)),
     ?assertEqual({ok, {Database, Key, HashFunc, Size, Doc}}, gen_partition:forward_request(update, {Database, {Key, Size, Doc}}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Key, Size, Doc}}, gen_partition:forward_request(update, {Database, {Key, Size, Doc}}, ChildMock)),
     ?assertEqual({ok, {Database, Key, HashFunc}}, gen_partition:forward_request(delete, {Database, Key}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Key}}, gen_partition:forward_request(delete, {Database, Key}, ChildMock)),
     ?assertEqual({ok, {Database, Term}}, gen_partition:forward_request(query_term, {Database, Term}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Term}}, gen_partition:forward_request(query_term, {Database, Term}, ChildMock)),
     ?assertEqual({ok, {Database, Query}}, gen_partition:forward_request(query, {Database, Query}, {ChildMock, HashFunc})),
     ?assertEqual({ok, {Database, Query}}, gen_partition:forward_request(query, {Database, Query}, ChildMock))].


multi_call_test() ->
    ServerMock = mock_server,
    {ok, _Pid} = tests_utils:init_mock_server(),
    Request1 = one,
    Request2 = two,
    Request3 = three,
    Asserts = 
        [?assertEqual([{success, {1, [[]]}}, {failed, {0, []}}], gen_partition:multi_call([node()], ServerMock, Request1)),
         ?assertEqual([{success, {0, []}}, {failed, {1, [[]]}}], gen_partition:multi_call([node()], ServerMock, Request2)),
         ?assertEqual([{success, {1, [three]}}, {failed, {0, []}}], gen_partition:multi_call([node()], ServerMock, Request3)),
         ?assertEqual([{success, {2, [three]}}, {failed, {0, []}}], gen_partition:multi_call([node(),node()], ServerMock, Request3)),
         ?assertEqual([{success, {3, [three]}}, {failed, {0, []}}], gen_partition:multi_call([node(), node(), node()], ServerMock, Request3))],
    ok = tests_utils:stop_mock_server(),
    Asserts.

validate_response_test() ->
    {ok, _Pid} = tests_utils:init_adb_dist_store(),
    Stats1 = [{success, {1, [one]}}, {failed, {0, []}}],
    Stats2 = [{success, {2, [one, two]}}, {failed, {0, []}}],
    Stats3 = [{success, {3, [one, two, three]}}, {failed, {0, []}}],
    Asserts = 
        [?assertEqual({success, one}, gen_partition:validate_response(Stats1, 1, write)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats1, 1, read)),
         ?assertEqual({failed, none}, gen_partition:validate_response(Stats1, 2, write)),
         ?assertEqual({failed, none}, gen_partition:validate_response(Stats1, 2, read)),
         ?assertEqual({failed, none}, gen_partition:validate_response(Stats1, 3, write)),
         ?assertEqual({failed, none}, gen_partition:validate_response(Stats1, 3, read)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats2, 1, write)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats2, 1, read)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats2, 2, write)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats2, 2, read)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats2, 3, write)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats2, 3, read)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats3, 1, write)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats3, 1, read)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats3, 2, write)),
         ?assertEqual({error, invalid_stats}, gen_partition:validate_response(Stats3, 2, read)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats3, 3, write)),
         ?assertEqual({success, one}, gen_partition:validate_response(Stats3, 3, read))],
    ok = tests_utils:stop_adb_dist_store(),
    Asserts.

generate_stats_test() ->
    Responses1 = [{ok, one}, {ok, two}, {error, one}, {error, two}],
    Responses2 = [{ok, one}, {ok, one}, {error, one}, {error, one}],
    Responses3 = [{ok, one}, {ok, two}],
    Responses4 = [{error, one}, {error, two}],
    [?assertEqual([{success, {2, [one, two]}}, {failed, {2, [one, two]}}], gen_partition:generate_stats(Responses1)),
     ?assertEqual([{success, {2, [one]}}, {failed, {2, [one]}}], gen_partition:generate_stats(Responses2)),
     ?assertEqual([{success, {2, [one, two]}}, {failed, {0, []}}], gen_partition:generate_stats(Responses3)),
     ?assertEqual([{success, {0, []}}, {failed, {2, [one, two]}}], gen_partition:generate_stats(Responses4))].

choose_response_test() ->
    Param1 = [],
    Param2 = [one],
    Param3 = [{one, two}],
    Param4 = [{one, two}, {three, four}],
    Param5 = one,
    [?assertEqual(none, gen_partition:choose_response(Param1)),
     ?assertEqual(one, gen_partition:choose_response(Param2)),
     ?assertEqual({one, two}, gen_partition:choose_response(Param3)),
     ?assertEqual({one, two}, gen_partition:choose_response(Param4)),
     ?assertError(function_clause, gen_partition:choose_response(Param5))].