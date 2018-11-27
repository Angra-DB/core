-module(gen_partition_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([check_start/1, 
         check_forward_request/1, 
         check_multi_call/1, 
         check_validate_response/1, 
         check_generate_stats/1, 
         check_choose_response/1]).

all() -> 
    [check_start, 
     check_forward_request, 
     check_multi_call, 
     check_validate_response, 
     check_generate_stats, 
     check_choose_response].

init_per_suite(_Config) -> 
    [].

end_per_suite(_Config) ->
    ok.

check_start(_Config) ->
    ok = gen_partition:start(test_child_module, []),
    ok.

check_forward_request(_Config) ->
    ChildMock = mock_partition,
    Database = "database test",
    Key = "key test",
    Keys = ["key1", "key2", "key3"],
    HashFunc = hash_test,
    Doc = "doc test",
    Size = 8,
    Term = {foo, bar},
    Query = "Hello World",
    {ok, Database} = gen_partition:forward_request(create_db, Database, {ChildMock, HashFunc}),
    {ok, Database} = gen_partition:forward_request(create_db, Database, ChildMock),
    {ok, Database} = gen_partition:forward_request(connect, Database, {ChildMock, HashFunc}),
    {ok, Database} = gen_partition:forward_request(connect, Database, ChildMock),
    {ok, {Database, _, HashFunc, Size, Doc}} = gen_partition:forward_request(save, {Database, {Size, Doc}}, {ChildMock, HashFunc}),
    {ok, {Database, _, Size, Doc}} = gen_partition:forward_request(save, {Database, {Size, Doc}}, ChildMock),
    {ok, {Database, Key, HashFunc, Size, Doc}} = gen_partition:forward_request(save_key, {Database, {Key, Size, Doc}}, {ChildMock, HashFunc}),
    {ok, {Database, Key, Size, Doc}} = gen_partition:forward_request(save_key, {Database, {Key, Size, Doc}}, ChildMock),
    {ok, {Database, Key, HashFunc}} = gen_partition:forward_request(lookup, {Database, Key}, {ChildMock, HashFunc}),
    {ok, {Database, Key}} = gen_partition:forward_request(lookup, {Database, Key}, ChildMock),
    {ok, {Database, Keys, HashFunc}} = gen_partition:forward_request(bulk_lookup, {Database, Keys}, {ChildMock, HashFunc}),
    {ok, {Database, Keys}} = gen_partition:forward_request(bulk_lookup, {Database, Keys}, ChildMock),
    {ok, {Database, Key, HashFunc, Size, Doc}} = gen_partition:forward_request(update, {Database, {Key, Size, Doc}}, {ChildMock, HashFunc}),
    {ok, {Database, Key, Size, Doc}} = gen_partition:forward_request(update, {Database, {Key, Size, Doc}}, ChildMock),
    {ok, {Database, Key, HashFunc}} = gen_partition:forward_request(delete, {Database, Key}, {ChildMock, HashFunc}),
    {ok, {Database, Key}} = gen_partition:forward_request(delete, {Database, Key}, ChildMock),
    {ok, {Database, Term}} = gen_partition:forward_request(query_term, {Database, Term}, {ChildMock, HashFunc}),
    {ok, {Database, Term}} = gen_partition:forward_request(query_term, {Database, Term}, ChildMock),
    {ok, {Database, Query}} = gen_partition:forward_request(query, {Database, Query}, {ChildMock, HashFunc}),
    {ok, {Database, Query}} = gen_partition:forward_request(query, {Database, Query}, ChildMock),
    ok.

check_multi_call(_Config) ->
    ServerMock = mock_server,
    {ok, _Pid} = tests_utils:init_mock_server(),
    Request1 = one,
    Request2 = two,
    Request3 = three,
    [{success, {1, [[]]}}, {failed, {0, []}}] = gen_partition:multi_call([node()], ServerMock, Request1),
    [{success, {0, []}}, {failed, {1, [[]]}}] = gen_partition:multi_call([node()], ServerMock, Request2),
    [{success, {1, [three]}}, {failed, {0, []}}] = gen_partition:multi_call([node()], ServerMock, Request3),
    [{success, {2, [three]}}, {failed, {0, []}}] = gen_partition:multi_call([node(),node()], ServerMock, Request3),
    [{success, {3, [three]}}, {failed, {0, []}}] = gen_partition:multi_call([node(), node(), node()], ServerMock, Request3),
    ok = tests_utils:stop_mock_server(),
    ok.

check_validate_response(_Config) ->
    {ok, _Pid} = tests_utils:init_adb_dist_store(),
    Stats1 = [{success, {1, [one]}}, {failed, {0, []}}],
    Stats2 = [{success, {2, [one, two]}}, {failed, {0, []}}],
    Stats3 = [{success, {3, [one, two, three]}}, {failed, {0, []}}],
    {success, one} = gen_partition:validate_response(Stats1, 1, write),
    {success, one} = gen_partition:validate_response(Stats1, 1, read),
    {failed, none} = gen_partition:validate_response(Stats1, 2, write),
    {failed, none} = gen_partition:validate_response(Stats1, 2, read),
    {failed, none} = gen_partition:validate_response(Stats1, 3, write),
    {failed, none} = gen_partition:validate_response(Stats1, 3, read),
    {error, invalid_stats} = gen_partition:validate_response(Stats2, 1, write),
    {error, invalid_stats} = gen_partition:validate_response(Stats2, 1, read),
    {success, one} = gen_partition:validate_response(Stats2, 2, write),
    {success, one} = gen_partition:validate_response(Stats2, 2, read),
    {success, one} = gen_partition:validate_response(Stats2, 3, write),
    {success, one} = gen_partition:validate_response(Stats2, 3, read),
    {error, invalid_stats} = gen_partition:validate_response(Stats3, 1, write),
    {error, invalid_stats} = gen_partition:validate_response(Stats3, 1, read),
    {error, invalid_stats} = gen_partition:validate_response(Stats3, 2, write),
    {error, invalid_stats} = gen_partition:validate_response(Stats3, 2, read),
    {success, one} = gen_partition:validate_response(Stats3, 3, write),
    {success, one} = gen_partition:validate_response(Stats3, 3, read),
    ok = tests_utils:stop_adb_dist_store(),
    ok.

check_generate_stats(_Config) ->
    Responses1 = [{ok, one}, {ok, two}, {error, one}, {error, two}],
    Responses2 = [{ok, one}, {ok, one}, {error, one}, {error, one}],
    Responses3 = [{ok, one}, {ok, two}],
    Responses4 = [{error, one}, {error, two}],
    [{success, {2, [one, two]}}, {failed, {2, [one, two]}}]  = gen_partition:generate_stats(Responses1),
    [{success, {2, [one]}}, {failed, {2, [one]}}] = gen_partition:generate_stats(Responses2),
    [{success, {2, [one, two]}}, {failed, {0, []}}] = gen_partition:generate_stats(Responses3),
    [{success, {0, []}}, {failed, {2, [one, two]}}] = gen_partition:generate_stats(Responses4).

check_choose_response(_Config) ->
    Param1 = [],
    Param2 = [one],
    Param3 = [{one, two}],
    Param4 = [{one, two}, {three, four}],
    Param5 = one,
    none = gen_partition:choose_response(Param1),
    one = gen_partition:choose_response(Param2),
    {one, two} = gen_partition:choose_response(Param3),
    {one, two} = gen_partition:choose_response(Param4),
    try gen_partition:choose_response(Param5)
    catch error:function_clause ->
        ok
    end,
    ok.