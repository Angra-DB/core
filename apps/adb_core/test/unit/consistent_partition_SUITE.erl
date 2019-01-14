-module(consistent_partition_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([check_create_db/1,
         check_connect/1,
         check_save/1,
         check_lookup/1,
         check_bulk_lookup/1,
         check_update/1,
         check_delete/1,
         check_query_term/1,
         check_query/1,
         check_map_key/1,
         check_map_keys/1,
         check_validate_hash/1,
         check_find_target_vnode/1,
         check_find_target/1,
         check_lookup_call/1,
         check_get_hash_func/1]).

-define(HASH_FUNCS, [md5, sha, sha256, sha384, sha512]).

all() ->
    [check_create_db,
     check_connect,
     check_save,
     check_lookup,
     check_bulk_lookup,
     check_update,
     check_delete,
     check_query_term,
     check_query,
     check_map_key,
     check_map_keys,
     check_validate_hash,
     check_find_target_vnode,
     check_find_target,
     check_lookup_call,
     check_get_hash_func].

init_per_suite(_Config) ->
    [].

end_per_suite(_Config) ->
    ok.

check_create_db(_Config) ->
    ok.

check_connect(_Config) ->
    ok.

check_save(_Config) ->
    ok.

check_lookup(_Config) ->
    ok.

check_bulk_lookup(_Config) ->
    ok.

check_update(_Config) ->
    ok.

check_delete(_Config) ->
    ok.

check_query_term(_Config) ->
    ok.

check_query(_Config) ->
    ok.

check_map_key(_Config) ->
    ok.

check_map_keys(_Config) ->
    ok.

check_validate_hash(_Config) ->
    ok.

check_find_target_vnode(_Config) ->
    ok.

check_find_target(_Config) ->
    ok.

check_lookup_call(_Config) ->
    ok.

check_get_hash_func(_Config) ->
    {ok, _Pid} = tests_utils:init_adb_dist_store(),
    lists:foreach(fun(HashFunc) ->
        adb_dist_store:set_config(partition, {consistent, HashFunc}),
        HashFunc = consistent_partition:get_hash_func()
    end, ?HASH_FUNCS),
    ok = tests_utils:stop_adb_dist_store(),
    ok.
