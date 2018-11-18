-module(tests_utils).

-export([init_adb_dist_store/0, init_adb_dist_store/1, stop_adb_dist_store/0, init_mock_server/0, stop_mock_server/0]).

-define(DEFAULT_CONFIG, [{persistence, adbtree},
                         {distribution, dist},
                         {partition, full},
                         {replication, 3},
                         {write_quorum, 2},
                         {read_quorum, 2},
                         {vnodes, 3},
                         {gossip_interval, 60000},
                         {server, none}]).

init_adb_dist_store() ->
    init_adb_dist_store(?DEFAULT_CONFIG).

init_adb_dist_store(Config) ->
    adb_dist_store:start_link(Config).

stop_adb_dist_store() ->
    adb_dist_store:stop().

init_mock_server() ->
    mock_server:start_link().

stop_mock_server() ->
    mock_server:stop().