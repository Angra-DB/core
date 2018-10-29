-module(adb_utils).

-export([get_env/1, get_env/2, gen_key/0, gen_name/0, choose_randomly/1, unique_list/1, get_vnode_name/1, get_vnode_name/2, get_database_name/2, sort_ring_info/1]).

get_env(EnvVar) ->
    case os:getenv(EnvVar, none) of
        none  -> application:get_env(EnvVar);
        Value -> {ok, Value}
    end.

get_env(EnvVar, Default) ->
     case os:getenv(EnvVar, none) of
        none  -> case application:get_env(EnvVar) of
                    undefined -> Default;
                    Value     -> Value
                 end;
        Value -> Value
    end.

gen_key() ->
    Time = erlang:system_time(nano_seconds),
    StringTime = integer_to_list(Time),
    UniformRandom = rand:uniform(10000),
    StringRandom = integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).

gen_name() ->
    UniformRandom = rand:uniform(10000),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, UniformRandom).

choose_randomly(List) ->
    Index = rand:uniform(length(List)),
    lists:nth(Index, List).

unique_list(List) ->
    gb_sets:to_list(gb_sets:from_list(List)).

get_vnode_name(Id) ->
    {ok, VNodes} = adb_dist_store:get_config(vnodes),
    get_vnode_name(Id, VNodes).

get_vnode_name(_, 1) ->
    {ok, adb_persistence};
get_vnode_name(Id, VNodes) when VNodes > 1 ->
    Name = string:concat("adb_persistence_", integer_to_list(Id)),
    {ok, list_to_atom(Name)}.

get_database_name(Database, adb_persistence) -> Database;
get_database_name(Database, VNodeName) when is_atom(VNodeName) ->
    string:join([Database, "@", atom_to_list(VNodeName)], "").

sort_ring_info(RingInfo) ->
    Pred = fun({_, {ANum, ADen}}, {_, {BNum, BDen}}) -> (ANum / ADen) =< (BNum / BDen) end,
    Sorted = lists:sort(Pred, RingInfo),
    {ok, Sorted}.