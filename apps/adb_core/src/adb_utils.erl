-module(adb_utils).

-export([get_env/1, get_env/2, gen_key/0, gen_name/0, choose_randomly/1, unique_list/1]).

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