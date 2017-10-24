-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/2, process_request/4, gen_key/0]).

behaviour_info(callbacks) ->
  [{setup, 1}, {teardown, 1}, {createDB, 1}, {connect, 1}, {save, 3}, {lookup, 2}, {update, 3}, {delete, 2}].

start(_Child, _Args) ->  ok. 

process_request(create_db, _, DB, Child) ->
     Child:createDB(DB);

process_request(connect, _, DB, Child) ->
    Child:connect(DB);

process_request(save, DB, Doc, Child) -> 
    Child:save(DB, gen_key(), Doc);

process_request(lookup, DB, Key, Child) -> 
    Child:lookup(DB, Key);

process_request(update, DB, {Key, Doc}, Child) -> 
    Child:update(DB, Key, Doc);

process_request(delete, DB, Key, Child) ->
    Child:delete(DB, Key). 
 

gen_key() ->
    Time = erlang:system_time(nano_seconds),
    StringTime = integer_to_list(Time),
    UniformRandom = rand:uniform(10000),
    StringRandom = integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).
