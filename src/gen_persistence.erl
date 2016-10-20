-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/1, process_request/1, gen_key/0]).

behaviour_info(callbacks) ->
  [{save, 2}, {lookup, 1}, {update, 2}, {delete, 1}].

start(Child) ->
  spawn(gen_persistence, process_request, [Child]).

process_request(Child) ->
  receive
    {_From, save, Doc} ->
      _From ! {save, Child:save(gen_key(), Doc)};
    {_From, lookup, Key} ->
      _From ! {lookup, Child:lookup(Key)};
    {_From, update, {Key, Doc}} ->
      _From ! {update, Child:update(Key, Doc)};
    {_From, delete, Key} ->
      _From ! {delete, Child:delete(Key)}
  end.

gen_key() ->
    Time = erlang:system_time(nano_seconds),
    StringTime = integer_to_list(Time),
    UniformRandom = rand:uniform(10000),
    StringRandom = integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).
