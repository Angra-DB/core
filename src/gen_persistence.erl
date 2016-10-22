-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/2, process_request/2, gen_key/0]).

behaviour_info(callbacks) ->
  [{setup, 1}, {teardown, 1}, {save, 3}, {lookup, 2}, {update, 3}, {delete, 2}].

start(Child, Args) ->
  spawn(gen_persistence, process_request, [Child, Args]).

process_request(Child, Args) ->
  State = Child:setup(Args),
  receive
    {_From, save, Doc} ->
      _From ! {save, Child:save(State, gen_key(), Doc)};
    {_From, lookup, Key} ->
      _From ! {lookup, Child:lookup(State, Key)};
    {_From, update, {Key, Doc}} ->
      _From ! {update, Child:update(State, Key, Doc)};
    {_From, delete, Key} ->
      _From ! {delete, Child:delete(State, Key)}
  end,
  Child:teardown(State).

gen_key() ->
    Time = erlang:system_time(nano_seconds),
    StringTime = integer_to_list(Time),
    UniformRandom = rand:uniform(10000),
    StringRandom = integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).
