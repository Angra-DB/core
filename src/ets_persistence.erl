-module(ets_persistence).

-export([save/0, lookup/0, update/0, delete/0]).

save() ->
  receive
    {_From, Doc} -> _From ! {save, save(Doc)}
  end.

lookup() ->
  receive
    {_From, Key} -> _From ! {lookup, lookup(Key)}
  end.

delete() ->
  receive
    {_From, Key} -> _From ! {delete, delete(Key)}
  end.

update() ->
  receive
    {_From, {Key, Doc}} -> _From ! {update, update(Key, Doc)}
  end.

save(Value) ->
    Key = gen_id(), 
    save(Key, Value).
save(Key, Value) ->
    ets:insert(docs, {Key, Value}),
    Key.

lookup(Key) -> 
    ets:lookup(docs, Key).

delete(Key) ->
    ets:delete(docs, Key).

update(Key, Value) ->
    delete(Key),
    save(Key,Value).

gen_id() ->
    Time=erlang:system_time(nano_seconds),
    StringTime=integer_to_list(Time),
    UniformRandom=rand:uniform(10000),
    StringRandom=integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).
