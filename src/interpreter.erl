-module(interpreter).

-export([execute/0, start/0, insert/2, lookup/1]).

start() ->
    _Docs = ets:new(docs, [set, public, named_table]).

insert(Key, Value) ->
    ets:insert(docs, {Key, Value}).

lookup(Key) -> 
    ets:lookup(docs, Key).

gen_id() ->
    Time=erlang:system_time(nano_seconds),
    StringTime=integer_to_list(Time),
    UniformRandom=rand:uniform(10000),
    StringRandom=integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).

execute() ->
    receive 
	{From, {[save, Document]}} -> 
            Id = gen_id(), 
	    insert(Id, Document),
            From ! {self(), ok, Id};
        {From, {[lookup, Key]}} -> 
	    From ! {self(), lookup(Key)}
    end.
