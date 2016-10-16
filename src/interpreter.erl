-module(interpreter).

-export([process_request/0, process_request/2 , process_request/3]).

insert(Value) ->
    Key = gen_id(), 
    insert(Key, Value).
insert(Key, Value) ->
    ets:insert(docs, {Key, Value}),
    Key.

lookup(Key) -> 
    ets:lookup(docs, Key).

delete(Key) ->
    ets:delete(docs, Key).

update(Key, Value) ->
    delete(Key),
    insert(Key,Value).

process_request() ->
    receive
        {_From, save, Doc}        -> _From ! process_request(save, Doc);
        {_From, lookup, Key}      -> _From ! process_request(lookup, Key);
        {_From, delete, Key}      -> _From ! process_request(delete, Key);
        {_From, update, Key, Doc} -> _From ! process_request(update, Key, Doc);
        _ -> throw(invalid_command)
    end.

process_request(save, Document) ->
    {save, insert(Document)};
process_request(lookup, Key) ->
    {lookup, lookup(Key)};
process_request(delete, Key) ->
    {delete, delete(Key)}.
process_request(update, Key, Document) ->
    {update, update(Key, Document)}.

gen_id() ->
    Time=erlang:system_time(nano_seconds),
    StringTime=integer_to_list(Time),
    UniformRandom=rand:uniform(10000),
    StringRandom=integer_to_list(UniformRandom),
    {Id, _} = string:to_integer(StringRandom++StringTime),
    Ctx = hashids:new([{salt, "AngraDB"}, {min_hash_length, 1}, {default_alphabet, "ABCDEF0123456789"}]),
    hashids:encode(Ctx, Id).
