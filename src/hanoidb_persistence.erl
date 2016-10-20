-module(hanoidb_persistence).

-export([save/0, lookup/0, update/0, delete/0]).

save() ->
  receive
    {_From, Doc} -> B_Doc = list_to_binary(Doc),
                    _From ! {save, save(B_Doc)}
  end.

lookup() ->
  receive
    {_From, Key} -> B_Key = list_to_binary(Key),
                    _From ! {lookup, lookup(B_Key)}
  end.

delete() ->
  receive
    {_From, Key} -> B_Key = list_to_binary(Key),
                    _From ! {delete, delete(B_Key)}
  end.

update() ->
  receive
    {_From, {Key, Doc}} -> B_Key = list_to_binary(Key),
                           B_Doc = list_to_binary(Doc),
                           _From ! {update, update(B_Key, B_Doc)}
  end.

save(Value) ->
    Key = gen_id(),
    save(list_to_binary(Key), Value).
save(Key, Value) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    hanoidb:put(Tree, Key, Value),
    hanoidb:close(Tree),
    Key.

lookup(Key) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    Result = hanoidb:get(Tree, Key),
    hanoidb:close(Tree),
    Result.

delete(Key) ->
    {ok, Tree} = hanoidb:open_link("adb"),
    Result = hanoidb:delete(Tree, Key),
    hanoidb:close(Tree),
    Result.

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
