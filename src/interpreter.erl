-module(interpreter).

-export([execute/0, start/0, insert/2, lookup/1]).

start() ->
    _Docs = ets:new(docs, [set, public, named_table]),
    execute. 

insert(Key, Value) ->
    ets:insert(docs, {Key, Value}).

lookup(Key) -> 
    ets:lookup(docs, Key).

    
execute() ->
    receive 
	{From, {[save, Key,Document]}} -> 
            io:format("saving the document with key: ~p~n", [key]),
	    io:format("saving the document: ~p~n", [Document]),
            insert(Key, Document),
	    From ! {self(), ok}
    end.
