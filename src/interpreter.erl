-module(interpreter).

-export([execute/0, start/0, insert/2, lookup/1]).

start() ->
    _Docs = ets:new(docs, [set, public, named_table]).

insert(Key, Value) ->
    ets:insert(docs, {Key, Value}).

lookup(Key) -> 
    ets:lookup(docs, Key).

    
execute() ->
    receive 
	{From, {[save, Key, Document]}} -> 
            {Id, _} = string:to_integer(Key), 
	    insert(Id, Document),
            From ! {self(), ok};
        {From, {[lookup, Key]}} -> 
            {Id, _} = string:to_integer(Key),  
	    From ! {self(), lookup(Id)}
    end.
