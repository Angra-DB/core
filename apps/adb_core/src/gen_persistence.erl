-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/2, process_request/4]).

behaviour_info(callbacks) ->
  [{setup, 1}, {teardown, 1}, {createDB, 2}, {connect, 2}, {save, 4}, {lookup, 3}, {update, 4}, {delete, 3}, {query_term, 3}, {query, 3}, {bulk_lookup, 3}].

start(_Child, _Args) ->  ok.
	 
process_request(create_db, _, DB, Child, VNodeId) ->
	Child:createDB(DB, VNodeId);

process_request(connect, _, DB, Child, VNodeId) ->
	Child:connect(DB, VNodeId);

process_request(save, DB, {_, Doc}, Child, VNodeId) ->
	Child:save(DB, adb_utils:gen_key(), Doc, VNodeId);

process_request(save_key, DB, {Key, _, Doc}, Child, VNodeId) ->
	Child:save(DB, Key, Doc, VNodeId);    

process_request(lookup, DB, Key, Child, VNodeId) ->
	Child:lookup(DB, Key, VNodeId);

process_request(update, DB, {Key, _, Doc}, Child, VNodeId) ->
	Child:update(DB, Key, Doc, VNodeId);

process_request(delete, DB, Key, Child, VNodeId) ->
	Child:delete(DB, Key, VNodeId);

process_request(query_term, DB, Term, Child, VNodeId) ->
	Child:query_term(DB, Term, VNodeId);

process_request(query, DB, Term, Child, VNodeId) ->
	Child:query(DB, Term, VNodeId);

process_request(bulk_lookup, DB, Keys, Child, VNodeId) ->
	Child:bulk_lookup(DB, split(Keys, " "), VNodeId).

split(Str, Separators) ->
	Stripped = string:strip(Str),
	Lines = string:tokens(Stripped, Separators),
	remove_empty(Lines).
	
remove_empty(List) ->
	Pred = fun (X) -> X =/= "" end,
	lists:filter(Pred, List).