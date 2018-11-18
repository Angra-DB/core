-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/2, process_request/5]).

behaviour_info(callbacks) ->
  [{setup, 1}, {teardown, 1}, {createDB, 2}, {connect, 2}, {save, 3}, {lookup, 2}, {update, 3}, {delete, 2}, {query_term, 2}, {query, 2}, {bulk_lookup, 2}].

start(_Child, _Args) ->  ok.
	 
process_request(create_db, _, DB, Child, Settings) ->
	Child:createDB(DB, Settings);

process_request(connect, _, DB, Child, Settings) ->
	Child:connect(DB, Settings);

process_request(save, DB, {_, Doc}, Child, _) ->
	Child:save(DB, adb_utils:gen_key(), Doc);

process_request(save_key, DB, {Key, _, Doc}, Child, _) ->
	Child:save(DB, Key, Doc);    

process_request(lookup, DB, Key, Child, _) ->
	Child:lookup(DB, Key);

process_request(update, DB, {Key, _, Doc}, Child, _) ->
	Child:update(DB, Key, Doc);

process_request(delete, DB, Key, Child, _) ->
	Child:delete(DB, Key);

process_request(query_term, DB, Term, Child, _) ->
	Child:query_term(DB, Term);

process_request(query, DB, Term, Child, _) ->
	Child:query(DB, Term);

process_request(bulk_lookup, DB, Keys, Child, _) ->
	Child:bulk_lookup(DB, split(Keys, " ")).

split(Str, Separators) ->
	Stripped = string:strip(Str),
	Lines = string:tokens(Stripped, Separators),
	remove_empty(Lines).
	
remove_empty(List) ->
	Pred = fun (X) -> X =/= "" end,
	lists:filter(Pred, List).