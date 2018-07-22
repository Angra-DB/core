-module(gen_persistence).

-export([behaviour_info/1]).

-export([start/2, process_request/4]).

behaviour_info(callbacks) ->
  [{setup, 1}, {teardown, 1}, {createDB, 1}, {connect, 1}, {save, 3}, {lookup, 2}, {update, 3}, {delete, 2}].

start(_Child, _Args) ->  ok.

process_request(create_db, _, DB, Child) ->
     Child:createDB(DB);
process_request(connect, _, DB, Child) ->
    Child:connect(DB);
process_request(save, DB, Doc, Child) ->
    Child:save(DB, adb_utils:gen_key(), Doc);
process_request(save_key, DB, {Key, Doc}, Child) ->
    Child:save(DB, Key, Doc);    
process_request(lookup, DB, Key, Child) ->
    Child:lookup(DB, Key);
process_request(update, DB, {Key, Doc}, Child) ->
    Child:update(DB, Key, Doc);
process_request(delete, DB, Key, Child) ->
    Child:delete(DB, Key).
