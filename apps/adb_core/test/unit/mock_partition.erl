-module(mock_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, update/3, delete/2]).

%%=============================================================================
%% gen_partition callbacks
%%=============================================================================

create_db(Database) ->
    {ok, {Database}}.

connect(Database) ->
    {ok, {Database}}.

save(Database, {Key, HashFunc}, Doc) ->
    {ok, {Database, Key, HashFunc, Doc}};
save(Database, Key, Doc) ->
    {ok, {Database, Key, Doc}}.

lookup(Database, {Key, HashFunc}) ->
    {ok, {Database, Key, HashFunc}};
lookup(Database, Key) ->
    {ok, {Database, Key}}.

update(Database, {Key, HashFunc}, Doc) ->
    {ok, {Database, Key, HashFunc, Doc}};
update(Database, Key, Doc) ->
    {ok, {Database, Key, Doc}}.

delete(Database, {Key, HashFunc}) ->
    {ok, {Database, Key, HashFunc}};
delete(Database, Key) ->
    {ok, {Database, Key}}.

%%==============================================================================
%% Internal functions
%%==============================================================================