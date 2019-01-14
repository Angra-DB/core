-module(mock_partition).
-behaviour(gen_partition).

-export([create_db/1, connect/1, save/3, lookup/2, bulk_lookup/2, update/3, delete/2, query_term/2, query/2]).

%%=============================================================================
%% gen_partition callbacks
%%=============================================================================

create_db(Database) ->
    {ok, Database}.

connect(Database) ->
    {ok, Database}.

save(Database, Key, {Size, Doc}) ->
    {ok, {Database, Key, Size, Doc}}.

lookup(Database, Key) ->
    {ok, {Database, Key}}.

bulk_lookup(Database, Keys) ->
    {ok, {Database, Keys}}.

update(Database, Key, {Size, Doc}) ->
    {ok, {Database, Key, Size, Doc}}.

delete(Database, Key) ->
    {ok, {Database, Key}}.

query_term(Database, Term) ->
    {ok, {Database, Term}}.

query(Database, Query) ->
    {ok, {Database, Query}}.

%%==============================================================================
%% Internal functions
%%==============================================================================