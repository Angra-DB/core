-module(adb_mr_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [#{id => adb_mr,
                    start => {adb_mr, start, []},
                    restart => permanent,
                    type => worker,
                    modules => [adb_mr]}],
    {ok, {SupFlags, ChildSpecs}}.