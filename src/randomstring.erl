% Code taken from https://github.com/yazdotai/randomstring

-module(randomstring).
-export([get/2]).

get(Length, AllowedChars) ->
  lists:foldl(fun(_, Acc) ->
    [lists:nth(rand:uniform(length(AllowedChars)),
      AllowedChars) | Acc]
              end, [], lists:seq(1, Length)).
