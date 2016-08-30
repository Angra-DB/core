-module(strings).

-export([startsWith/2]).

startsWith(C, S) -> startsWith1(C, S, false).

startsWith1([], _, true) -> true;
startsWith1(_, [], _)    -> false;
startsWith1([C|CS], [X|XS], _) when C == X -> startsWith1(CS, XS, true);
startsWith1(_, _, _) -> false.
     
