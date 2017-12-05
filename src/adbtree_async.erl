% ------------------------------------------------
% @author Joao Vasconcelos <ninovasc@gmail.com>
%
% @doc a first attempt use messages in adbtree calls
%
% @end
% ------------------------------------------------


-module(adbtree_async).

-export([start/0, commands/0]).



start() ->
  PID = spawn(adbtree_async,commands,[]),
  register(adbtreeasync, PID).

commands() ->
  receive
    {connect, Sender_PID, DB} ->
      case adbtree:connect(DB) of
          {ok, Tree} ->
              Sender_PID ! ok;
          {error, enoent} ->
              Sender_PID ! db_does_not_exist
      end,
      commands();
    {create_db, Sender_PID, DB} ->
      Sender_PID ! adbtree:create_db(DB),
      commands();
    {save, Sender_PID, Tree, Key, Value} ->
      try
        {ok, NewKey} = adbtree:save(atom_to_list(Tree), list_to_binary(Value), list_to_integer(Key, 16)),
        Sender_PID ! integer_to_list(NewKey, 16)
      catch
        _Class:_Err ->
          Sender_PID ! not_found
      end,
      commands();
    {lookup, Sender_PID, Tree, Key} ->
      try
        {ok, _Version, Doc} = adbtree:lookup(atom_to_list(Tree), list_to_integer(Key, 16)),
        Sender_PID ! binary_to_list(Doc)
      catch
        _Class:_Err ->
          Sender_PID ! not_found
      end,
      commands();
    {delete, Sender_PID, Tree, Key} ->
      Sender_PID ! adbtree:delete(atom_to_list(Tree), list_to_integer(Key, 16)),
      commands();
    {update, Sender_PID, Tree, Key, Value} ->
        adbtree:update(atom_to_list(Tree), list_to_binary(Value), list_to_integer(Key, 16)),
        Sender_PID ! ok,
        commands();
    finished ->
      ok
  end.
