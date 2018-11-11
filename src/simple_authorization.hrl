% permission levels for each command
-define(OwnerPermission, 3).
-define(ReadAndWritePermission, 2).
-define(ReadPermission, 1).
-define(NoPermission, 0).

% if a command is not listed in CommandsAndPermissions, it is automatically considered a permission-free command (NoPermission)
% (for example: "connect", "create_db", "login", "logout", "register)
-define(CommandsAndPermissions,
  [
    % owner-permission commands
    {"delete_db", ?OwnerPermission},
    {"grant_permission", ?OwnerPermission},
    {"revoke_permission", ?OwnerPermission},
    {"show_permission", ?OwnerPermission},

    % read-write-permission commands
    {"save", ?ReadAndWritePermission},
    {"save_key", ?ReadAndWritePermission},
    {"update", ?ReadAndWritePermission},
    {"delete", ?ReadAndWritePermission},

    % read-permission commands
    {"lookup", ?ReadPermission},
    {"query_term", ?ReadPermission}
  ]
).
