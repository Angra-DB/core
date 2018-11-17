% declares a SERVER macro constant (?MODULE is the module's name)
-define(SERVER, ?MODULE).

% 'state' definition: a record for keeping the server state
% . persistence_setup: {PersistenceScheme, PersistenceSettings};
% . authentication_setup: {AuthenticationScheme, AuthenticationSettings};
% . authentication_status: {UserAuthenticationStatus, UserAuthenticationInfo#authentication_info};
% . authorization_setup: {AuthorizationScheme, AuthorizationSettings};
% . communication (if ssl options were set on "adb_core.app", the app will use SSL. If no ssl options are set, the app will use purely TCP): gen_tcp | ssl
-record(state, {lsock, persistence_setup, parent, current_db = none, authentication_setup, authentication_status, authorization_setup, communication}).