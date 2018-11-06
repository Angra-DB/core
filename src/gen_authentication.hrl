-define(LoggedIn, logged_in).
-define(LoggedOut, logged_out).
-define(AuthenticationDBName, "AuthenticationDataBase").
-define(ErrorUserAlreadyExists, user_already_exists).
-define(ErrorInvalidPasswordOrUsername, invalid_password_or_username).

-record(authentication_info, {username}).