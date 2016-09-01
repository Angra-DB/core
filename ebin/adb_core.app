{application, adb_core,
[{description, "The angra-db core service."},
 {vsn, "0.01"},
 {modules, [adb_app, adb_sup, adb_server]},
 {registered, [adb_sup]},
 {applications, [kernel, stdlib]},
 {mod, {adb_app, []}}
]}.
