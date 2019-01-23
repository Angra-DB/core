{application, adb_core,
[{description, "The angra-db core service."},
 {vsn, "0.01"},
 {modules, [adb_app, adb_sup, adb_server]},
 {registered, [adb_sup]},
 {applications, [kernel, stdlib, lager]},
 {mod, {adb_app, 
        % {persistence, {{name, ets | hanoidb | adbtree }, [Args]}
        [{persistence, {{name, adbtree}, [{max_index_size, 5000000}]}}]
       }
 }
]}.
