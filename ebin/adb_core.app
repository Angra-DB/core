{application, adb_core,
[{description, "The angra-db core service."},
 {vsn, "0.01"},
 {modules, [adb_app, adb_sup, adb_server]},
 {registered, [adb_sup]},
 {applications, [kernel, stdlib, lager]},
 {mod, {adb_app, 
        % {persistence, {{name, ets | hanoidb | adbtree }, [Args]}}
        % {auth, {{name, adb_authentication | (soon, there will be others...) }, [Args (none until then]}}
        [
          {persistence, {{name, adbtree}, [{max_index_size, 50000000}]}},
          {authentication, {{name, adb_cert_authentication}, []}},
          {authorization, {{name, simple_authorization}, []}},
          {ssl, [{certfile, "./test_certificates/server_cert.pem"}, {keyfile, "./test_certificates/server_key.pem"}, {cacertfile, "./test_certificates/ca_cert.pem"}]}
        ]
       }
 }
]}.
