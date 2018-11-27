# AngraDB
   
## Run

To start a standalone AngraDB node, execute the following command:

```
$ rebar3 shell --apps adb_core
```

> Tip: The flag `--apps` inform which apps should be started on `shell` command.

To start a AngraDB node and enter in a cluster, execute the following command passing, as a environment variable, the node's name from one the members in the 
cluster:

```
$ env ADB_ACCESS='node_name@host_name' rebar3 shell --adb_core
```

## Tests

To run tests, execute the following command:

```
$ rebar3 ct
```

> To specify the tests, this application used the [Common Tests' Framework](http://erlang.org/doc/apps/common_test/users_guide.html).

### Coverage

To enable coverage analysis, its necessary to add a flag to the `ct` command: `rebar3 ct --cover`.

And to see coverage results, you can execute the command `rebar3 conver --verbose`.
After executing it, a page with the results will be available on the `_build/test/cover` folder.

## Configuration

The AngraDB configuration are made on `adb_core.app.src` file. 

The following table will show the possible options available to be set as a 
environment variable, this configuration is used just as a default value:

| Env. Variable | Description                                           |
|:-------------:|:------------------------------------------------------|
| ADB_PORT      | Default port on which the adb_server will listen      |
| ADB_HOST      | Default hostname on which the node will be identified |

And other configurations are on this following table, this options will be
replicated across the cluster following the oldest node on the cluster:

| App. Config.    | Description                                            |
|:---------------:|:-------------------------------------------------------|
| persistence     | Persistency strategy used  (ets, hanoidb or adbtree)   |
| max_index_size  | Maximum size of the memory index in bytes              |
| distribution    | Distribution mode (dist or mono)                       |
| partition       | Partition strategy used (full or consistent)           |
| replication     | Number of copies on replication of data                |
| write_quorum    | Number of successful writes required on each operation |
| read_quorum     | Number of successful read required  on each operation  |
| vnodes          | Number of VNodes                                       |
| gossip_interval | Gossip Interval (milliseconds)                         |


## Documentation

To generate the project's documentation, you have to run the following command:

```
$ rebar3 edoc
```

> This application uses [edown](https://github.com/esl/edown) library to generate
a documentation written in markdown.
