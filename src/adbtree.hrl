-define(SizeOfPointer, 8).
-define(SizeOfHeader, 66).
-define(OrderSize, 2).
-define(KeySize, 8).
-define(Node, 0).
-define(Leaf, 1).
-define(SizeOfSize, 8).

-record(dbsettings, {dbname, sizeinbytes, sizeversion}).

-record(btree, {order, curNode}).
-record(node, {keys, nodePointers}).
-record(leaf, {keys, docPointers, leafPointer, versions}).