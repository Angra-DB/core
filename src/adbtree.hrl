-define(SizeOfPointer, 8).
-define(SizeOfHeader, 66).
-define(OrderSize, 2).
-define(KeySize, 8).
-define(Node, 0).
-define(Leaf, 1).

-record(dbsettings, {dbname, sizeinbytes, sizeversion}).

-record(btree, {order, root}).
-record(node, {keys, nodePointers}).
-record(leaf, {keys, docPointers, leafPointer}).