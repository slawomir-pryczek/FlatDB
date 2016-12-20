#Replication support
FlatDB supports both master-master and master-slave replication, which is lazy and eventualy consistent. Key stored on master is not guaranteed
to be stored on replica after the writing function returns. Instead, the replication works by dividing time into 100ms windows and sending aggregated, compressed changes
from this period to the slave, then replying it.

This mode of replication plays very well with sharding. To improve performance of the cluster you can create more nodes and use them as shards.
To have high-availablity you can use one of three replication models described later.

##Simple setup, just sharding
Servers are divided into groups, if you add >1 server into single group it'll form a sharded cluster. Keys will be distributed according to weight (10/5). 
You can define many groups. Sharded or not.
```php
FlatDB::addServer("192.168.10.1", "7777", 10, [group]);
FlatDB::addServer("192.168.10.2", "7777", 5, [group]);
...
$fdb = new FlatDB([group]);
```

Same thing can be acomplished using FlatDBCluster, you can leave **replicas** empty to define cluster without replication.
```php
FlatDB::addServers(["192.168.10.1:7777:10", "192.168.10.2:7777:5"], [replicas], [group]);
...
$fdb = new FlatDB([group]);
```

##Configuring slave
To configure slave you'll need to edit master's conf.json to add "SLAVE":"127.0.0.1:7777,..." you can define more than one slave(s) by specifying more than one servers and 
using comma as separator. You can also specify replication mode, using "REPLICATION_MODE":"SAFE", there are 2 modes available:
 * OPTIMISTIC (default) - will minimize network usage eg. by sending touch as touch commands
 * SAFE - will also send item's content alongside for operations that don't require it (eg. touch), if your slave is low on memory this can help keeping data in better shape

###Replication modes


