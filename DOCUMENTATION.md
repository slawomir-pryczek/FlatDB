#Making connections
To make a connection first you need to add server to pool,
you'll need to add servers at the very beginning of your code
because after making first connection in your script you won't be
able to alter server config, as it could lead to keys being assigned
to incorrect servers.
```
FlatDB::addServer('127.0.0.1', '7777');
$fdb = new FlatDB();
```

You can create as many FlatDB objects as you wish, they'll share single persistent
connection if you're using same IP and Port.


###Advanced connect, using server pools
You can also define server pools. The, idea is that
pool is a group of servers so you can have multiple
groups intended to be used for different purposes. **$weight** can be used to
assign more items, proportionally to a given server. If you won't specify **$group**
default group will be used.
 
_addServer( **$host**, **$port**, **$weight**=0, **$group**=false)_<br>__construct( $group = false)_

```
FlatDB::addServer('127.0.0.1', '7777');
FlatDB::addServer('192.168.10.1', '7777', 5, 'caching-cluster');
FlatDB::addServer('192.168.10.2', '7777', 5, 'caching-cluster');
FlatDB::addServer('192.168.10.3', '7777', 10, 'caching-cluster');
...
$fdb = new FlatDB();
$fdb_caching = new FlatDB('caching-cluster');
```
This creates 2 server groups, one is default, with single server, the second group - 
caching-cluster, consist of 3 servers, servers **.1** and **.2** will get 25% of items each,
the server **.3** will get 50%.

#Basic operations
These are operations designed to be compatible with standard memcached server. Parameter list in many cases,
is same as in original memcached client. One difference is that default key **TTL** is one hour, instead of infinite.
There's also no support for items that never expire, if **TTL** is greater than 10 years, we assume 
we're using timestamp instead.

##Creating items
Functions used to create and replace items.

_public function set(**$key**, **$var**, **$expire** = 3600, **$safe** = false)_

This function will create/overwrite item **$key** with value **$var**, and set its
TTL to **$expire**. If you need to be sure that the item was in fact set on the server
you can set **$safe** to true.

If you use the default (**$safe**=false) true will be returned if PHP was able to send
request to set the item to FlatDB server, which is much faster because it won't require 2-way communication
so items would be set almost like in bulk because TCP packets could be merged and we
won't need to wait for server reply


_public function add(**$key**, **$value**, **$expire** = 3600)_

This function will add the item to the server, it means it'll return true only if the item can be added
and it doesn't exist yet. If there is an item with specified **$key** already, it'll return false.
 
_public function replace(**$key**, **$var**, **$expire** = 3600)_

This function will replace already existing item, it'll return false if there's no item
named **$key** on the server, or there's too little memory to replace the item. It is added just for compatibilityt
with memcached server, as there are not many use cases for this operation.

##Item operations
Operations will modify key TTL and its value

public function touch(**$key**, **$expire** = 3600)
This function will change TTL of the item, it'll return TRUE if operation succeeded, meaning that item exists and has not expired
because, by design, changing TTL of existing item can't fail.

_public function increment(**$key**, **$value** = 1, **$expire** = false)_<br>
_public function decrement(**$key**, **$value** = 1, **$expire** = false)_

These functions will increment/decrement **$key** by **$value**, only integer values are supported.
You can also specify new TTL in **expire**. Will return FALSE if item doesn't exists, is not numeric, or if there's not
enough memory on server to increase item's value (very uncommon)

_public function delete(**$key**)_

This will simply delete the **$key** and will return TRUE if delete was successfull,
eg. if there's no **$key** in datastore, it'll return FALSE.

##Querying items
These functions will allow you to query item values and state. Please note that flatdb client doesn't contain serialization
features, so if you want to store, eg. false, you'll have to use json_encode / json_decode.

_public function exists(**$key**, **&$out_ttl** = false)_

This function will return true if item named **$key** exists, the item's TTL will be placed inside **$out_ttl**
if required.

_public function get(**$key**, **&$out_cas** = false, **&$out_expires** = false)_

This function will return value of **$key** or FALSE, you can also get CAS Token this way, and its TTL.

##Advanced operations
These operations are additional features available in FlatDB, that differs a lot from memcached version.

_public function getFirst(array **$keys**, **&$out_key** = false, **&$out_cas** = false, **&$out_expires** = false)_

Get value of first key from array **$keys**, or FALSE if nothing could be found. It'll only return ONE key, you can see which one
using **$out_key**. It'll also return it's CAS Token in **$out_cas** and TTL time in **$out_expires**

_public function getMulti(array **$keys**)_

Get values of all keys, or false... will return an array, each item will have its own row.
```
FlatDB::addServer('127.0.0.1', '7777');
$fdb = new FlatDB();
// add items
$fdb->add('i2', 'valueof 2', 100);
$fdb->add('i3', 'valueof 3', 100);
// get all
$val = $fdb->getMulti(['i1','i2','i3']);
print_r($val);
```

Output will be
```
Array
(
    [i1] => Array
        (
            [data] => false 
            [cas] => false
            [e] => false
        )

    [i2] => Array
        (
            [data] => valueof 2
            [cas] => 3
            [e] => 100
        )

    [i3] => Array
        (
            [data] => valueof 3
            [cas] => 4
            [e] => 100
        )

)
```

_public function atomicSet(**$key**, **$expire**, **$get_val_fn**, **&$out_new_cas** = false)_

This is modern way of implementing compare and swap, that guarantees that script is thread safe if you want to modify already
existing values. Basically you'll provide **$key** you'll work on, **$expire**, and **$get_val_fn** which is
modifying function, it'll get current item's value and in return it should give a new value or FALSE if the item 
needs to be deleted. It can be called multiple times to re-calculate calue if item's value on server change between GET and SET.<br>
It'll return new **$key**'s value or FALSE, and also you can get CAS Token assigned using **&$out_new_cas**

```
$x = $fdb->atomicSet("myitem", 10, function($val) {
    if ($val === false) // (A)
	    return "start-";
    if (strlen($val) > 123) // (B)
        return false;
        
    return "{$val}a"; // (C)
}, $new_cas);
```

This function when run in paralell in multiple threads 10 times, will result in myitem that'll contain: _start-aaaaaaaaa_

As for what this function does, it'll create item **(A)** that starts with 'start-' and for each subsequent run it'll add 'a' character 
to this item **(C)**, if the item size is larger that 123 it'll get deleted **(B)**.

_public function atomicAdvancedInsert(**$key**, **$value** = false, **$cas** = false, **$expire** = 3600)_

This is advanced way of doing **Compare And Swap**, which is much more difficult to use. Generally you won't have to use this, it's provided there just for documentation purposes
and if you'd like to do more advanced processing, eg. handle out-of-memory errors differently than _atomicSet_ which will just return false.

For every CAS operation there are 2 parts, first one is COMPARE<br>
**$key** is key we're working on<br>
**$cas** is CAS token of existing item or false if item doesn't exist, $expire is new expire time if operation succeeds.<br>
The **Compare** phase is successfull if one of 2 following conditions are meet<br>
C1. $cas is false, and there is no item named **$key** on the server<br> 
C2. $cas is not false, there is item named **$key** on the server and its CAS matches the **$cas** passed to function<br> 

The **Swap** phase is using these 2 parameters<br>
**$value** is new value to which we want to set this key to<br> 
**$expies** is new TTL if operation succeeds

Return value:<br> 
R1. If COMPARE phase **succeeds** and item value will be set to new one, in return you'll get [true, new_cas_token]<br> 
R2. If COMPARE phase **fails** and item we work on is already existing in server - you'll get [error_code, current_value, current_cas] so you can re-calculate its new value<br> 
R3. If COMPARE phase **fails** and item we work on doesn't exists on the server - you'll get [error_code, false, false], so you'll know that you need to use false for **$cas** to insert new item.  

Error codes:<br>
ADVI_INSERT_CAS_MISMATCH - cas is not matching<br>
ADVI_INSERT_OUT_OF_MEMORY - not enough memory to store new value<br>
ADVI_CONN_ERROR - can't connect / send data to server
