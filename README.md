# FlatDB


FlatDB is modern memcached replacement, which is designed to be ideal for dynamic web applications.

Firstly it's written from scratch, taking modern hardware specifications into account, it's simple, has no legacy code such as ascii protocol support and has many features which are not supported or buggy in original memcached server.

It was created mainly to deal with limitations of memcached LRU algorithm and lack of properly working memcached extension for PHP7. It's based on [HSServer](https://github.com/slawomir-pryczek/HSServer) which is generic server implementation you can use to easily expose services written in golang over network.

# Requirements
PHP7 for the client, golang 1.7 for compiling server, 32 bit compilation is **not supported** and the server compiled for 32 bit enviroments will likely segfault because it won't be able to allocate continuous blocks of memory. 

## Main advantages
- Server is written using 100% golang, can be easily compiled on serveral operating system
- Native PHP Client, there's no extension required, you can just include everything required in your project manually or by using composer
- Support for TCP and UDP protocols. UDP can be used to send non-important and diagnostic data into server
- Supports HTTP for all operations, and comprehensive server status / statistics
- Automatic, optimal slab cleanup and rebalance, items are moved between slabs to optimize memory usage
- No LRU algorithm, items in slabs are pre-scanned so there is no problem with garbage occupying memory when item's time to live differs, you can keep items that have 2s and 2days timeout and all will be properly collected
- Compression settings that can be defined for IP ranges, you can compress items sent using WAN network, skip compression for localhost and limit compression for private network items.
- Tested in production, with billions of items and terabytes of processed data
- Keepalive and nodelay connections by default, allowing for much faster communication, especially on loopback interfaces on modern linux distributions that use high MTU
- Ability to save data snapshot to disk, using MVCC mechanism
- Real flush operation (items are deleted and slabs freed)
- New modern approach to atomic operations, insert-or-replace-with-CAS using PHP callbacks prevent data from being damaged during multithread modification
- Ability to get item TTL, increment / decrement operations which can, optionally set a new TTL
- Get first key from set operation, will return first available item from set of keys
- Peek operation, see which items are available without sending data
- Support for large items, and variable size SLABs
