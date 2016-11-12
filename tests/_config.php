<?php

include_once '../../autoload.php';
use FlatDB\FlatDB;

include_once __DIR__.'/_inc.common.php';

define("HOST", "192.168.10.1");
define("PORT", "7777");
define("VERBOSE", false);

define("MAX_OPS", 40);

function getFDB() 
{
	FlatDB::addServer(HOST, PORT);
	return new FlatDB();
}
