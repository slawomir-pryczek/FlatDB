<?php

include_once '../../autoload.php';
use FlatDB\FlatDB;
use FlatDB\FlatDBCluster;

include_once __DIR__.'/_inc.common.php';

$host1 = '192.168.10.1:7777';
$host2 = '192.168.10.1:6777';

define("HOST", "192.168.10.1");
define("PORT", "7777");
define("VERBOSE", false);

define("FDB_MASTER", [$host1, $host2]);
define("FDB_SLAVE", [$host2, $host1]);

define("MAX_OPS", 40);

function getFDB() 
{
	if (getenv('cluster'))
		return getFDBCluster();
	
	FlatDB::addServers(HOST.":".PORT);
	$fdb = new FlatDB();
	
	$test_str = 'aaa12345678-498342798fsdj9fsj9';
	$is_ok = ($fdb->getClientN(0)[0])->sendData(["action"=>"echo","data"=>$test_str]);
	$is_ok = stripos($is_ok, $test_str);
	if ($is_ok === false)
		return false;
	
	return $fdb;
}

function getFDBCluster()
{
	static $added = false;
	if (!$added)
	{
		FlatDBCluster::addServers(FDB_MASTER, []);
		$added = true;
	}
	
	$fdb = new FlatDBCluster();

	$test_str = 'aaa12345678-498342798fsdj9fsj9';
	$is_ok = ($fdb->getClientN(0)[0])->sendData(["action"=>"echo","data"=>$test_str]);
	$is_ok = stripos($is_ok, $test_str);

	$is_ok2 = ($fdb->getClientN(1)[0])->sendData(["action"=>"echo","data"=>$test_str]);
	$is_ok2 = stripos($is_ok2, $test_str);
	
	if ($is_ok === false || $is_ok2 === false)
		return false;

	return $fdb;
}

function getFDBClusterReplicated()
{
	static $added = false;
	if (!$added)
	{
		FlatDBCluster::addServers(FDB_MASTER, FDB_SLAVE);
		$added = true;
	}
	
	return new FlatDBCluster();
}

function get1($k)
{
	static $req = false;
	static $host = false;
	if ($host === false)
	{
		$fdb = getFDBClusterReplicated();
		$req = ['action'=>'mc-get'];
		$host = $fdb->getClientN(0)[0];
	}

	$req['k'] = $k;
	$a = ($host->sendData($req));
	if ($a === "")
		$a = false;
	return $a;
}
function get2($k)
{
	static $req = false;
	static $host = false;
	if ($host === false)
	{
		$fdb = getFDBClusterReplicated();
		$req = ['action'=>'mc-get'];
		$host = $fdb->getClientN(1)[0];
	}

	$req['k'] = $k;
	$a = ($host->sendData($req));
	if ($a === "")
		$a = false;
	return $a;
}