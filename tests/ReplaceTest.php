<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;
use FlatDB\FlatDB;

class TestReplace extends TestCase
{
	public function testReplace()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$key = "repl-test-XX|{$pid}|";

		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$k = $key.rand(1,99);
			$v = rand(1,999999);
			
			$fdb->set($k, $v, 10);
			$cas_token = false;
			$vv = $fdb->get($k, $cas_token);
			$this->assertEquals($v, $vv);
			
			$fdb->replace($k, $v+1, 10);
			$this->assertEquals($v+1, $fdb->get($k));
			
			// testing advanced replace with CAS, assert CAS changed because of replace, and also value is unchanged because of invalid CAS
			$repl_ret = ($fdb->atomicAdvancedInsert($k, "e{$v}", $cas_token, 10));
			assert($repl_ret[0] === FlatDB::ADVI_INSERT_CAS_MISMATCH);
			$this->assertEquals($v+1, $fdb->get($k));
			$cas_token = $repl_ret[1];
			
			// is cas token returned ok?
			$fdb->get($k, $__cas);
			$this->assertEquals($cas_token, $__cas);
			
			// do the same thing with correct CAS, assert that key value was changed, new cas assigned, and correct value is there
			$repl_ret = ($fdb->atomicAdvancedInsert($k, "e{$v}", $cas_token, 10));
			assert($repl_ret[0] === true);
			$this->assertNotEquals($repl_ret[1], $cas_token);
			$this->assertEquals("e{$v}", $fdb->get($k));
			
			progressReport($i);
		}

		progressReport(false);
	}
	

	// ...
}