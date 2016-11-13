<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestExists extends TestCase
{
	public function testExists()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$key = "existstest|{$pid}|";

		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$k = $key.rand(1,999);
			$v = rand(1,999999);
			$fdb->set($k, $v, 10);
			$this->assertEquals($fdb->exists($k), true);
			$this->assertEquals($fdb->exists($k."|".rand(1,999)), false);
			progressReport($i);
		}

		progressReport(false);
	}

	public function testExistsTTL()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$key = "existstest|{$pid}|";

		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$k = $key.rand(1,999);
			$v = rand(1,999999);
			$fdb->set($k, $v, 10);
			$fdb->exists($k, $ttl);
			$this->assertLessThan($ttl, 8);

			$fdb->exists($k."|".rand(1,999), $ttl);
			$this->assertEquals($ttl, false);

			progressReport($i);
		}

		progressReport(false);
	}

	// ...
}