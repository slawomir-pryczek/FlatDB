<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestSet extends TestCase
{
	public function testSet()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "settest-XX|{$pid}|";

		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$k = $key.rand(1,99);
			$v = rand(1,999999);
			$fdb->set($k, $v, 10);
			$this->assertEquals($fdb->add($k, $v, 10), false);
			$this->assertEquals($fdb->delete($k), true);
			if (rand(1, 10) > 5)
				$this->assertEquals($fdb->delete($k), false);
			progressReport($i);
		}

		progressReport(false);
	}

	public function testSet2()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "settest-XX|{$pid}|";

		$k = $key;
		$v = 10;
		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$fdb->set($k, $v, 10);
			if (rand(1, 10) > 5)
				$this->assertEquals($fdb->add($k, $v, 10), false);
			$this->assertEquals($fdb->delete($k), true);
			if (rand(1, 10) > 5)
				$this->assertEquals($fdb->delete($k), false);
			progressReport($i);
		}

		progressReport(false);
	}

	// ...
}