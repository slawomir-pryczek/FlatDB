<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestAdd extends TestCase
{
	public function testAddANDDelete()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$key = "addtest|{$pid}";
		
		$fdb->delete($key);
		$fdb->add($key, 0, 5);
		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$this->assertEquals($fdb->touch($key, 5), true);
			$this->assertEquals($fdb->delete($key), true);
			$this->assertEquals($fdb->delete($key), false);
			$this->assertEquals($fdb->add($key, 0, 5), true);
			$this->assertEquals($fdb->add($key, 0, 5), false);
			$this->assertEquals($fdb->add($key, 5), false);
			$this->assertEquals($fdb->increment($key, 1), 1);
			$this->assertEquals($fdb->increment($key, 1), 2);
			$this->assertEquals($fdb->get($key), 2);
			$this->assertEquals($fdb->increment($key, 0), 2);
			progressReport($i);
		}

		progressReport(false);
	}

	public function testDecrement()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$key = "dectest|{$pid}";

		$fdb->delete($key);
		$fdb->add($key, 0, 5);
		progressReport(MAX_OPS);
		for ($i=1; $i<MAX_OPS; $i++)
		{
			$this->assertEquals($fdb->decrement($key, 10, 5), -10*$i);
			$this->assertEquals($fdb->decrement($key, 0, 5), -10*$i);
			$this->assertEquals($fdb->increment($key, 0, 5), -10*$i);
			progressReport($i);
		}

		progressReport(false);
	}

	// ...
}