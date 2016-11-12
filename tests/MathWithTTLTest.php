<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestCAS extends TestCase
{
	public function testCallbackReplace()
	{
		$fdb = getFDB();
		$pid = getmypid();
		$usekey = "atomictest|{$pid}|";

		$fdb->delete("{$usekey}1");
		$fdb->delete("{$usekey}2");
		$fdb->delete("{$usekey}3");

		$fdb->add("{$usekey}1", 0, 1);
		$fdb->add("{$usekey}2", 0, 1);
		$fdb->add("{$usekey}3", 0, 1);

		for ($i = 0; $i < 20; $i++)
		{
			($fdb->increment("{$usekey}1", 1));
			($fdb->increment("{$usekey}2", 1, 1));
			($fdb->increment("{$usekey}3", 1));
			$fdb->touch("{$usekey}1", 1);
			usleep(100000);

			$a = $fdb->get("{$usekey}1");
			$b = $fdb->get("{$usekey}2");
			$c = $fdb->get("{$usekey}3");
		}

		$this->assertEquals($a, 20);
		$this->assertEquals($b, 20);
		$this->assertEquals($c, false);
	}

	// ...
}