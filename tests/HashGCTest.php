<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestHashGC extends TestCase
{
	public function testHashGC()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "gc-test-|{$pid}|";
		
		$fdb->set("{$key}|set", 1000, 100);

		// increment test
		$fdb->set("{$key}|inc", 999, 100);
		$fdb->increment("{$key}|inc", 1);

		// replace test
		$fdb->set("{$key}|repl", 999, 100);
		$fdb->replace("{$key}|repl", 1000);

		// atomic set
		$fdb->atomicSet("{$key}|atom", 100, function($prev) {
			return 1000;
		});

		$fdb->runMaintenance('hash-gc');
		$fdb->set("{$key}|set", '1001', 1);
		$fdb->increment("{$key}|inc", 1, 1);
		$fdb->replace("{$key}|repl", '1001', 1);
		$fdb->atomicSet("{$key}|atom", 1, function($prev) {
			return 1001;
		});


		progressReport(50);
		$set = $rep = $inc = $atom = [];
		for ($i=0; $i<50; $i++)
		{
			progressReport($i+1);
			$set[] = $_s = $fdb->get("{$key}|set");
			$inc[] = $_i = $fdb->get("{$key}|inc");
			$rep[] = $_r = $fdb->get("{$key}|repl");
			$atom[] = $_a = $fdb->get("{$key}|atom");
			$fdb->runMaintenance('hash-gc');
			usleep(100000);

			if ($_s === false)
				$fdb->set("{$key}|set", 1001);
			if ($_i === false)	// make sure we can't increment on expired key
				$fdb->increment("{$key}|inc", 1234);
			if ($_r === false) // make sure we can't replace expired key
				$fdb->replace("{$key}|repl", 1234);
			if ($_a === false)
			{
				$fdb->atomicSet("{$key}|atom", 1, function($prev) {
					return 1000;
				});
				$fdb->atomicSet("{$key}|atom", 1, function($prev) {
					return 1001;
				});
			}
		}
		progressReport(false);

		$set = array_values(array_unique($set));
		$inc = array_values(array_unique($inc));
		$rep = array_values(array_unique($rep));
		$atom = array_values(array_unique($atom));
		
		$this->assertEquals(2, count($set));
		$this->assertEquals(2, count($inc));
		$this->assertEquals(2, count($rep));
		$this->assertEquals(2, count($atom));

		$this->assertEquals(json_encode([0=>"1001", 1=>false]), json_encode($set));
		$this->assertEquals(json_encode([0=>"1001", 1=>false]), json_encode($inc));
		$this->assertEquals(json_encode([0=>"1001", 1=>false]), json_encode($rep));
		$this->assertEquals(json_encode([0=>"1001", 1=>false]), json_encode($atom));
	}

	// ...
}