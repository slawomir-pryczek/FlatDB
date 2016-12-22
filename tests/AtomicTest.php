<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestAtomic extends TestCase
{
	public function testCallbackReplace()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "atomictest|{$pid}";
		
		$fdb->delete($key);
		$fdb->add($key, 0, 1);
		
		progressReport(MAX_OPS);
		for ($i = 0; $i < MAX_OPS; $i++)
		{
			$prev = false;
			$curr = false;
			$fdb->atomicSet($key, 10, function($val) use(&$prev, &$curr) {
				$prev = $val;
				$curr =  "{$val}|0";
				return $curr;
			}, $new_cas_token);
			
			$this->assertEquals(strlen($prev)+2, strlen($curr));
			$cas_compare = false;
			$curr_from_server = $fdb->get($key, $cas_compare);

			if ($cas_compare === $new_cas_token)
			{
				// single threaded, check the value is properly set!
				$this->assertEquals($curr, $curr_from_server);
			}
			
			progressReport($i);
		}
		
		progressReport(false);
	}

	// ...
}