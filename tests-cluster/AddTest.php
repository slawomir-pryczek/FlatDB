<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestAdd extends TestCase
{
	public function testAddANDDelete()
	{
		$fdb = getFDBClusterReplicated();
		if ($fdb === false)
		{
			$this->markTestSkipped('Cannot get cluster, please make sure both flatdb servers are running');
			return;
		}
		
		progressReport(MAX_OPS*20);
		$data_set = [];
		$op_info = [];
		for ($i = 0; $i < MAX_OPS*20; $i++)
		{
			$pid = getmypid()."//".time();
			$key = "ClX/R/addtest|{$pid}|{$i}";

			$fdb->delete($key);
			$fdb->add($key, 0, 5);
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
			$this->assertEquals($fdb->decrement($key, 3), -1);
			$this->assertEquals($fdb->increment($key, $i+1), $i);
			$this->assertEquals($fdb->get($key), $i);
			$fdb->touch($key, 120);
			
			$data_set[$key] = $i;
			
			$key_old = "ClX/R/addtest|{$pid}|".($i-150);

			$op_info[$key][] = "norm";
			if (rand(1, 10) == 1 && $i > 150)
			{
				$op_info[$key_old][] = "del";
				$data_set[$key_old] = false;
				$fdb->delete($key_old);
			}	
			if (rand(1, 10) == 1 && $i > 150)
			{
				$op_info[$key_old][] = "touch";
				$data_set[$key_old] = false;
				$fdb->touch($key_old, 0);
			}	
			
			progressReport($i);
		}

		progressReport(false);
		
		sleep(1);
		
		foreach ($data_set as $k=>$v)
		{
			$gg = $fdb->get($k);
			if ($v != $gg)
				fwrite(STDERR, "{$v} != {$gg} / op: ".implode(", ", $op_info[$key]));
			$this->assertEquals($v, $gg);

			$a = get1($k);
			$b = get2($k);
			$this->assertEquals($v, $a);
			$this->assertEquals($v, $b);
		}
	}
	
	// ...
}