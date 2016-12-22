<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestReplicationMulti extends TestCase
{
	public function testReplication()
	{
		$fdb = getFDBClusterReplicated();
		$key = getmypid().'//'.time();
		$key = "rtest/p/{$key}|";
		
		$datax = [];
		$ops = [];

		fwrite(STDERR, "writing: ");
		progressReport(20000);
		for ($i=0; $i<20000; $i++)
		{
			$r = rand(1,100);
			if ($r > 50)
				$fdb->add("{$key}|{$i}", $i-100, 3600);
			$r = rand(1,100);
			if ($r > 70)
				$fdb->set("{$key}|{$i}", $i+100, 3600);
			$r = rand(1,100);
			if ($r > 80)
				$fdb->add("{$key}|{$i}", $i+500, 3600);

			$r = rand(1,100);
			if ($r > 80)
				$fdb->delete("{$key}|{$i}");

			$r = rand(1,100);
			if ($r > 90) {
				$ii = $i - 100;
				$fdb->touch("{$key}|{$ii}", 2);
			}

			$r = rand(1,100);
			if ($r > 40)
				$fdb->increment("{$key}|{$i}", 20);
			$r = rand(1,100);
			if ($r > 80)
				$fdb->decrement("{$key}|{$i}", 260);

			$r = rand(1,100);
			if ($r > 90)
				$fdb->replace("{$key}|{$i}", $i+811);

			$r = rand(1,100);
			if ($r > 90) {
				$fdb->atomicSet("{$key}|{$i}", 3600, function ($curr) {
					return "xxxx";
				});
			}
			$r = rand(1,100);
			if ($r > 90) {
				$fdb->atomicSet("{$key}|{$i}", 3600, function ($curr) use ($i) {
					return $i + 99;
				});
			}

			$r = rand(1,100);
			if ($r > 90) {
				$fdb->touch("{$key}|{$i}", 2);
			}

			progressReport($i);
		}
		progressReport(false);
		
		sleep(1);
		fwrite(STDERR, "\nchecking: ");
		progressReport(20000);

		$req = ['action'=>'mc-get'];
		for ($i=0; $i<20000; $i++)
		{
			$kk = "{$key}|{$i}";
			$a = get1($kk);
			$b = get2($kk);
			if ($a != $b)
				fwrite(STDERR, "mismatched key {$kk}: {$a} vs {$b}");
			$this->assertEquals($a, $b);
			progressReport($i);
		}
		progressReport(false);
		
	}
	
	
	
}