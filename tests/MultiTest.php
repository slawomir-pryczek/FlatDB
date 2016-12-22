<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestMulti extends TestCase
{
	public function testGetMulti()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "testmul|{$pid}";

		for ($ops = 0; $ops<MAX_OPS; $ops++)
		{
			$keys_set = [];
			$keys_all = [];
			for ($i = 0; $i < 10; $i++)
			{
				$fdb->delete("{$key}{$i}");
				if (rand(1, 10) >= 7)
				{
					$fdb->add("{$key}{$i}", $i * 10, 5);
					$keys_set["{$key}{$i}"] = $i * 10;
				}
				else
					$keys_set["{$key}{$i}"] = false;
				$keys_all[] = "{$key}{$i}";
			}

			if (count($keys_set) == 0)
				continue;
			
			foreach ($fdb->getMulti($keys_all) as $key=>$val)
				$this->assertEquals($val['data'], $keys_set[$key]);
		}
	}

	public function testGetFirst()
	{
		$fdb = getFDB();
		if ($fdb === false)
		{
			$this->markTestSkipped('Flatdb not running');
			return;
		}
		
		$pid = getmypid();
		$key = "testmul|{$pid}|";

		for ($ops = 0; $ops<MAX_OPS; $ops++)
		{
			$keys_set = [];
			$keys_all = [];
			for ($i = 0; $i < 10; $i++)
			{
				$fdb->delete("{$key}{$i}");
				if (rand(1, 10) >= 7)
				{
					$fdb->add("{$key}{$i}", $i * 10, 5);
					$keys_set["{$key}{$i}"] = $i * 10;
				}
				else
					$keys_set["{$key}{$i}"] = false;
				$keys_all[] = "{$key}{$i}";
			}

			if (count($keys_set) == 0)
				continue;

			$out_val = $fdb->getFirst($keys_all, $key_name);
			foreach ($keys_set as $k=>$v)
			{
				if ($v === false)
					continue;
				$this->assertEquals($k, $key_name);
				$this->assertEquals($v, $out_val);
				break;
			}
				
		}
	}

	// ...
}