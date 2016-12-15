<?php
include_once __DIR__.'/_config.php';

use PHPUnit\Framework\TestCase;

class TestCache extends TestCase
{
	public function testCacheOk()
	{
		$fdb = getFDB();
		$key = "cachetest-".getmypid()."|".rand(1,999)."|";
		$fdb->delete($key);
		$fdb->delete("{$key}|2");

		$fdb->add($key, 'v', 4);
		$start = time();
		
		$should_refresh = 0;
		$out = [];
		$cas_1 = [];
		$cas_2 = [];
		while (time() - $start <= 7)
		{
			$fdb->cacheGet($key, 2, $cas, $e);
			if ($fdb->cacheShouldRefresh())
				$should_refresh++;
			if ($cas !== false)
				$cas_1[$cas] = true;

			$val = $fdb->cacheGet("{$key}|2", 2, $cas, $e);
			if ($val === false || $fdb->cacheShouldRefresh())
			{
				$new = max(1, $val)*2;
				$fdb->set("{$key}|2", $new, 4);
				$out[$new] = true;
			}

			if ($cas !== false)
				$cas_2[$cas] = true;
		}
		
		$this->assertEquals(1, count($cas_1));
		$this->assertEquals(3, count($cas_2));
		$this->assertEquals(3, count($out));

		$out = array_keys($out);
		$this->assertEquals(2, $out[0]);
		$this->assertEquals(4, $out[1]);
		$this->assertEquals(8, $out[2]);

		$this->assertEquals(8, $fdb->get("{$key}|2"));
		
		/*var_dump($should_refresh);
		print_r($out);
		print_r($cas_1);
		print_r($cas_2);*/
	}

	// ...
}