<?php

namespace FlatDB;
use FlatDB\FlatDB;

class FlatDBCluster 
{
	/**
	 * @var FlatDB[]
	 */
	private static $fdb = [];
	private static $fdb_r = [];
	private static $master_last_try = [];
	private $group = false;
	
	public $last_error = false;
	
	static public function addServers($servers, $replicas = [], $group = false)
	{
		$group_replica = "{$group}/replica";
		if ($group === false)
			$group_replica = "replica";
				
		if (!is_array($servers))
			$servers = [$servers];
		if (!is_array($replicas))
			$replicas = [$replicas];
		
		foreach ($servers as $v)
			FlatDB::addServers($v, $group);
		foreach ($replicas as $v)
			FlatDB::addServers($v, $group_replica);
		return true;
	}

	function __construct($group = false)
	{
		$this->group = $group;
		if (isset(self::$fdb[$group]))
			return;

		self::$fdb[$group] = new FlatDB($group);
		self::$master_last_try[$group] = false;
	}

	private function getNode()
	{
		$group = $this->group;
		$fdb = self::$fdb[$group];
		
		if ($fdb === false)
		{
			$this->last_error = "Connection error";
			if (self::$master_last_try[$group] === false)
				self::$master_last_try[$group] = time();
			else
			{
				// try reconnect to master after 10 seconds!
				if (time() - self::$master_last_try[$group] > 10)
				{
					self::$master_last_try[$group] = false;
					self::$fdb[$this->group] = new FlatDB($this->group);
				}
			}
		}
		
		return $fdb;
	}
	
	private function getReplica()
	{
		$group = $this->group;
		if ($group === false)
			$group = "replica";
		else 
			$group = "{$group}/replica";
		
		if (isset(self::$fdb_r[$group]))
			return self::$fdb_r[$group];

		self::$fdb_r[$group] = new FlatDB($group);
		return self::$fdb_r[$group];
	}
	
	public function setOption( $option, $value )
	{
		return true;
	}

	public function setOptions( $options )
	{
		return true;
	}

	public function add($key, $value, $expire = 3600)
	{
		$ret = false;
		$fdb = $this->getNode();
		
		if ($fdb !== false)
		{
			$ret = $fdb->add($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}
		
		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->add($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}
		
		return $ret;
	}

	public function replace($key, $var, $expire = 3600)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->replace($key, $var, $expire);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->replace($key, $var, $expire);
			$this->last_error = $fdb->last_error;
		}
			
		return $ret;
	}

	public function set($key, $var, $expire = 3600, $safe = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->set($key, $var, $expire, $safe);
			$this->last_error = $fdb->last_error;
		}
			
		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;
			
			$ret = $fdb->set($key, $var, $expire, $safe);
			$this->last_error = $fdb->last_error;
		}
		
		return $ret;
	}

	public function touch($key, $expire = 3600)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->touch($key, $expire);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->touch($key, $expire);
			$this->last_error = $fdb->last_error;
		}
		
		return $ret;
	}

	public function exists($key, &$out_ttl = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->exists($key, $out_ttl);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->exists($key, $out_ttl);
			$this->last_error = $fdb->last_error;
		}
		
		return $ret;
	}

	public function increment($key, $value = 1, $expire = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->increment($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->increment($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function decrement($key, $value = 1, $expire = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->decrement($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->decrement($key, $value, $expire);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function delete($key)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->delete($key);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->delete($key);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function get($key, &$out_cas = false, &$out_expires = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->get($key, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->get($key, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function getFirst(array $keys, &$out_key = false, &$out_cas = false, &$out_expires = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->getFirst($keys, $out_key, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->getFirst($keys, $out_key, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function getMulti(array $keys)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->getMulti($keys);
			$this->last_error = $fdb->last_error;
		}

		if ($fdb === false || $fdb->last_error === "Connection error")
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->getMulti($keys);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function getMultiSimple(array $keys)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->getMultiSimple($keys);
			$this->last_error = $fdb->last_error;
		}

		if ($fdb === false || $fdb->last_error === "Connection error")
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->getMultiSimple($keys);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	const ADVI_INSERT_CAS_MISMATCH = FlatDB::ADVI_INSERT_CAS_MISMATCH;
	const ADVI_INSERT_OUT_OF_MEMORY = FlatDB::ADVI_INSERT_OUT_OF_MEMORY;
	const ADVI_CONN_ERROR = FlatDB::ADVI_CONN_ERROR;
	public function atomicAdvancedInsert($key, $value = false, $cas = false, $expire = 3600)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->atomicAdvancedInsert($key, $value, $cas, $expire);
			$this->last_error = $fdb->last_error;
		}

		if ($fdb === false || $fdb->last_error === "Connection error")
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return [FlatDB::ADVI_CONN_ERROR];

			$ret = $fdb->atomicAdvancedInsert($key, $value, $cas, $expire);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	public function atomicSet($key, $expire, $get_val_fn, &$out_new_cas = false)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->atomicSet($key, $expire, $get_val_fn, $out_new_cas);
			$this->last_error = $fdb->last_error;
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
				return false;

			$ret = $fdb->atomicSet($key, $expire, $get_val_fn, $out_new_cas);
			$this->last_error = $fdb->last_error;
		}

		return $ret;
	}

	private $cacheShouldRegenerate = false;
	public function cacheGet($key, $refresh_time, &$out_cas, &$out_expires)
	{
		$ret = false;
		$fdb = $this->getNode();

		if ($fdb !== false)
		{
			$ret = $fdb->cacheGet($key, $refresh_time, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
			$this->cacheShouldRegenerate = $fdb->cacheShouldRefresh();
		}

		if ($ret === false && ($fdb === false || $fdb->last_error === "Connection error"))
		{
			$this->cacheShouldRegenerate = false;
			self::$fdb[$this->group] = false;
			$fdb = $this->getReplica();
			if ($fdb === false)
			{
				$out_cas = false;
				$out_expires = false;
				return false;
			}	

			$ret = $fdb->cacheGet($key, $refresh_time, $out_cas, $out_expires);
			$this->last_error = $fdb->last_error;
			$this->cacheShouldRegenerate = $fdb->cacheShouldRefresh();
		}

		return $ret;
	}

	public function cacheShouldRefresh()
	{
		return $this->cacheShouldRegenerate;
	}

	public function runMaintenance($operation, $other_parameters = [], $run_on_replica = false)
	{
		$fdb = self::$fdb[$this->group];
		if ($run_on_replica)
			$fdb = $this->getReplica();

		if ($fdb === false)
		{
			$this->last_error = "Connection error";
			return false;
		}
		
		$this->last_error = $fdb->last_error;
		return $fdb->runMaintenance($operation, $other_parameters);
	}

	function getClientN($num = false)
	{
		$fdb = self::$fdb[$this->group];
		if ($fdb === false)
		{
			$this->last_error = "Connection error";
			return false;
		}

		return $fdb->getClientN($num);
	}

	public function getClient($key, $id_only = false)
	{
		$fdb = self::$fdb[$this->group];
		if ($fdb === false)
		{
			$this->last_error = "Connection error";
			return false;
		}
		
		$fdb->getClient($key, $id_only);
	}
}

?>