<?php

class TimeSpan
{
	var $start_time;

	function __construct($start_now = true)
	{
		$this->start_time = 0;
		if ($start_now) $this->start();
	}

	function start()
	{
		$this->start_time = $this->getmicrotime();
	}

	function getTimeSpanMS($format_for_output = true)
	{
		$ret = ($this->getmicrotime() - $this->start_time)*1000;
		if ($format_for_output)
			$ret = sprintf("%0.2f", $ret);
		return $ret;
	}

	function getTimeSpanF()
	{
		$took = $this->getTimeSpanMS(false);
		if ($took >= 1000)
			$took = sprintf("%.1f", ((float)$took) / 1000.0)."s";
		else
			$took = sprintf("%.2fms", $took);

		return $took;
	}

	function getmicrotime()
	{
		list($usec, $sec) = explode(" ", microtime());
		return ((float)$usec + (float)$sec);
	}
}


class curl
{
	static function multi($urls, $custom_options = array(), $rolling_window = 20)
	{
		$ret = array();

		// make sure the rolling window isn't greater than the # of urls
		$rolling_window = min($rolling_window, count($urls));

		$master = curl_multi_init();
		$curl_arr = array();

		// add additional curl options here
		$std_options = array(CURLOPT_RETURNTRANSFER => true,
			CURLOPT_FOLLOWLOCATION => true,
			CURLOPT_MAXREDIRS => 5);
		$options = ($std_options + $custom_options);

		// start the first batch of requests
		$handle_map = array();
		for ($i = 0; $i < $rolling_window; $i++)
		{
			$ch = curl_init();
			$handle_map[] = $ch;
			$options[CURLOPT_URL] = $urls[$i];
			curl_setopt_array($ch, $options);
			curl_multi_add_handle($master, $ch);
		}

		do
		{
			while(($execrun = curl_multi_exec($master, $running)) == CURLM_CALL_MULTI_PERFORM);
			curl_multi_select($master);

			if($execrun != CURLM_OK)
				break;

			// a request was just completed -- find out which one
			while($done = curl_multi_info_read($master))
			{
				$h_ = $done['handle'];
				$info = curl_getinfo($h_);

				$output = false;
				if ($info['http_code'] == 200)
					$output = curl_multi_getcontent($done['handle']);

				foreach ($handle_map as $k=>$v)
				{
					if ($v == $h_)
						$ret[$k] = $output;
				}

				// start a new request (it's important to do this before removing the old one)
				if ($i<count($urls))
				{
					$ch = curl_init();
					$handle_map[] = $ch;
					$options[CURLOPT_URL] = $urls[$i++];  // increment i
					curl_setopt_array($ch, $options);
					curl_multi_add_handle($master, $ch);

					// not running YET... but we need to continue the loop
					$running = 1;
				}

				// remove the curl handle that just completed
				curl_multi_remove_handle($master, $done['handle']);
			}

		} while ($running > 0);

		curl_multi_close($master);
		ksort($ret);
		return $ret;
	}
}

class input
{
	static function get_req($param_name, $default = '')
	{
		return (isset($_REQUEST[$param_name]) ? $_REQUEST[$param_name] : $default);
	}

	static function random_string($length, $charset = 'qwertyuiopasdfghjklzxcvbnm1234567890!@#$%^&*()_}{"P:,')
	{
		$clen = strlen($charset) - 1;
		$ret = '';
		for ($i = 0; $i < $length; $i++)
			$ret .= $charset[rand(0, $clen)];

		return $ret;
	}

	static function url_assemble_($arr, $pos = array())
	{
		$ret = array();

		$pos_curr = $pos;
		foreach ($arr as $k => $v)
		{
			$pos = $pos_curr;
			$pos[] = $k;

			if (is_array($v))
			{
				$tmp = self::url_assemble_($v, $pos);
				foreach ($tmp as $k => $v)
					$ret[] = $v;
				continue;
			}

			$pos[] = $v;
			$ret[] = $pos;
		}

		return $ret;
	}
}

function print_rr($data)
{
	echo "<pre>";
	print_r($data);
	echo "</pre>";
}

function progressReport($prg)
{
	static $max = false;
	static $lastoutput = false;
	if ($max === false || $prg === false)
	{
		$max = $prg;
		return;
	}
	
	if ($max < 300)
		return;

	$progress = ( $prg * 1000 / $max );
	if ($progress - $lastoutput > 50.0)
	{
		$lastoutput = $progress;

		$progress = ( floor($progress/10) ).".".( $progress%10 ).'% ';
		fwrite(STDERR, $progress);
	}
	
	return $progress;
}
?>