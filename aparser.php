<?php 

error_reporting(E_ALL | E_STRICT);
date_default_timezone_set('Europe/Minsk');

require_once 'aparser-api-php-client.php';

class Aparser_Worker
{
	const RESULTS_DIR = 'results';
	const TASKS_IDS_FILENAME = 'tasks/tasks_ids.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'
	const CONFIG_FILE_FILENAME = 'config.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'

	protected $_api_server = 'http://127.0.0.1:9092/API';
	protected $_upload_server = 'base.parser.by';
	protected $_username = 'base';
	protected $_key_public = 'keys/public';
	protected $_key_private = 'keys/private';

	protected $_file_lock = NULL;
	protected $_config = array();

	public function __construct(){}
	
	protected static function arr_get($array, $key, $default = NULL)
	{
		if (!is_array($array))
		{
			$array = array();
		}
		return isset($array[$key]) ? $array[$key] : $default;
	}
	
	protected static function clear_arr($array)
	{
		if (!is_array($array))
		{
			$array = array();
		}
		$array = array_map('trim', $array);
		return array_diff($array, array('', NULL));
	}

	protected function set_lock($lockname)
	{
		$this->_file_lock = fopen('locks/' . $lockname . '.lock', 'w');
		if (!flock($this->_file_lock, LOCK_EX|LOCK_NB))
		{
			die('Already in action');
		}
	}
	
	protected function load_config()
	{
		$config_array = explode("\n", file_get_contents(self::CONFIG_FILE_FILENAME));
		foreach ($config_array as $config_str)
		{
			$config_str_exp = explode('=', $config_str, 2);
			$this->_config[trim(self::arr_get($config_str_exp, 0))] = trim(self::arr_get($config_str_exp, 1));
		}
	}

	protected function get_config($key, $default = NULL)
	{
		return self::arr_get($this->_config, $key, $default);
	}

	protected function open_and_lock_file($filename)
	{
		$file = fopen($filename, 'r+');
		flock($file, LOCK_EX);
		$filesize = filesize($filename);
		if (!$filesize)
		{
			return FALSE;
		}
		return $file;
	}
	
	protected function rewrite_and_close_file($file, $data)
	{
		fseek($file, 0);
		ftruncate($file, 0);
		fwrite($file, $data);
		fclose($file);
	}
	
	protected function send_request($params)
	{
		$request = json_encode($params);
		$ch = curl_init($this->_api_server);

		curl_setopt($ch, CURLOPT_POST, 1);
		curl_setopt($ch, CURLOPT_POSTFIELDS, $request);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Length: ' . strlen($request)));
		curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: text/plain; charset=UTF-8'));

		$response = curl_exec($ch);
		curl_close($ch);

		if (!$response)
		{
			return FALSE;
		}
		$response = json_decode($response, true);
		if (!self::arr_get($response, 'success'))
		{
			return FALSE;
		}
		return $response;
	}

	public function set_task()
	{
		$this->set_lock('set_task');
		$this->load_config();

		$aparser = new Aparser($this->_api_server, '', array('debug'=>'true'));
		$aparser->ping();
		$aparser->info();
		$task_id = $aparser->addTask(
				$this->get_config('config_preset'),
				$this->get_config('task_preset'),
				$this->get_config('query_from'),
				$this->get_config('task_query')
		);

		$tasks_file = fopen(self::TASKS_IDS_FILENAME, "a+");
		flock($tasks_file, LOCK_EX);
		fwrite($tasks_file, $task_id . "\n");
		fclose($tasks_file);
	}

	public function get_results()
	{
		$this->set_lock('get_results');
		$this->load_config();

		$tasks_file = $this->open_and_lock_file(self::TASKS_IDS_FILENAME);
		if (!$tasks_file)
		{
			return FALSE;
		}

		$tasks_file_content =  fread($tasks_file, filesize(self::TASKS_IDS_FILENAME));

		$tasks_ids = explode("\n", $tasks_file_content);
		$tasks_ids = self::clear_arr($tasks_ids);

		foreach ($tasks_ids as $key => $task_id)
		{
			$state = $this->send_request(array(
				'action' => 'getTaskState',
				'data' => array (
					'parser' => 'SE::Google',
					'preset' => 'Pages Count use Proxy',
					'taskUid' => $task_id //947
					),
				'password' => ''
			));

			if (!$state or !isset($state['data']['status']) or $state['data']['status'] != 'completed')
			{
				continue;
			}

			$result = $this->send_request(array(
					'action' => 'getTaskResultsFile',
					'data' => array (
						'parser' => 'SE::Google',
						'preset' => 'Pages Count use Proxy',
						'taskUid' => $task_id //947
						),
					'password' => ''
			));

			$results_file = self::RESULTS_DIR . '/task_' . $task_id . '.xml';
			file_put_contents($results_file, file_get_contents(self::arr_get($result, 'data')));
			unset($tasks_ids[$key]);
		}

		$this->rewrite_and_close_file($tasks_file, implode("\n", $tasks_ids));
	}

	public function upload_files($files = array())
	{
		$this->set_lock('upload_files');
		$files = array_diff(scandir(self::RESULTS_DIR), array('.', '..'));

		$connection = ssh2_connect($this->_upload_server, 22);
		if (!$connection)
		{
			die('Connection not established');
		}

		if (!ssh2_auth_pubkey_file(
			$connection,
			$this->_username,
			$this->_key_public,
			$this->_key_private
		))
		{
			die('Authentication failed');
		}
		foreach ($files as $file)
		{
			ssh2_scp_send(
					$connection, 
					self::RESULTS_DIR . '/' . $file, 
					'/srv/base/app/upload/import/xml/person/[' .$this->_country_code. ']_[' . $this->_source . ']_' . $file, 
					0777);
			unlink(self::RESULTS_DIR . '/' . $file);
		}
	}
}

if (!isset($argv[1]))
{
	die('Action is not specified');
}

$akafi_worker = new Aparser_Worker();
$method_name = $argv[1];
if (!method_exists($akafi_worker, $method_name))
{
	die('Action is not implemented');
}

$akafi_worker->$method_name();