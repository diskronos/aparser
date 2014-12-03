<?php 

error_reporting(E_ALL | E_STRICT);
date_default_timezone_set('Europe/Minsk');

require_once 'aparser-api-php-client.php';

class Aparser_Worker
{
	const RESULTS_DIR = 'results';
	const TASKS_IDS_FILENAME = 'tasks/tasks_ids.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'
	const CONFIG_FILE_FILENAME = 'config.xml'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'
	const DELIMITER = '|';

	protected $_api_server = 'http://127.0.0.1:9092/API';
	protected $_upload_server = 'base.parser.by';
	protected $_username = 'base';
	protected $_key_public = 'keys/public';
	protected $_key_private = 'keys/private';

	protected $_file_lock = NULL;
	protected $_config = array();

	public function __construct(){}
	
	public static function arr_get($array, $key, $default = NULL)
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
	
	protected function load_config($task_name = NULL)
	{
		$config_loaded = FALSE;

		$dom_doc = new DOMDocument();
		$dom_doc->loadXML(file_get_contents(self::CONFIG_FILE_FILENAME));

		foreach ($dom_doc->getElementsByTagName('task') as $task)
		{
			if ($task->getAttribute('name') == $task_name)
			{
				$config_loaded = TRUE;
				break;
			}
		}
		if (!$config_loaded)
		{
			die('Task name not found');
		}
		foreach ($task->childNodes as $params)
		{
			$config_key = trim($params->localName);
			$config_value = trim($params->nodeValue);
			
			if ($config_key)
			{
				$this->_config[$config_key] = $config_value;
			}
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

	public function set_task($params = array())
	{
		$task_name = self::arr_get($params, 0);
		
		$save_needed = self::arr_get($params, 1, 'yes');
		$save_needed = (mb_strtolower($save_needed) == 'yes');

		if (is_null($task_name))
		{
			die('Task not specified');
		}

		$this->set_lock('set_task');
		$this->load_config($task_name);

		$aparser = new Aparser($this->_api_server, '', array('debug'=>'true'));
		$aparser->ping();
		$aparser->info();
		$task_id = $aparser->addTask(
				$this->get_config('config_preset'),
				$this->get_config('task_preset'),
				$this->get_config('query_from'),
				$this->get_config('task_query')
		);

		if ($save_needed)
		{
			$tasks_file = fopen(self::TASKS_IDS_FILENAME, "a+");
			flock($tasks_file, LOCK_EX);
			$task_update = 
					$task_id . self::DELIMITER . 
					$task_name . self::DELIMITER .
					$this->get_config('default_country_code') . self::DELIMITER . 
					$this->get_config('source') . "\n";
			fwrite($tasks_file, $task_update);
			fclose($tasks_file);
		}
	}

	public function get_results($params = array())
	{
		$this->set_lock('get_results');

		$tasks_file = $this->open_and_lock_file(self::TASKS_IDS_FILENAME);
		if (!$tasks_file)
		{
			return FALSE;
		}

		$tasks_file_content =  fread($tasks_file, filesize(self::TASKS_IDS_FILENAME));

		$tasks_ids = explode("\n", $tasks_file_content);
		$tasks_ids = self::clear_arr($tasks_ids);

		foreach ($tasks_ids as $key => $task_params)
		{
			$task_params_arr = explode(self::DELIMITER, $task_params);

			$task_id = self::arr_get($task_params_arr, 0);
			$task_name = self::arr_get($task_params_arr, 1);
			$default_country_code = self::arr_get($task_params_arr, 2);
			$source = self::arr_get($task_params_arr, 3);

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

			$results_file = self::RESULTS_DIR . '/[' . $default_country_code . ']_[' . $source . ']_' . $task_name . '_' . date('d_m_Y') . '.xml';
			file_put_contents($results_file, file_get_contents(self::arr_get($result, 'data')));
			unset($tasks_ids[$key]);
		}

		$this->rewrite_and_close_file($tasks_file, implode("\n", $tasks_ids) . "\n");
	}

	public function upload_files($params = array())
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
					'/srv/base/app/upload/import/xml/person/' . $file, 
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

unset($argv[0]);
unset($argv[1]);

if (!method_exists($akafi_worker, $method_name))
{
	die('Action is not implemented');
}

$akafi_worker->$method_name(array_merge($argv));