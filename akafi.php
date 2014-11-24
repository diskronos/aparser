<?php 

error_reporting(E_ALL | E_STRICT);
date_default_timezone_set('Europe/Minsk');

require_once 'aparser-api-php-client.php';

class Akafi
{
	const RESULTS_DIR = 'results';
	const TASKS_FILENAME = 'tasks.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'
	const TASKS_IDS_FILENAME = 'tasks/tasks_ids.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'

	protected $_api_server = 'http://127.0.0.1:9092/API';
	protected $_upload_server = 'base.parser.by';
	protected $_username = 'base';
	protected $_key_public = 'keys/public';
	protected $_key_private = 'keys/private';
	
	protected $_country_code = 'SA';
	protected $_source = 'aparser-A';
	
	protected $_file_lock = NULL;
	
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

	public function set_task($count = 1000)
	{
		$this->set_lock('set_task');

		$tasks_file = $this->open_and_lock_file(self::TASKS_FILENAME);
		if (!$tasks_file)
		{
			return FALSE;
		}

		$akafi_tasks_ids = fread($tasks_file, filesize(self::TASKS_FILENAME));
		$akafi_tasks_ids = explode("\n", $akafi_tasks_ids);
		$akafi_tasks_ids = self::clear_arr($akafi_tasks_ids);

		rsort($akafi_tasks_ids, SORT_NUMERIC);
		$min_id_key  = (count($akafi_tasks_ids) > 1000 ? 999 : end($akafi_tasks_ids));

		$min_id = $akafi_tasks_ids[$min_id_key];
		$max_id = $akafi_tasks_ids[0];

		$aparser = new Aparser($this->_api_server, '', array('debug'=>'true'));
		$aparser->ping();
		$aparser->info();
		$task_id = $aparser->addTask('default', 'akafi.net', 'text', 'https://www.akafi.net/akfnew/Sub-{num:'.$min_id.':'.$max_id.'}.html');
		
		$akafi_tasks_ids = array_slice($akafi_tasks_ids, $min_id_key);
		$this->rewrite_and_close_file($tasks_file, implode("\n", $akafi_tasks_ids));

		$tasks_file = fopen(self::TASKS_IDS_FILENAME, "a+");
		flock($tasks_file, LOCK_EX);
		fwrite($tasks_file, $task_id . "\n");
		fclose($tasks_file);
	}

	public function get_results()
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

		foreach ($tasks_ids as $key => $task_id)
		{
			$request = json_encode(array(
				'action' => 'getTaskResultsFile',
				'data' => array (
					'parser' => 'SE::Google',
					'preset' => 'Pages Count use Proxy',
					'taskUid' => $task_id //947
					),
				'password' => ''
			));

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
				continue;
				return FALSE;
			}
			$response = json_decode($response, true);
			if (!self::arr_get($response, 'success'))
			{
				continue;
			}
			$results_file = self::RESULTS_DIR . '/task_' . $task_id . '.xml';
			file_put_contents($results_file, file_get_contents(self::arr_get($response, 'data')));
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

$akafi_worker = new Akafi();
$method_name = $argv[1];
if (!method_exists($akafi_worker, $method_name))
{
	die('Action is not implemented');
}

$akafi_worker->$method_name();