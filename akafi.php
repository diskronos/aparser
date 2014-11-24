<?php 

error_reporting(E_ALL | E_STRICT);
date_default_timezone_set('Europe/Minsk');

require_once 'aparser-api-php-client.php';

class Akafi
{
	const RESULTS_DIR = 'results';
	const TASKS_FILENAME = 'tasks.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'
	const TASKS_IDS_FILENAME = 'tasks/tasks_ids.txt'; //'C:\Users\kirill\Desktop\Aparser1.1.89Beta\results\akaf\akafid.txt'

//	protected $_api_server = 'http://127.0.0.1:9092/API';
	protected $_api_server = 'http://176.9.30.153:9092/API';
	protected $_upload_server = 'base.parser.by';
	protected $_username = 'base';
	protected $_key_public = 'keys/public';
	protected $_key_private = 'keys/private';
	
	protected $_file_lock = NULL;
	
	public function __construct(){}
	
	protected function set_lock($lockname)
	{
		$this->_file_lock = fopen('locks/' . $lockname . '.lock', 'w');
		if (!flock($this->_file_lock, LOCK_EX|LOCK_NB))
		{
			die('Already in action');
		}
	}
	
	public function set_task($count = 1000)
	{
		$this->set_lock('set_task');

		$file = file(self::TASKS_FILENAME);
		rsort($file, SORT_NUMERIC);
		$idmax = trim($file[0]);
		$idmin = $idmax - $count;
		$aparser = new Aparser($this->_api_server, '', array('debug'=>'true'));
		$aparser->ping();
		$aparser->info();
		$task_id = $aparser->addTask('default', 'akafi.net', 'text', 'https://www.akafi.net/akfnew/Sub-{num:'.$idmin.':'.$idmax.'}.html');
		$tasks_file = fopen(self::TASKS_IDS_FILENAME, "a+");
		flock($tasks_file, LOCK_EX);
		fwrite($tasks_file, $task_id . "\n");
		fclose($tasks_file);
	}

	public function get_results()
	{
		$this->set_lock('get_results');

		$tasks_file = fopen(self::TASKS_IDS_FILENAME, "r+");
		flock($tasks_file, LOCK_EX);

		$tasks_ids = explode("\n", fread($tasks_file, filesize(self::TASKS_IDS_FILENAME)));

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
				return FALSE;
			}
			$results_file = self::RESULTS_DIR . '/task_' . $task_id . '.xml';
			$response = json_decode($response, true);
			file_put_contents($results_file, $response['data']);
			unset($tasks_ids[$key]);
		}

		fseek($tasks_file, 0);
		ftruncate($tasks_file, filesize(self::TASKS_IDS_FILENAME));
		fwrite($tasks_file, implode("\n", $tasks_ids) . 'test');
		fclose($tasks_file);
	}

	public function upload_files($files = array())
	{
		$source = 'aparser';
		$country_code = 'SA';

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
					'/srv/base/test/[' .$country_code. ']_[' . $source . ']_' . $file, 
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