<?php

declare(strict_types=1);

namespace App\Lib;

use Aws\Athena\AthenaClient;
use Aws\Credentials\Credentials;
use phpDocumentor\Reflection\Types\False_;
use Throwable;
use RuntimeException;
use GuzzleHttp\Client;
use GuzzleHttp\Middleware;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\MessageFormatter;

use Psr\Log\LoggerInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use Hyperf\Utils\Coroutine;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Logger\LoggerFactory;

use Hyperf\Guzzle\PoolHandler;
use Hyperf\Guzzle\RetryMiddleware;

class Athena
{
    const ENDPOINT = '/v1/statement';

    const HEADER_USER = 'X-Presto-User';
    const HEADER_SOURCE = 'X-Presto-Source';
    const HEADER_CATALOG = 'X-Presto-Catalog';
    const HEADER_SCHEMA = 'X-Presto-Schema';
    const HEADER_TIME_ZONE = 'X-Presto-Time-Zone';
    const HEADER_LANGUAGE = 'X-Presto-Language';
    const HEADER_TRACE_TOKEN = 'X-Presto-Trace-Token';
    const HEADER_SESSION = 'X-Presto-Session';
    const HEADER_SET_CATALOG = 'X-Presto-Set-Catalog';
    const HEADER_SET_SCHEMA = 'X-Presto-Set-Schema';
    const HEADER_SET_SESSION = 'X-Presto-Set-Session';
    const HEADER_CLEAR_SESSION = 'X-Presto-Clear-Session';
    const HEADER_SET_ROLE = 'X-Presto-Set-Role';
    const HEADER_ROLE = 'X-Presto-Role';
    const HEADER_PREPARED_STATEMENT = 'X-Presto-Prepared-Statement';
    const HEADER_ADDED_PREPARE = 'X-Presto-Added-Prepare';
    const HEADER_DEALLOCATED_PREPARE = 'X-Presto-Deallocated-Prepare';
    const HEADER_TRANSACTION_ID = 'X-Presto-Transaction-Id';
    const HEADER_STARTED_TRANSACTION_ID = 'X-Presto-Started-Transaction-Id';
    const HEADER_CLEAR_TRANSACTION_ID = 'X-Presto-Clear-Transaction-Id';
    const HEADER_CLIENT_INFO = 'X-Presto-Client-Info';
    const HEADER_CLIENT_TAGS = 'X-Presto-Client-Tags';
    const HEADER_RESOURCE_ESTIMATE = 'X-Presto-Resource-Estimate';
    const HEADER_EXTRA_CREDENTIAL = 'X-Presto-Extra-Credential';
    const HEADER_SESSION_FUNCTION = 'X-Presto-Session-Function';
    const HEADER_ADDED_SESSION_FUNCTION = 'X-Presto-Added-Session-Functions';
    const HEADER_REMOVED_SESSION_FUNCTION = 'X-Presto-Removed-Session-Function';

    const HEADER_CURRENT_STATE = 'X-Presto-Current-State';
    const HEADER_MAX_WAIT = 'X-Presto-Max-Wait';
    const HEADER_MAX_SIZE = 'X-Presto-Max-Size';
    const HEADER_TASK_INSTANCE_ID = 'X-Presto-Task-Instance-Id';
    const HEADER_PAGE_TOKEN = 'X-Presto-Page-Sequence-Id';
    const HEADER_PAGE_NEXT_TOKEN = 'X-Presto-Page-End-Sequence-Id';
    const HEADER_BUFFER_COMPLETE = 'X-Presto-Buffer-Complete';

    /** Query has been accepted and is awaiting execution. */
    const QUERY_STATE_QUEUED = 'QUEUED';

    /** Query is waiting for the required resources (beta).  */
    const QUERY_STATE_WAITING_FOR_RESOURCES = 'WAITING_FOR_RESOURCES';

    /** Query is being dispatched to a coordinator.  */
    const QUERY_STATE_DISPATCHING = 'DISPATCHING';

    /** Query is being planned.  */
    const QUERY_STATE_PLANNING = 'PLANNING';

    /** Query execution is being started.  */
    const QUERY_STATE_STARTING = 'STARTING';

    /** Query has at least one running task.  */
    const QUERY_STATE_RUNNING = 'RUNNING';

    /** Query is finishing (e.g. commit for autocommit queries) */
    const QUERY_STATE_FINISHING = 'FINISHING';

    /** Query has finished executing and all output has been consumed.  */
    const QUERY_STATE_FINISHED = 'FINISHED';

    /** Query execution failed.  */
    const QUERY_STATE_FAILED = 'FAILED';

    protected $url = '';

    protected $config = [];

    protected $logger = null;

    protected $httpClient = null;

    protected $httpHeaders = [];

    protected static $connections = [];

    protected static $connectionKeys = [];

    public static function getConnection(array $config, ?LoggerInterface $logger = null, ?ClientInterface $client = null)
    {
        if (null === $logger) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('presto', 'default');
        }

        // client 如果为 null，会自动实例化一个，因为这个实例化对象没有 set 到 DI 中
        // 所以每次实例化都会返回一个新对象，为避免出现这种情况，这里取 key 不应考虑默认实例化的 client
        $key = self::getConnectionsKey($config, $logger, $client);
        if (!isset(self::$connections[$key])) {
            $key2 = -1;
            if (null === $client) {
                $retries = max(min(intval($config['retries'] ?? 3), 0), 20);
                $client = self::createHttpClient($retries, $logger, $config['debug'] ?? false);
                $key2 = self::getConnectionsKey($config, $logger, $client);
            }

            if ($key2 !== -1) {
                if (!isset(self::$connections[$key2])) {
                    self::$connections[$key2] = new self($config, $logger, $client);
                }

                self::$connections[$key] = self::$connections[$key2];
            } else {
                self::$connections[$key] = new self($config, $logger, $client);
            }
        }

        return self::$connections[$key];
    }

    protected static function getConnectionsKey(array $config, LoggerInterface $logger, ?ClientInterface $client = null): int
    {
        foreach (self::$connectionKeys as $key => $val) {
            if ($config === $val[0] && $logger === $val[1] && $client === $val[2]) {
                return $key;
            }
        }

        return array_push(self::$connectionKeys, [$config, $logger, $client]) - 1;
    }

    private static function createHttpClient(int $maxRetries, LoggerInterface $logger, bool $debug): ClientInterface
    {
        $handler = null;
        if (Coroutine::inCoroutine()) {
            $handler = make(PoolHandler::class, [
                'option' => [
                    'max_connections' => 50,
                ],
            ]);
        }

        $stack = HandlerStack::create($handler);

        // 开发模式下，记录请求详情
        if ($debug || 'dev' === env('APP_ENV')) {
            $stack->push(Middleware::log($logger, new MessageFormatter(MessageFormatter::DEBUG), 'DEBUG'));
        }

        $stack->push(Middleware::retry(function(
            $retries,
            RequestInterface $req,
            ResponseInterface $resp = null,
            $exception = null
        ) use (&$logger, $maxRetries) {
            if ($resp) {
                $statusCode = $resp->getStatusCode();
                if (200 === $statusCode) {
                    return false;
                }

                // If the client request returns an HTTP 503, that means the server was busy,
                // and the client should try again in 50-100 milliseconds.
                // Any HTTP status other than 503 or 200 means that the query has failed.
                // v.a. https://prestodb.io/docs/current/develop/client-protocol.html#overview-of-query-processing
                if (503 === $statusCode && $retries < $maxRetries) {
                    return true;
                }

                $logger->error('anthea 请求失败', [
                    'response' => (string)$resp->getBody(),
                ]);
            } else {
                if ($exception) {
                    $logger->error('anthea 请求异常', [
                        'exception' => $exception instanceof Throwable ? $exception->getMessage() : $exception
                    ]);
                }
            }

            return false;
        }, function() {
            return rand(50, 100);
        }), 'retry');

        return make(Client::class, [
            'config' => [
                'handler' => $stack,
            ],
        ]);
    }


    /**
     * 转义字符串内的特殊字符
     * presto 的字符串规定只能使用 单引号，转义只需要将字符串内的 单引号 转义为 2个单引号 即可
     *
     * @param string $val
     * @return string
     */
    public static function escape(string $val): string
    {
        return strtr($val, ["'" => "''"]);
    }

    public static function bindValue($val): string
    {
        if (is_string($val)) {
            // presto 的字符串只能用 单引号，而单引号内的单引号使用2个单引号作为转义
            return sprintf("'%s'", self::escape($val));
        } elseif (is_bool($val)) {
            return $val ? 'true' : 'false';
        } elseif (is_array($val)) {
            $in = [];
            // todo presto 还支持 array,map 等 数据类型，但我们一般不会这样查，真遇到的时候添加支持
            if (array_filter($val, 'is_string') === $val) {
                foreach ($val as $v) {
                    $in[] = self::bindValue((string)$v);
                }
            } elseif (array_filter($val, 'is_bool') === $val) {
                foreach ($val as $v) {
                    $in[] = self::bindValue((bool)$v);
                }
            } elseif (array_filter($val, 'is_numeric') === $val) {
                $in = $val;
            }

            return $in ? join(',', $in) : '';
        } elseif (is_numeric($val)) {
            return (string)$val;
        }

        return '';
    }

    private function __construct(array $config, LoggerInterface $logger, ClientInterface $client)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->httpClient = $client;

    }

    public function query(string $sql)
    {
        $credentials = new Credentials($this->config['athena_access_key'], $this->config['athena_secret_key']);

        $options = [
            'credentials'   => $credentials,
            'region'        => $this->config['athena_region'],
            'version'       => $this->config['athena_version'],
        ];
//        $sql = 'SELECT * FROM (SELECT row_number() over() AS rn, * FROM ods.ods_dataark_b_user_admin WHERE rn BETWEEN 1 AND 2';
//        $sql = "SELECT * FROM (
//    SELECT row_number() over() AS rn, * FROM ($sql) as t)
//WHERE rn BETWEEN 1 AND 100;";
        $param_Query = [

            'QueryString' => $sql, // REQUIRED
            'ResultConfiguration' => [
                'EncryptionConfiguration' => [
                    'EncryptionOption' => $this->config['athena_encryption_option'], // REQUIRED
//                    'KmsKey' => '<string>',
                ],
                'OutputLocation' =>$this->config['athena_output_location'],
            ],
//            'WorkGroup' => '<string>',
        ];
        try{
            $athenaClient = new AthenaClient($options);
            $result           = $athenaClient->startQueryExecution($param_Query);
            $QueryExecutionId = $result['QueryExecutionId'];
            $i = 1;
            do{
                usleep(50000);//休眠一小段时间等待执行成功
                $result1 = $athenaClient->getQueryExecution([
                    'QueryExecutionId' => $QueryExecutionId, // REQUIRED
                ]);
                $status = $result1['QueryExecution']['Status']['State'];
                $i++;
            }while($status == "QUEUED" or $status == "RUNNING");
            if ($status == 'SUCCEEDED'){
                $result = $athenaClient->getQueryResults([
                    'QueryExecutionId' => $QueryExecutionId, // REQUIRED
                ]);
            }else{
                $this->logger->error('athena sql执行未成功，status:', $status);
                return false;
            }
            $data   = $result->toArray();
            $lists  = array();
            $i = 0;
            if (!empty($data['ResultSet']) && !empty($data['ResultSet']['Rows']) && count($data['ResultSet']['Rows']) > 1){

                $fields = $data['ResultSet']['Rows'][0]['Data'];
                unset($data['ResultSet']['Rows'][0]);
                foreach ($data['ResultSet']['Rows'] as $key => $datum){
                    foreach ($fields as $k=> $value){
                        $lists[$i][$value['VarCharValue']] = $datum['Data'][$k]['VarCharValue'];

                    }
                    $i++;
                }

            }
            return $lists;
        }catch (\Exception $exception){
            $this->logger->error('athena 异常出错了', $exception->getMessage());
            return false;
        }
    }


}
