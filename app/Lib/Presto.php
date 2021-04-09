<?php

declare(strict_types=1);

namespace App\Lib;

use Throwable;
use RuntimeException;
use GuzzleHttp\Client;
use GuzzleHttp\Middleware;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\MessageFormatter;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use Hyperf\Utils\Coroutine;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Logger\LoggerFactory;

use Hyperf\Guzzle\PoolHandler;
use Hyperf\Guzzle\RetryMiddleware;

class Presto
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

    protected $retries = 3;

    public function __construct(array $config = [], ?LoggerInterface $logger = null, ?ClientInterface $client = null)
    {
        if (null === $logger) {
            $this->logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('presto', 'default');
        }

        if (null === $client) {
            $this->httpClient = $this->createHttpClient();
        }

        if (empty($config)) {
            $config = ApplicationContext::getContainer()->get(ConfigInterface::class)->get('presto', []);
        }

        if (empty($config) || !isset($config['user'], $config['server'], $config['catalog'], $config['schema'])) {
            $this->logger->error('presto 连接参数错误', [$config]);
            throw new RuntimeException('Presto connection config is required.');
        }

        if (!is_string($config['user']) || !is_string($config['server'])
            || !is_string($config['catalog']) || !is_string($config['schema'])
        ) {
            $this->logger->error('presto 连接参数数据类型错误', [$config]);
            throw new RuntimeException('Invalid presto connection config.');
        }

        $this->config = $config;
        $this->retries = max(min(intval($config['retries'] ?? 3), 0), 20);

        $this->httpHeaders = [
            self::HEADER_USER => $config['user'],
            self::HEADER_SCHEMA => $config['schema'],
            self::HEADER_CATALOG => $config['catalog'],
        ];

        if (1 === preg_match('#^https?://#i', $config['server'])) {
            $this->url = trim($config['server'], '/') . self::ENDPOINT;
        } else {
            $this->url = 'http://' . trim($config['server'], '/') . self::ENDPOINT;
        }
    }

    private function createHttpClient(): ClientInterface
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
        if ('dev' === env('APP_ENV')) {
            $stack->push(Middleware::log($this->logger, new MessageFormatter(MessageFormatter::DEBUG), 'DEBUG'));
        }

        $stack->push(Middleware::retry(function(
            $retries,
            RequestInterface $req,
            ResponseInterface $resp = null,
            $exception = null
        ) {
            if ($resp) {
                $statusCode = $resp->getStatusCode();
                if (200 === $statusCode) {
                    return false;
                }

                // If the client request returns an HTTP 503, that means the server was busy,
                // and the client should try again in 50-100 milliseconds.
                // Any HTTP status other than 503 or 200 means that the query has failed.
                // v.a. https://prestodb.io/docs/current/develop/client-protocol.html#overview-of-query-processing
                if (503 === $statusCode && $retries < $this->retries) {
                    return true;
                }

                $this->logger->error('presto 请求失败', [
                    'response' => (string)$resp->getBody(),
                ]);
            } else {
                if ($execption) {
                    $this->logger->error('presto 请求异常', [
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

    protected function executeParams(array $params): string
    {
        $vals = [];
        foreach ($params as $key => $val) {
            if (is_string($val)) {
                // presto 的字符串只能用 单引号，而单引号内的单引号使用2个单引号作为转义
                $val = sprintf("'%s'", strtr($val, ["'" => "''"]));
            } elseif (is_bool($val)) {
                $val = $val ? 'true' : 'false';
            }

            $vals[] = $val;
        }

        return join(',', $vals);
    }

    public function query(string $sql, ...$params)
    {
        if (empty($params)) {
            // 普通查询
            $result = $this->request($sql);
        } else {
            // prepare 查询
            $executeParams = $this->executeParams($params);
            $statementName = 'stmt_' . md5($sql . $executeParams);
            $result = $this->request("EXECUTE {$statementName} USING {$executeParams}", [
                self::HEADER_PREPARED_STATEMENT => $statementName . '=' . urlencode($sql),
            ]);
        }

        $results = [$result];
        while ($result && isset($result['nextUri'])) {
            $result = $this->nextUri($result['nextUri']);
            $results[] = $result;
        }

        if ($result) {
            return $this->mergeResult($results);
        } else {
            if (sizeof($results) > 1) {
                // 最后一个 $result 为 false，记录之前的响应并返回 false
                $this->logger->error('presto 响应中断', $results);
            }

            return false;
        }
    }

    protected function mergeResult(array $results): array
    {
        $datas = [];
        $stats = [];
        $errors = [];
        $columns = [];
        $warnings = [];

        foreach ($results as $result) {
            if ($result['stats']['state'] === self::QUERY_STATE_FINISHED) {
                // 如果查询有结束，只记录最后的状态
                $stats = $result['stats'];
            } else {
                $stats[] = $result['stats'];
            }

            if (!empty($result['warnings'])) {
                $warnings[] = $result['warnings'];
            }

            if (!empty($result['error'])) {
                $errors[] = $result['error'];
            }

            if (!empty($result['columns'])) {
                if (empty($columns)) {
                    $columns = $result['columns'];
                }
            }

            if (empty($result['data'])) {
                continue;
            }

            foreach ($result['data'] as $row) {
                $data = [];
                foreach ($row as $key => $val) {
                    $column = $columns[$key] ?? null;
                    if (null === $column) {
                        throw new RuntimeException('Unexpected presto response.');
                    }

                    $data[$column['name']] = $val;
                }

                $datas[] = $data;
            }
        }

        if ($errors) {
            $this->logger->error('presto 请求出现错误信息', [$errors]);
        }

        if ($warnings) {
            $this->logger->error('presto 请求出现警告信息', [$warnings]);
        }

        if ($stats) {
            $this->logger->debug('presto status', [$stats]);
        }

        return $datas;
    }

    protected function request(string $data, array $headers = [], string $url = '')
    {
        try {
            if ($url !== '') {
                $resp = $this->httpClient->request('GET', $url);
            } else {
                $resp = $this->httpClient->request('POST', $this->url, [
                    'body' => $data,
                    'headers' => array_merge($headers, $this->httpHeaders),
                ]);
            }

            $body = (string)$resp->getBody();
            $statusCode = $resp->getStatusCode();
            if (200 === $statusCode) {
                return $this->parseResponse($body);
            } else {
                $this->logger->error('presto 请求返回非预期响应', [
                    'response' => $body,
                    'statusCode' => $statusCode,
                ]);
                return false;
            }
        } catch (Throwable $t) {
            $this->logger->error('presto 请求异常', [$t->getMessage()]);
            return false;
        }
    }

    protected function nextUri(string $nextUri)
    {
        return $this->request('', [], $nextUri);
    }

    protected function parseResponse(string $rawResp)
    {
        $data = @json_decode($rawResp, true);
        if (null === $data) {
            $this->logger->error('非预期响应', [$rawResp]);
            return false;
        }

        if (!isset($data['id'], $data['infoUri'], $data['stats'], $data['stats']['state'], $data['warnings'])) {
            // 非预期响应
            return false;
        }

        return $data;
    }
}
