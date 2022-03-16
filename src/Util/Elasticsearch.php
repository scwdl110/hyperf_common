<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Elasticsearch\Client;
use Elasticsearch\Transport;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Endpoints\AbstractEndpoint;
use Elasticsearch\Namespaces\AbstractNamespace;
use Elasticsearch\Serializers\SerializerInterface;
use Elasticsearch\Namespaces\NamespaceBuilderInterface;
use Elasticsearch\ConnectionPool\SimpleConnectionPool;
use Elasticsearch\ConnectionPool\Selectors\SelectorInterface;
use Elasticsearch\Common\Exceptions\InvalidArgumentException;
use Elasticsearch\Common\Exceptions\UnexpectedValueException;

use Swoole\Coroutine;
use Psr\Log\LoggerInterface;
use Hyperf\Guzzle\RingPHP\CoroutineHandler;

class Elasticsearch
{
    // @var \Elasticsearch\Client
    protected $client = null;

    // @var array
    protected $config = [];

    // @var \Psr\Log\LoggerInterface
    protected $logger = null;

    // @var \Psr\Log\LoggerInterface
    protected $tracer = null;

    // @var ?callable
    protected $builder = null;

    /**
     * @param array $config
     * @param ?\Psr\Log\LoggerInterface $logger
     * @param ?\Psr\Log\LoggerInterface $tracer
     * @param ?callable $builder 用于自定义 client builder 参数的回调
     *   会在 build client 前调用，调用时传递2个参数
     *   - \Elasticsearch\ClientBuilder $clientBuilder
     *   - array $config 即构造方法传递的 $config 参数
     * @return self
     * @throws \TypeError 参数类型错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\InvalidArgumentException $config 参数错误或必传的参数(如hosts)没有传递
     */
    public function __construct(
        array $config,
        ?LoggerInterface $logger = null,
        ?LoggerInterface $tracer = null,
        ?callable $builder = null
    ) {
        $this->logger = $logger;
        $this->tracer = $tracer;
        $this->config = $this->verifyConfig($config);
        $this->builder = $builder;
    }

    /**
     * 验证配置信息
     *
     * @param array $config
     * @return array
     * @throws \TypeError 参数类型错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\InvalidArgumentException
     */
    protected function verifyConfig(array $config): array
    {
        if (isset($config['selector']) && !($config['selector'] instanceof SelectorInterface)) {
            throw new \TypeError(sprintf(
                'Argument %s passed to %s::__construct() must be an instance of %s, %s given',
                'config.selector',
                __CLASS__,
                SelectorInterface::class,
                is_object($config['selector']) ? get_class($config['selector']) : gettype($config['selector'])
            ));
        }

        if (isset($config['retries'])) {
            if (!is_numeric($config['retries']) || $config['retries'] < 0) {
                throw new InvalidArgumentException('config.retries 必须大于0');
            }

            $config['retries'] = (int)$config['retries'];
        }

        if (empty($config['hosts']) || !is_array($config['hosts'])) {
            throw new InvalidArgumentException('config.hosts 不能为空且必须是数组');
        }

        return $config;
    }

    /**
     * 获取 aws SQL 命名空间
     *
     * @return \Captainbi\Hyperf\Util\OpendistroSqlNamespace
     */
    public function awsSql(): OpendistroSqlNamespace
    {
        return $this->getClient()->awsSql();
    }

    /**
     * {@inheritdoc}
     * @throws \Error 方法不存在时抛出此异常
     */
    public function __call(string $name, array $arguments)
    {
        if (method_exists($this->getClient(), $name)) {
            return $this->getClient()->$name(...$arguments);
        }

        throw new \Error('Call to undefined method ' . __CLASS__ . "::{$name}()");
    }

    /**
     * 获取 ES 连接实例
     *
     * @return \Elasticsearch\Client
     */
    protected function getClient()
    {
        if (!($this->client instanceof Client)) {
            $clientBuilder =  ClientBuilder::create();

            if (Coroutine::getCid() > 0) {
                $clientBuilder->setHandler(new CoroutineHandler());
            }

            if (isset($this->config['retries']) && $this->config['retries']) {
                $clientBuilder->setRetries($this->config['retries']);
            }

            if (isset($this->config['selector'])) {
                $clientBuilder->setSelector($this->config['selector']);
            }

            if ($this->logger) {
                $clientBuilder->setLogger($this->logger);
            }

            if ($this->tracer) {
                $clientBuilder->setTracer($this->tracer);
            }

            $clientBuilder->setHosts($this->config['hosts'])
                 ->setConnectionPool(SimpleConnectionPool::class)
                 ->registerNamespace(new OpendistroSqlNamespace());

            if (is_callable($this->builder)) {
                $this->builder($clientBuilder, $this->config);
            }

            $this->client = $clientBuilder->build();
        }

        return $this->client;
    }
}

/**
 * 自定义 Opendistro-for-Search SQL 命名空间
 * 用于注册命名空间到 ES SDK 中
 */
class OpendistroSqlNamespace extends AbstractNamespace implements NamespaceBuilderInterface
{
    // @var string
    const PARAM_INT = 'INT';

    // @var string
    const PARAM_STR = 'STR';

    const PARAM_STRING = 'STRING';

    // @var string
    const PARAM_NULL = 'NULL';

    // @var string
    const PARAM_FLOAT = 'FLOAT';

    // @var string
    const PARAM_BOOL = 'BOOL';

    // @var string
    const PARAM_BOOLEAN = 'BOOLEAN';

    // @var string
    const PARAM_BYTE = 'BYTE';

    // @var string
    const PARAM_SHORT = 'SHORT';

    // @var string
    const PARAM_INTEGER = 'INTEGER';

    // @var string;
    const PARAM_LONG = 'LONG';

    // @var string
    const PARAM_KEYWORD = 'KEYWORD';

    // @var string
    const PARAM_DATE = 'DATE';

    // @var string
    const PARAM_DOUBLE = 'DOUBLE';

    // @var string[]
    const PREPARE_PARAMS = [
        self::PARAM_INT => self::PARAM_INTEGER,
        self::PARAM_STR => self::PARAM_STRING,
        self::PARAM_NULL => self::PARAM_NULL,
        self::PARAM_LONG => self::PARAM_LONG,
        self::PARAM_FLOAT => self::PARAM_FLOAT,
        self::PARAM_BOOL => self::PARAM_BOOLEAN,
        self::PARAM_BOOLEAN => self::PARAM_BOOLEAN,
        self::PARAM_BYTE => self::PARAM_BYTE,
        self::PARAM_SHORT => self::PARAM_SHORT,
        self::PARAM_INTEGER => self::PARAM_INTEGER,
        self::PARAM_KEYWORD => self::PARAM_KEYWORD,
        self::PARAM_DATE => self::PARAM_DATE,
        self::PARAM_DOUBLE => self::PARAM_DOUBLE,
    ];

    /** {@inheritdoc} */
    public function __construct()
    {
        // just overrive parent __construct method
    }

    /** {@inheritdoc} */
    public function getName(): string
    {
        return 'awsSql';
    }

    /** {@inheritdoc} */
    public function getObject(Transport $transport, SerializerInterface $serializer)
    {
        $this->transport = $transport;
        $this->serialize = $serializer;
        $this->endpoints = function(string $uri = '') {
            return new OpendistroSqlEndpoint($uri);
        };

        return $this;
    }

    /**
     * 执行 ES sql 查询
     *
     * @param string|array $sql 可以是sql语句也可以是数组，数组支持以下参数
     *   - query: string sql 语句
     *   - format: string 响应格式，支持的格式有 json(默认), jdbc, csv, raw
     *   - parameters: object[] 如果 sql 语句使用占位符"?"(仅支持位置占位符，不支持占位符命名）
     *       占位符实参通过该数组传递，每个数组元素都是一个包含 type 和 value 的对象
     *       [{"type": "string", "value": "str"}, {"type": "integer", "value": 123}]
     *       支持的 type 有
     *       - string, keyword, date (统一为 string, \Captainbi\Hyperf\Util\OpendistroSqlNamespace::PARAM_STR）
     *       - long, integer, short, byte (统一为 long, \Captainbi\Hyperf\Util\OpendistroSqlNamespace::PARAM_INT)
     *       - double, float (统一为 double, \Captainbi\Hyperf\Util\OpendistroSqlNamespace::PARAM_FLOAT)
     *       - boolean (\Captainbi\Hyperf\Util\OpendistroSqlNamespace::PARAM_BOOL)
     *       - null (\Captainbi\Hyperf\Util\OpendistroSqlNamespace::PARAM_NULL)
     *   - filter: array|object 原生 ES DSL 语句里的 filter
     *   - fetch_size: int 仅在 format=jdbc 的情况下可用，且 ES 的 opendistro.sql.cursor.enabled 参数必须为 true
     *   - cursor: string 通过 fetch_size 开启游标后，之后的请求通过传递 cursor 即可
     *
     * @param mixed ...$vars 通过可变数量参数传递的 parameters 实参
     *   如果是标量，则根据标量在 PHP 中的数据类型进行类型映射
     *   如果是数组或对象，必须包含 type 和 value
     * @return array|callable
     * @throws \TypeError 参数类型错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\InvalidArgumentException 参数错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\UnexpectedValueException 请求参数(requestBody)为空时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\ElasticsearchException|\Exception 请求错误时抛出异常
     */
    public function query($sql, ...$vars)
    {
        list($body, $params) = $this->pareseRequest($sql, $vars);
        return $this->request($body, $params);
    }

    /**
     * 获取 ES sql 语句转换后的 ES DSL 语句
     *
     * @see query 方法说明
     */
    public function explain($sql, ...$vars)
    {
        list($body, $params) = $this->pareseRequest($sql, $vars);
        return $this->request($body, $params, OpendistroSqlEndpoint::ENDPOINT_EXPLAIN);
    }

    /**
     * 关闭游标
     *
     * @param string $cursor 游标
     * @return array|callable
     * @throws \Elasticsearch\Common\Exceptions\ElasticsearchException|\Exception 请求错误时抛出异常
     */
    public function close(string $cursor)
    {
        return $this->request(
            [OpendistroSqlEndpoint::REQ_CURSOR => $cursor],
            [],
            OpendistroSqlEndpoint::ENDPOINT_CLOSE
        );
    }

    /**
     * 获取游标的下一批数据
     * 注！仅 jdbc 响应格式支持游标，且 ES 的 opendistro.sql.cursor.enabled 参数必须为 true
     *
     * @param string $cursor 游标
     * @return array|callable
     * @throws \Elasticsearch\Common\Exceptions\ElasticsearchException|\Exception 请求错误时抛出异常
     */
    public function cursor(string $cursor)
    {
        return $this->request(
            [OpendistroSqlEndpoint::REQ_CURSOR => $cursor],
            [OpendistroSqlEndpoint::PARAM_FORMAT => OpendistroSqlEndpoint::FORMAT_JDBC]
        );
    }

    /**
     * 发起 es 请求
     *
     * @param array $body
     * @param array $params
     * @param string $uri
     * @return array|callable
     * @throws \Elasticsearch\Common\Exceptions\ElasticsearchException|\Exception 请求错误时抛出异常
     */
    protected function request(array $body, array $params, string $uri = '')
    {
        $endpointBuilder = $this->endpoints;
        $endpoint = $endpointBuilder($uri);
        $endpoint->setParams($params);
        $endpoint->setBody($body);

        return $this->performRequest($endpoint);
    }

    /**
     * 抛出 \TypeError 异常
     *
     * @param mixed $value
     * @param string $arg
     * @param string $expect
     * @throws \TypeError 参数类型错误时抛出此异常
     */
    private function throwTypeError($value, string $arg, string $expect)
    {
        throw new \TypeError(sprintf(
            'Argument %s passed to %s::%s() must be an instance of %s, %s given',
            $arg,
            __CLASS__,
            'query',
            $expect,
            is_object($value) ? get_class($value) : gettype($value)
        ));
    }

    /**
     * 断言 parameters 数组元素符合期望
     *
     * @param mixed $v
     * @param string $args
     * @throws \TypeError 参数类型错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\InvalidArgumentException 参数错误时抛出此异常
     */
    private function assertParametersItem($v, string $arg)
    {
        if (!isset($v['type'], $v['value'])) {
            throw new InvalidArgumentException("{$arg} 必须包含 type 和 value");
        }

        if (!is_string($v['type'])) {
            $this->throwTypeError($v['type'], "{$arg}.type", 'string');
        }

        if (!is_scalar($v['value'])) {
            $this->throwTypeError($v['value'], "{$arg}.value", 'scalar');
        }

        if (!isset(self::PREPARE_PARAMS[strtoupper($v['type'])])) {
            throw new InvalidArgumentException("{$arg}.type 不是允许的参数类型");
        }
    }

    /**
     * 格式化请求参数
     *
     * @param array|string &$request
     * @param array $vars
     * @return array 返回的数组里包含 请求body 和请求url参数 2 个数组
     * @throws \TypeError 参数类型错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\InvalidArgumentException 参数错误时抛出此异常
     * @throws \Elasticsearch\Common\Exceptions\UnexpectedValueException 请求参数(requestBody)为空时抛出此异常
     */
    protected function pareseRequest(&$request, array $vars): array
    {
        $body = $prepareParams = [];
        if (is_string($request)) {
            $body[OpendistroSqlEndpoint::REQ_QUERY] = $request;
            $request = [OpendistroSqlEndpoint::PARAM_FORMAT => OpendistroSqlEndpoint::FORMAT_JSON];
        } elseif (is_array($request)) {
            $query = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_QUERY);
            $filter = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_FILTER);
            $cursor = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_CURSOR);
            $fetchSize = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_FETCH_SIZE);
            $prepareParams = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_PARAMS);
            $requestTimeout = $this->extractArgument($request, OpendistroSqlEndpoint::REQ_REQUEST_TIMEOUT);

            if ($cursor) {
                if (is_string($cursor) && !empty($cursor = trim($cursor))) {
                    $body[OpendistroSqlEndpoint::REQ_CURSOR] = $cursor;
                    $request[OpendistroSqlEndpoint::PARAM_FORMAT] = OpendistroSqlEndpoint::FORMAT_JDBC;
                } else {
                    if (!is_string($cursor)) {
                        $this->throwTypeError($cursor, '1.cursor', 'string');
                    } else {
                        throw new InvalidArgumentException('cursor 不能为空');
                    }
                }
            } else {
                if ($query) {
                    if (is_string($query) && !empty($query = trim($query))) {
                        $body[OpendistroSqlEndpoint::REQ_QUERY] = $query;
                    } else {
                        if (!is_string($query)) {
                            $this->throwTypeError($query, '1.query', 'string');
                        } else {
                            throw new InvalidArgumentException('query 不能为空');
                        }
                    }
                }

                if ($filter) {
                    if (is_array($filter) || is_object($filter)) {
                        $body[OpendistroSqlEndpoint::REQ_FILTER] = (array)$filter;
                    } else {
                        $this->throwTypeError($filter, '1.filter', 'array|object');
                    }
                }

                if ($fetchSize) {
                    if (is_numeric($fetchSize) && $fetchSize >= 0) {
                        if (($fetchSize = (int)$fetchSize) > 0) {
                            $body[OpendistroSqlEndpoint::REQ_FETCH_SIZE] = $fetchSize;
                            $request[OpendistroSqlEndpoint::PARAM_FORMAT] = OpendistroSqlEndpoint::FORMAT_JDBC;
                        }
                    } else {
                        if (!is_numeric($fetchSize)) {
                            $this->throwTypeError($fetchSize, '1.fetch_size', 'integer');
                        } else {
                            throw new InvalidArgumentException('fetch_size 必须大于等于0');
                        }
                    }
                }

                if ($prepareParams && !is_array($prepareParams)) {
                    $this->throwTypeError($prepareParams, '1.parameters', 'array');
                }

                if ($requestTimeout) {
                    if (!is_string($requestTimeout)
                        || 1 !== preg_match('/^\d+(s|m|h|d|ms|micros|nanos)$/', $requestTimeout)
                    ) {
                        $this->throwTypeError($requestTimeout, '1.request_timeout', 'string');
                    }

                    $body[OpendistroSqlEndpoint::REQ_REQUEST_TIMEOUT] = $requestTimeout;
                }
            }
        } else {
            $this->throwTypeError($request, '1', 'array|string');
        }

        if (empty($body)) {
            throw new UnexpectedValueException('请求不能为空');
        }

        if ($prepareParams && $vars) {
            throw new InvalidArgumentException('Argument 1.parameters 不能同时通过可变数量参数和 parameters 参数传递');
        }

        if ($prepareParams) {
            foreach ($prepareParams as $k => $v) {
                $this->assertParametersItem($v, "parameters[{$k}]");
                $prepareParams[$k]['type'] = self::PREPARE_PARAMS[strtoupper($v['type'])];
            }

            $body[OpendistroSqlEndpoint::REQ_PARAMS] = $prepareParams;
        }

        if ($vars) {
            foreach ($vars as $k => $v) {
                if (is_scalar($v)) {
                    switch(gettype($v)) {
                        case 'integer':
                            $vars[$k] = ['type' => self::PARAM_LONG, 'value' => $v];
                            break;
                        case 'boolean':
                            $vars[$k] = ['type' => self::PARAM_BOOLEAN, 'value' => $v];
                            break;
                        case 'NULL':
                            $vars[$k] = ['type' => self::PARAM_NULL, 'value' => null];
                            break;
                        case 'float':
                        case 'double':
                            $vars[$k] = ['type' => self::PARAM_DOUBLE, 'value' => $v];
                            break;
                        default:
                            $vars[$k] = ['type' => self::PARAM_STRING, 'value' => (string)$v];
                    }
                } elseif (is_array($v)) {
                    $this->assertParametersItem($v, (string)$k);
                    $vars[$k]['type'] = self::PREPARE_PARAMS[strtoupper($v['type'])];
                } else {
                    $this->throwTypeError($v, (string)$k, 'scalar|array');
                }
            }

            $body[OpendistroSqlEndpoint::REQ_PARAMS] = $vars;
        }

        return [$body, $request];
    }
}

class OpendistroSqlEndpoint extends AbstractEndpoint
{
    // @var string
    const FORMAT_JSON = 'json';

    // @var string
    const FORMAT_JDBC = 'jdbc';

    // @var string
    const FORMAT_CSV = 'csv';

    // @var string
    const FORMAT_RAW = 'raw';

    // @var string
    const PARAM_FORMAT = 'format';

    // @var string
    const REQ_QUERY = 'query';

    // @var string
    const REQ_FILTER = 'filter';

    // @var string
    const REQ_CURSOR = 'cursor';

    // @var string
    const REQ_FETCH_SIZE = 'fetch_size';

    // @var string
    const REQ_PARAMS = 'parameters';

    // @var string
    const REQ_REQUEST_TIMEOUT = 'request_timeout';

    // @var string
    const ENDPOINT_EXPLAIN = 'explain';

    // @var string
    const ENDPOINT_CLOSE = 'close';

    // @var string
    protected $uri = '/_opendistro/_sql';

    /**
     * @param string $uri
     * @return self
     */
    public function __construct(string $uri = '')
    {
        if ($uri) {
            $this->uri .= "/_{$uri}";
        }
    }

    /** {@inheritdoc} */
    public function getParamWhitelist(): array
    {
        return [self::PARAM_FORMAT];
    }

    /** {@inheritdoc} */
    public function getURI(): string
    {
        return $this->uri;
    }

    /** {@inheritdoc} */
    public function getMethod(): string
    {
        return 'POST';
    }

    /**
     * @param array $body
     * @return self
     */
    public function setBody(array $body)
    {
        if ($body) {
            $this->body = $body;
        }

        return $this;
    }
}
