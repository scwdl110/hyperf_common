<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Throwable;
use RuntimeException;
use UnexpectedValueException;

use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Pool\SimplePool\PoolFactory;

use Psr\Log\LoggerInterface;
use Swoole\Coroutine\Http\Client;

/**
 * Solr 封装类
 * 仅封装为兼容 BI solr类，没有实现所有 solr 功能
 *
 * @author Chopin Ngo <wushaobin@captainbi.com>
 */
class Solr
{
    // @var int 默认请求重试次数
    const DEFAULT_RETRY = 1;

    // @var int 默认请求超时时间
    const DEFAULT_TIMEOUT = 60;

    // @var string http get 方法
    const HTTP_METHOD_GET = 'GET';

    // @var string http post 方法
    const HTTP_METHOD_POST = 'POST';

    // @var \Hyperf\Pool\SimplePool\Pool http 连接池对象
    protected $pool = null;

    // @var string 默认的 solr 请求路径
    protected $endpoint = '';

    // @var \Psr\Log\LoggerInterface 日志实例
    protected $logger = null;

    // @var \Swoole\Coroutine\Http\Client http 请求实例
    protected $httpClient = null;

    // @var int 请求重试次数
    protected $retry = self::DEFAULT_RETRY;

    // @var int 请求超时时间
    protected $timeout = self::DEFAULT_TIMEOUT;

    // @var \Hyperf\Pool\SimplePool\Pool[] 存储 http 连接池对象的数组
    // 使用类静态属性存储避免重复调用 连接池工厂方法
    // 不使用该属性也没问题
    protected static $httpClientPool = [];

    /**
     * 实例化
     *
     * @param array $config 实例化参数，允许的参数如下
     * ```php
     * $config = [
     *     'host' => 'string', // 必须，solr 服务器地址，可以是 ip 也可以是域名，https 的话需要带上 协议头
     *     'port' => 'int',    // 必须，服务器端口
     *     'path' => 'string', // 必须，solr 请求路径
     *     'timeout' => 'int', // 可选，请求超时时间，单位 秒
     *     'retry' => 'int',   // 可选，请求重试次数
     * ]
     * ```
     * @param ?\Psr\Log\LoggerInterface $logger
     * @return self
     * @throws \RuntimeException 参数错误或参数类型不正确时抛出此异常
     */
    public function __construct(array $config, ?LoggerInterface $logger = null)
    {
        if (null === $logger) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('solr', 'default');
        }

        $this->logger = $logger;

        if (!isset($config['host'], $config['port'], $config['path'])) {
            $this->logger->error('solr 连接参数错误', [$config]);
            throw new RuntimeException('Solr connection config is required.');
        }

        $ssl = false;
        $port = (int)$config['port'];
        $host = trim($config['host']);
        $this->endpoint = trim($config['path']);

        if (empty($host) || empty($this->endpoint) || $port < 1 || $port > 65535) {
            $this->logger->error('solr 连接参数数据类型错误', [$config]);
            throw new RuntimeException('Invalid solr connection config.');
        }

        $this->endpoint = '/' . trim($this->endpoint, '/');
        if (1 === preg_match('/^(https)?:\/\/(.+)$/i', $host, $match)) {
            $ssl = 5 === strlen($match[1]);
            $host = $match[2];
        }

        $timeout = (int)ini_get('default_socket_timeout');
        if (isset($config['timeout']) && is_numeric($config['timeout']) && $config['timeout'] > 0) {
            $timeout = (int)$config['timeout'];
        }

        if ($timeout <= 0) {
            $timeout = self::DEFAULT_TIMEOUT;
        }

        $this->timeout = $timeout;

        if (isset($config['retry']) && is_numeric($config['retry']) && $config['retry'] >= 0) {
            $this->retry = (int)$config['retry'];
        }

        $poolKey = "solr_http_client_pool_{$host}_{$port}_{$ssl}";
        if (!isset(static::$httpClientPool[$poolKey])) {
            $factory = ApplicationContext::getContainer()->get(PoolFactory::class);

            static::$httpClientPool[$poolKey] = $factory->get($poolKey, function() use ($host, $port, $ssl) {
                return new Client($host, $port, $ssl);
            }, [
                'max_connections' => 20,
            ])->get();
        }

        $this->pool = static::$httpClientPool[$poolKey];
    }

    /**
     * 从连接池中获取 http 请求连接
     *
     * @return \Swoole\Coroutine\Http\Client
     */
    protected function getClient(): Client
    {
        if (!($this->httpClient instanceof Client)) {
            $this->httpClient = $this->pool->getConnection();
        }

        return $this->httpClient;
    }

    /**
     * 将 http 请求连接释放回连接池
     *
     * @return void
     */
    protected function releaseClient()
    {
        if ($this->httpClient instanceof Client) {
            $this->pool->release();
        }

        $this->httpClient = null;
    }

    /**
     * 发起 solr 请求
     *
     * @param string|array $param 具体参数见 solr 官方文档
     * @param ?string $endpoint 请求路径
     * @param ?int $timeout 连接超时时间
     * @param string $contentType http header content-type
     * @param string $method 仅支持 GET 或 POST
     * @param ?int $retry 连接超时 或 solr 服务器错误 时的重试次数
     * @return false|array 失败返回 false，成功返回 json_decode($response, true) 后的数组
     */
    protected function request(
        $param,
        ?string $endpoint = null,
        ?int $timeout = null,
        string $contentType = 'application/x-www-form-urlencoded; charset=UTF-8',
        string $method = self::HTTP_METHOD_POST,
        ?int $retry = null
    ) {
        $retry = $retry ?? $this->retry;
        $endpoint = $endpoint ?? $this->endpoint;

        try {
            $path = $endpoint;
            $client = $this->getClient();
            if (self::HTTP_METHOD_GET === $method) {
                $client->setHeaders([]);
                $client->setMethod(self::HTTP_METHOD_GET);
                if ($param) {
                    if (is_array($param)) {
                        $path = $endpoint . '&' . $this->generateQueryString($param);
                    } elseif (is_string($param)) {
                        $path = "{$endpoint}&{$param}";
                    }
                }

                if ($path === $endpoint) {
                    return false;
                }
            } else {
                if (empty($param) || !(is_string($param) || is_array($param))) {
                    return false;
                }

                $client->setHeaders(['Content-Type' => $contentType]);
                $client->setMethod(self::HTTP_METHOD_POST);
                $client->setData($param);
            }

            $client->set(['timeout' => $timeout ?? $this->timeout]);
            $result = $client->execute($path);
            if ($result && $client->statusCode === 200) {
                return $this->parseResponse($client->body);
            } else {
                $msg = "[{$client->errCode}]{$client->errMsg}";
                $logExt = [$path, $client];

                if (-1 === $result || -2 === $result || $client->statusCode >= 500) {
                    $msg = $client->statusCode > 500
                        ? "solr 服务器异常: [{$client->statusCode}]{$msg}"
                        : "solr 请求超时：{$msg}";

                    if ($retry) {
                        $this->logger->info("{$msg} : 等待重试(最多重试 {$retry} 次)");
                        // 随机等待 0.3 到 1 秒
                        \Swoole\Coroutine\System::sleep(mt_rand(300, 1000) / 1000);
                        return $this->request($param, $endpoint, $timeout, $contentType, $method, --$retry);
                    } else {
                        $this->logger->error("{$msg} : 停止重试", $logExt);
                    }
                } else {
                    $this->logger->error("solr 请求失败: {$msg} : [{$client->statusCode}]{$client->body}", $logExt);
                }
            }
        } catch (Throwable $t) {
            $this->logger->error("solr 请求异常：[{$t->getCode()}]{$t->getMessage()}", [$t]);
        } finally {
            $this->releaseClient();
        }

        return false;
    }

    /**
     * 将数组转换为 query_string
     *
     * @param array $param
     * @return string
     */
    protected function generateQueryString(array $param): string
    {
        // because http_build_query treats arrays differently than we want to, correct the query
        // string by changing foo[#]=bar (# being an actual number) parameter strings to just
        // multiple foo=bar strings. This regex should always work since '=' will be urlencoded
        // anywhere else the regex isn't expecting it
        return preg_replace('/%5B(?:[0-9]|[1-9][0-9]+)%5D=/', '=', http_build_query($param, '', '&'));
    }

    /**
     * 格式化 solr 原始请求为 php 数组
     *
     * @param string $body
     * @return array
     * @throws \UnexpectedValueException 响应数据不是 json 格式时抛出此异常
     */
    protected function parseResponse(string $body): array
    {
        $data = json_decode($body, true);
        if (null === $data) {
            throw new UnexpectedValueException("solr 响应异常：{$body}");
        }

        if (isset($data['response']['docs']) && is_array($data['response']['docs'])) {
            $docs = [];
            foreach ($data['response']['docs'] as $doc) {
                foreach ($doc as &$v) {
                    if (is_array($v) && sizeof($v) <= 1) {
                        $v = array_shift($v);
                    }
                }

                $docs[] = $doc;
            }

            $data['response']['docs'] = $docs;
        }

        return $data;
    }

    /**
     * 通过查询条件删除 solr 文档
     *
     * @param string $query 查询条件
     * @return false|array
     */
    public function deleteByQuery(string $query)
    {
        // escape special xml characters
        $query = htmlspecialchars($query, ENT_NOQUOTES, 'UTF-8');
        $xml = "<delete fromPending='true' fromCommitted='true'><query>{$query}</query></delete>";

        return $this->request($xml, $this->endpoint, 3600, 'text/xml; charset=UTF-8');
    }

    /**
     * 发起 solr 查询
     *
     * @param string $query 查询条件
     * @param array $param 其他 solr search 参数，具体将 solr 官方文档
     * @param int $page 分页页码
     * @param int $size 分页每页数量
     * @return false|array 失败返回 false
     */
    public function query(string $query, array $param = [], int $page = 1, int $size = 20)
    {
        // 避免 '+' 被转义为 空格
        $param['q'] = urldecode(str_replace('+', '%2B', $query));
        $param['wt'] = 'json';
        $param['json.nl'] = 'map';
        $param['start'] = ($page - 1) * $size;
        $param['rows'] = $size;

        if (false === ($resp = $this->request($this->generateQueryString($param), "{$this->endpoint}/select"))) {
            return false;
        }

        if (!isset($resp['response']['docs'], $resp['response']['numFound'])
            || !is_numeric($resp['response']['numFound'])
            || !is_array($resp['response']['docs'])
        ) {
            $this->logger->error('solr 非预期响应', [$resp]);
            return false;
        }

        $facetList = [];
        if (!empty($resp['facet_counts'])) {
            foreach ($resp['facet_counts'] as $fields) {
                $item = [];
                foreach ($fields as $k => $v) {
                    $item[$k] = json_decode(json_encode($v), true);
                }
                $facetList[] = $item;
            }
        }

        $statsList = [];
        if (!empty($resp['stats'])) {
            foreach ($resp['stats'] as $fields) {
                $item = [];
                foreach ($fields as $k => $v) {
                    $item[$k] = json_decode(json_encode($v), true);
                }

                $statsList[] = $item;
            }
        }

        return [
            'total' => (int)$resp['response']['numFound'],
            'list' => $resp['response']['docs'],
            'facet' => $facetList,
            'stats' => $statsList,
        ];
    }

    /**
     * @param string $word
     * @param string $dataType 仅支持 complex 和 simple
     * @param bool $isSynonym
     * @param string $type
     * @return false|array
     */
    public function dict(string $word, string $dataType = 'complex', bool $isSynonym = true, string $type = 'textComplex')
    {
        if ('complex' !== $dataType && 'simple' !== $dataType) {
            return false;
        }

        $resp = $this->request([
            'q' => $word,
            'wt' => 'json',
            'indent' => 'true',
            'analysis.fieldtype' => $type,
        ], "{$this->endpoint}/analysis/field");

        if (false === $resp) {
            return false;
        }

        if (!isset($resp['analysis']['field_types'][$type]['query'][1])
            && !isset($resp['analysis']['field_types'][$type]['query'][3])
        ) {
            $this->logger->error('solr 非预期响应', [$resp]);
            return false;
        }

        // 判断是否返回包含同义词  true是返回
        $res = $resp['analysis']['field_types'][$type]['query'][$isSynonym ? 3 : 1];

        // 判断返回数据类型，是复杂二位数据 还是只返回分词 结果一维数组
        if ($dataType === 'complex') {
            /**
             * Array
             * (
             *      [text] => 分词
             *      [raw_bytes] => [e5 a5 bd]
             *      [start] => 原词开始位置
             *      [end] => 原词结束为止
             *      [org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute#positionLength] => 1  位置长度属性
             *      [type] => word/SYNONYM 类型 是原词 还是同义词
             *      [position] => 2 位置
             *      [positionHistory] => Array  历史位置，记录分词进行了多少次筛选过滤
             *      (
             *          [0] => 2
             *          [1] => 2
             *      )
             * )
             */
            return $res;
        } elseif ($dataType === 'simple') {
            return array_column($res, 'text');
        }

        return false;
    }

    /**
     * @param string $ids
     * @param string $dbhost
     * @param int $page
     * @param int $size
     * @param string $command
     * @return false|array
     */
    public function replaceIntoGoods(
        string $ids,
        string $dbhost = '001',
        int $page = 1,
        int $size = 1000,
        string $command = 'full-import'
    ) {
        return $this->request([
            'command' => $command,
            'commit' => 'true',
            'optimize' => 'false',
            'wt' => 'json',
            'indent' => 'true',
            'verbose' => 'false',
            'clean' => 'false',
            'debug' => 'false',
            'start' => ($page - 1) * $size,
            'row' => $size,
            'ids' => $ids,
        ], "{$this->endpoint}/one{$dbhost}");
    }
}
