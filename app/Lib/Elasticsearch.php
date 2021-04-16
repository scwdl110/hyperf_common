<?php

namespace App\Lib;

use Throwable;
use ReflectionClass;
use RuntimeException;

use Swoole\Coroutine;
use Psr\Log\LoggerInterface;
use Elasticsearch\ClientBuilder;

use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Guzzle\RingPHP\PoolHandler;

use App\Lib\Elasticsearch\OpenSqlNamespaceBuilder;

class Elasticsearch
{
    protected $logger = null;

    protected $handler = null;

    protected $esClient = null;

    protected $sqlPath = '';

    protected static $connections = [];

    protected static $connectionKeys = [];

    public static function getConnection(array $config, ?LoggerInterface $logger = null, ?callable $handler = null, ?LoggerInterface $tracer = null)
    {
        if (null === $logger) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('elasticsearch', 'default');
        }

        if (!isset($config['hosts']) || !is_array($config['hosts'])) {
            $logger->error('es 连接参数错误', [$config]);
            throw new RuntimeException('Elasticsearch connection config is required.');
        }

        $key = self::getConnectionsKey($config, $logger, $handler, $tracer);
        if (!isset(self::$connections[$key])) {
            $key2 = -1;
            if (null === $handler) {
                if (null !== ($handler = self::createHandler($logger))) {
                    $key2 = self::getConnectionsKey($config, $logger, $handler, $tracer);
                }
            }

            if ($key2 !== -1) {
                if (!isset(self::$connections[$key2])) {
                    self::$connections[$key2] = new self($config, $logger, $handler, $tracer);
                }

                self::$connections[$key] = self::$connections[$key2];
            } else {
                self::$connections[$key] = new self($config, $logger, $handler, $tracer);
            }
        }

        return self::$connections[$key];
    }

    protected static function getConnectionsKey(array $config, LoggerInterface $logger, ?callable $handler = null, ?LoggerInterface $tracer = null): int
    {
        foreach (self::$connectionKeys as $key => $val) {
            if ($config === $val[0] && $logger === $val[1] && $handler === $val[2] && $tracer === $val[3]) {
                return $key;
            }
        }

        return array_push(self::$connectionKeys, [$config, $logger, $handler, $tracer]) - 1;
    }

    protected static function createHandler(LoggerInterface $logger): ?callable
    {
        if (Coroutine::getCid() > 0) {
            return make(PoolHandler::class, [
                'option' => [
                    'max_connections' => 50,
                ],
            ]);
        }

        return null;
    }

    private function __construct(array $config, LoggerInterface $logger, ?callable $handler = null, ?LoggerInterface $tracer = null)
    {
        $this->logger = $logger;

        $this->sqlPath = ltrim(trim($config['sql_path'] ?? ''), '/');
        unset($config['sql_path']);

        $config['logger'] = $logger;
        if ($handler) {
            $config['handler'] = $handler;
        }

        if ($tracer) {
            $config['tracer'] = $tracer;
        }

        $builder = new ClientBuilder();
        foreach ($config as $key => $value) {
            $method = "set$key";
            $reflection = new ReflectionClass($builder);
            if ($reflection->hasMethod($method)) {
                $func = $reflection->getMethod($method);
                if ($func->getNumberOfParameters() > 1) {
                    $builder->$method(...$value);
                } else {
                    $builder->$method($value);
                }
                unset($config[$key]);
            }
        }

        if (count($config) > 0) {
            $unknown = implode(array_keys($config));
            throw new RuntimeException("Unknown parameters provided: $unknown");
        }

        if ($this->sqlPath) {
            $builder->registerNamespace(new OpenSqlNamespaceBuilder($this->sqlPath));
        }

        $this->esClient = $builder->build();
    }

    public function query(string $sql)
    {
        try {
            return $this->esClient->opensql()->query(['body' => json_encode(['query' => $sql])]);
        } catch (Throwable $t) {
            $this->logger->error('es 请求异常：' . $t->getMessage(), [$sql, $t]);
            return false;
        }
    }
}
