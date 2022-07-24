<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Captainbi\Hyperf\Exception\BusinessException;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use Hyperf\Di\Container;
use Hyperf\Guzzle\PoolHandler;
use Hyperf\Pool\SimplePool\PoolFactory;
use Swoole\Coroutine;
use Swoole\Coroutine\PostgreSQL;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;
use Psr\Log\LoggerInterface;

/**
 * 需要安装 swoole_postgres 扩展
 * v.a. https://github.com/swoole/ext-postgresql
 * 调用pgsql库的连接池
 */
class Postgresql
{
    public static function getConnection(string $configKey)
    {
        $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('postgresql', 'default');
        $factory = ApplicationContext::getContainer()->get(PoolFactory::class);

        $config = $container->get(ConfigInterface::class)->get($configKey);
        if (!isset($config['host'], $config['database'], $config['port'], $config['username'], $config['password'])) {
            $logger->error('缺少必须的postgresql连接参数', $config);
            return false;
        }

        $pool = $factory->get($configKey, function () use ($config) {
            return new Pgsql($config);
        }, [
            'max_connections' => 50
        ]);

        $connection = $pool->get();

        $client = $connection->getConnection();

        return $client;
    }
}
