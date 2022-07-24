<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Hyperf\Pool\SimplePool\PoolFactory;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;

/**
 * 需要安装 swoole_postgres 扩展
 * v.a. https://github.com/swoole/ext-postgresql
 * 调用pgsql库的连接池
 */
class Postgresql
{
    /**
     * @param string $configKey
     * @return Pgsql|bool
     */
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
