<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\Session\Handler;


use Hyperf\Contract\ConfigInterface;
use Hyperf\Redis\RedisFactory;
use Psr\Container\ContainerInterface;

class RedisHandlerFactory
{

    public function __invoke(ContainerInterface $container)
    {
        $config = $container->get(ConfigInterface::class);
        $connection = $config->get('session.options.connection');
        $gcMaxLifetime = $config->get('session.options.gc_maxlifetime', 1200);
        $prefix = $config->get('session.options.prefix', '');
        $redisFactory = $container->get(RedisFactory::class);
        $redis = $redisFactory->get($connection);
        return new RedisHandler($redis, $gcMaxLifetime, $prefix);
    }

}