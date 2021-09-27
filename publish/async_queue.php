<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.(用于管理基于 Redis 实现的简易队列服务)
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
return [
    env('DEFAULT_QUEUE', 'default') => [
        'driver' => Hyperf\AsyncQueue\Driver\RedisDriver::class,
        'redis' => [
            'pool' => env('DEFAULT_QUEUE_DRIVER_REDIS_POOL', 'default'),//redis 连接池
        ],
        'channel' => env('DEFAULT_QUEUE_CHANNEL', '{queue}'),//队列前缀
        'timeout' => 2,//pop 消息的超时时间
        'retry_seconds' => [1, 5, 10, 20],//失败后重新尝试间隔
        'handle_timeout' => 60,//消息处理超时时间
        'processes' => 1,//消费进程数
        'concurrent' => [
            'limit' => 10,//同时处理消息数
        ],
    ],
];
