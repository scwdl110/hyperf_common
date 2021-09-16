<?php

namespace Captainbi\Hyperf;

use Hyperf\Cache\Driver\RedisDriver;
use Captainbi\Hyperf\Util\Encryption\Contracts\EncrypterInterface;
use Captainbi\Hyperf\Util\Encryption\EncrypterFactory;
use Captainbi\Hyperf\Util\Redis\Lua\Contracts\LuaInterface;
use Captainbi\Hyperf\Util\Redis\Lua\LuaFactory;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                LuaInterface::class => LuaFactory::class,
                //EncrypterInterface::class => EncrypterFactory::class,
                //FactoryInterface::class  => DingNoticeFactory::class,
            ],
            'commands' => [
            ],
            'annotations' => [
                'scan' => [
                    'paths' => [
                        __DIR__,
                    ],
                    'class_map' => [
                        // 需要映射的类名 => 类所在的文件地址
                        RedisDriver::class => __DIR__ . '/../class_map/Hyperf/Cache/Driver/RedisDriver.php',
                    ],
                ],
            ],
            'publish' => [
//                [
//                    'id' => 'config',
//                    'description' => 'The config for ding.',
//                    'source' => __DIR__ . '/../publish/ding.php',
//                    'destination' => BASE_PATH . '/config/autoload/ding.php',
//                ],
            ],
        ];
    }

}
