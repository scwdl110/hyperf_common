<?php

declare(strict_types=1);

namespace Captainbi\Hyperf;

use Hyperf\Cache\Driver\RedisDriver;
use Hyperf\ConfigApollo\PipeMessage;
use Hyperf\ConfigApollo\Process\ConfigFetcherProcess;
use Captainbi\Hyperf\Process\RestartServiceProcess;
use Captainbi\Hyperf\Util\Redis\Lua\LuaFactory;
use Captainbi\Hyperf\Util\Redis\Lua\Contracts\LuaInterface;

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
            'processes' => [
                RestartServiceProcess::class,
            ],
            'annotations' => [
                'scan' => [
                    'paths' => [
                        __DIR__,
                    ],
                    'class_map' => [
                        // 需要映射的类名 => 类所在的文件地址
                        RedisDriver::class => __DIR__ . '/../class_map/Hyperf/Cache/Driver/RedisDriver.php',
                        PipeMessage::class => __DIR__ . '/ConfigApollo/class_map/PipeMessage.php',
                        ConfigFetcherProcess::class => __DIR__ . '/ConfigApollo/class_map/Process/ConfigFetcherProcess.php',
                    ],
                ],
            ],
            'publish' => [
                [
                    'id' => 'config',
                    'description' => 'The config for apollo',
                    'source' => __DIR__ . '/../publish/apollo.php',
                    'destination' => BASE_PATH . '/config/autoload/apollo.php',
                ],
                [
                    'id' => 'config',
                    'description' => 'The config for restart process',
                    'source' => __DIR__ . '/../publish/restart_console.php',
                    'destination' => BASE_PATH . '/config/autoload/restart_console.php',
                ],
                [
                    'id' => 'config',
                    'description' => 'The script for restart process',
                    'source' => __DIR__ . '/../publish/bin/restart.php',
                    'destination' => BASE_PATH . '/bin/restart.php',
                ],
            ],
        ];
    }
}
