<?php

namespace Captainbi\Hyperf;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
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
