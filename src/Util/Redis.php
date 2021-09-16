<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Hyperf\Redis\RedisFactory;
use Hyperf\Utils\ApplicationContext;

class Redis {
    //客户端
    //protected $client = '';

    /**
     * 返回redis客户端
     * @param string $poolName
     * @return \Hyperf\Redis\RedisProxy|string
     */
    final public function getClient(string $poolName  = 'default'){
//        if(empty($this->client)){
//            $container = ApplicationContext::getContainer();
//
//            // 通过 DI 容器获取或直接注入 RedisFactory 类
//            $this->client = $container->get(RedisFactory::class)->get($poolName);
//        }
//
//        return $this->client;

        return ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);

    }

}