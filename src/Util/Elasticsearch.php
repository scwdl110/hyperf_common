<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Elasticsearch\ClientBuilder;
use Hyperf\Guzzle\RingPHP\CoroutineHandler;
use Swoole\Coroutine;

class Elasticsearch {

    //客户端
    protected $client = '';

    /**
     * 返回客户端
     * @param string $host
     * @return \Elasticsearch\Client|string
     */
    final public function getClient(array $hosts = ['http://127.0.0.1:9200']){
        if(empty($this->client)){
            $clientBuilder =  ClientBuilder::create()->setConnectionPool('\Elasticsearch\ConnectionPool\SimpleConnectionPool', []);
            if (Coroutine::getCid() > 0) {
//                $handler = make(PoolHandler::class, [
//                    'option' => [
//                        'max_connections' => 50,
//                    ],
//                ]);
                $handler = new CoroutineHandler();
                $clientBuilder->setHandler($handler);
            }
            //            ->setHosts(['https://vpc-captain-search-test-yka42farqqrngrcags3vab6nhq.cn-northwest-1.es.amazonaws.com.cn']);
            //setRetries重试次数
            $this->client = $clientBuilder->setRetries(5)->setHosts($hosts)->build();
        }
        return $this->client;
    }

}