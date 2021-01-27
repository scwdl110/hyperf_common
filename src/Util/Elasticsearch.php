<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Aws\Credentials\Credentials;
use Aws\Signature\SignatureV4;
use Elasticsearch\ClientBuilder;
use Hyperf\Guzzle\RingPHP\CoroutineHandler;
use Swoole\Coroutine;

class Elasticsearch {

    //凭据
    protected $credentials = '';

    //签名
    protected $signature = '';

    //客户端
    protected $client = '';

    public function __construct($key = '',$secret = '', $region = '') {
        $this->credentials = new Credentials($key, $secret);
        $this->signature = new SignatureV4('es', $region);

    }

    /**
     * 返回客户端
     * @param array $hosts
     * @return \Elasticsearch\Client|string
     */
    final public function getClient(array $hosts = ['http://127.0.0.1:9200']){
        if(empty($this->client)){
            $middleware = new \Wizacha\Middleware\AwsSignatureMiddleware($this->credentials, $this->signature);
            $clientBuilder =  ClientBuilder::create();
            if (Coroutine::getCid() > 0) {
//                $handler = make(PoolHandler::class, [
//                    'option' => [
//                        'max_connections' => 50,
//                    ],
//                ]);
                $handler = new CoroutineHandler();
                $awsHandler = $middleware($handler);
                $clientBuilder->setHandler($awsHandler);
            }
    //            ->setHosts(['https://vpc-captain-search-test-yka42farqqrngrcags3vab6nhq.cn-northwest-1.es.amazonaws.com.cn']);
            $this->client = $clientBuilder->setHosts($hosts)->build();
        }
        return $this->client;
    }

}