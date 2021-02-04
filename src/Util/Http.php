<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use Hyperf\Guzzle\CoroutineHandler;
use Hyperf\Guzzle\RetryMiddleware;

class Http {
    //客户端
    protected $client = '';

    /**
     * 返回http客户端
     * @param string $host
     * @param string $proxy
     * @return Client|string
     */
    final public function getClient(string $host = 'http://127.0.0.1:80', string $proxy=""){
        if(empty($this->client)){
            // 默认的重试Middleware
            $retry = make(RetryMiddleware::class, [
                'retries' => 5,
                'delay' => 5,
            ]);
            $handler = new CoroutineHandler();

            $stack = HandlerStack::create($handler);
            $stack->push($retry->getMiddleware(), 'retry');

            $config = [
                'base_uri' => $host,
                'handler' => $stack,
                'timeout' => 5
            ];
            if($proxy){
                $config['proxy'] = $proxy;
            }
            $this->client = new Client($config);
        }

        return $this->client;
    }

}