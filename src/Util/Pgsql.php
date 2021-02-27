<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Hyperf\Logger\LoggerFactory;
use Swoole\Coroutine\PostgreSQL;
use Psr\Log\LoggerInterface;


class Pgsql {

    //客户端
    protected $client = '';

    /**
     * @var LoggerInterface
     */
    protected $logger;

    public function __construct(LoggerFactory $loggerFactory)
    {
        $this->logger = $loggerFactory->get('log', 'default');
    }

    /**
     * 返回客户端
     * @param string $host
     * @return \Elasticsearch\Client|string
     */
    final public function getClient(array $hosts = []){
        if(!$hosts || !isset($hosts['host']) || !isset($hosts['database']) || !isset($hosts['port']) || !isset($hosts['username']) || !isset($hosts['password'])){
            $this->logger->error('pgsql无数据库参数');
            return false;
        }
        if(empty($this->client)){
            $pg = new PostgreSQL();
            $conn = $pg->connect("host=".$hosts['host']." port=".$hosts['port']." dbname=".$hosts['database']." user=".$hosts['username']." password=".$hosts['password']);
            if (!$conn) {
                $this->logger->error('pgsql连接失败');
                return false;
            }
            $this->client = $pg;
        }
        return $this->client;
    }

}