<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use Hyperf\Utils\ApplicationContext;
use Psr\Log\LoggerInterface;

class Log {
    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * 返回http客户端
     * @param string $host
     * @param string $proxy
     * @return Client|string
     */
    public static function getClient(){
        if(empty(self::$logger)){
            $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
            self::$logger = $loggerFactory->get('log', 'default');
        }

        return self::$logger;
    }

}