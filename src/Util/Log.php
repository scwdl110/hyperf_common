<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use Hyperf\Utils\ApplicationContext;
use Psr\Log\LoggerInterface;

class Log {
    /**
     * @var LoggerInterface
     */
    protected static $logger;

    /**
     * 返回日志客户端
     * @return LoggerInterface
     */
    public static function getClient(){
        if(empty(self::$logger)){
            $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
            self::$logger = $loggerFactory->get('log', 'default');
        }

        return self::$logger;
    }

    /**
     * 返回定时任务日志客户端
     * @return LoggerInterface
     */
    public static function getCrontabClient(){
        if(empty(self::$logger)){
            $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
            self::$logger = $loggerFactory->get('log', 'crontab');
        }

        return self::$logger;
    }

}