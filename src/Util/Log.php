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
     * @param string $group 日志文件夹名字
     * @param string $name 日志记录名字
     * @return LoggerInterface
     */
    public static function getClient($group = 'default', $name = 'log'){
        if(empty(self::$logger)){
            $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
            self::$logger = $loggerFactory->get($name, $group);
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