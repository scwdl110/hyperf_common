<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\Listener;

use Hyperf\Database\Events\QueryExecuted;
use Hyperf\DbConnection\Db;
use Hyperf\Event\Annotation\Listener;
use Hyperf\Event\Contract\ListenerInterface;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\Arr;
use Hyperf\Utils\Str;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Hyperf\Utils\Context;
use Psr\Http\Message\ServerRequestInterface;
use Captainbi\Hyperf\Util\Logger;

/**
 * @Listener
 */
class DbExecutedListener implements ListenerInterface
{

    private $UnlistenTables = array('c_access_logger', 'c_execute_logger');

    private $UnlistenOps = array('select');

    public function listen(): array
    {
        return [
            QueryExecuted::class,
        ];
    }

    /**
     * @param QueryExecuted $event
     */
    public function process(object $event)
    {
        if ($event instanceof QueryExecuted) {
            $sql = $event->sql;
            if (!Arr::isAssoc($event->bindings)) {
                foreach ($event->bindings as $key => $value) {
                    $sql = Str::replaceFirst('?', "'{$value}'", $sql);
                }
            }

            $request = Context::get(ServerRequestInterface::class);
            $userInfo = $request->getAttribute('userInfo');
            $admin_id = $userInfo['admin_id'];
            $user_id = $userInfo['user_id'];
            foreach ($this->UnlistenTables as $UnlistenTable) {
                if (strstr($sql, $UnlistenTable) != false) {
                    return;
                }
            }

            foreach ($this->UnlistenOps as $UnlistenOp) {
                if (strstr($sql, $UnlistenOp) == false) {
                    Logger::execute_log($admin_id, $user_id, $sql);
                }
            }
        }
    }
}