<?php

namespace App\Crontab;


use App\Model\UserExtInfoModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Crontab\Annotation\Crontab;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\WaitGroup;
use Captainbi\Hyperf\Util\Log;

use App\Service\AccountingService;
use App\Model\SynchronouslyManagementTaskModel;

//singleton=true, onOneServer=true, mutexPool="default", mutexExpires=120,

/**
 * @Crontab(name="SyncTaskAdd", rule="* *\/1 * * * *", callback="execute", memo="")
 */
class SyncTaskAddCrontab
{
    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    public function execute()
    {
        Log::getCrontabClient()->info("SyncTaskAddCrontab开始执行");

        $userExtInfoList = UserExtInfoModel::query()->where([
            ['is_authorized', "=", 1]
        ],
        )->select("uuid", "admin_id", "user_id", "client_id", "synchronously_method", "synchronously_day")->lockForUpdate()->get();

        foreach ($userExtInfoList as $userExtInfo) {
            $where = [
                ["uuid", "=", $userExtInfo['uuid']],
                ["admin_id", "=", $userExtInfo['admin_id']],
                ["user_id", "=", $userExtInfo['user_id']],
                ["client_id", "=", $userExtInfo['client_id']],
                ["synchronously_method", "=", $userExtInfo['synchronously_method']],
                ["synchronously_day", "=", $userExtInfo['synchronously_day']],
                ["myear", "=", date("Y", strtotime("last month"))],
                ["mmouth", "=", date("m", strtotime("last month"))]
            ];
            $synchronouslyManagementTask = SynchronouslyManagementTaskModel::where($where)->first();
            if ($synchronouslyManagementTask == null) {
                $save_data = array(
                    "uuid" => $userExtInfo['uuid'],
                    "admin_id" => $userExtInfo['admin_id'],
                    "user_id" => $userExtInfo['user_id'],
                    "client_id" => $userExtInfo['client_id'],
                    "synchronously_method" => $userExtInfo['synchronously_method'],
                    "synchronously_day" => $userExtInfo['synchronously_day'],
                    "myear" => date("Y", strtotime("last month")),
                    "mmouth" => date("m", strtotime("last month"))
                );
                SynchronouslyManagementTaskModel::create($save_data);
            }
        }

        Log::getCrontabClient()->info("SyncTaskAddCrontab执行完毕");
    }
}