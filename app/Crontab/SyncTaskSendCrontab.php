<?php

namespace App\Crontab;


use Hyperf\Contract\ConfigInterface;
use Hyperf\Crontab\Annotation\Crontab;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\WaitGroup;
use Captainbi\Hyperf\Util\Log;

use App\Service\AccountingService;
use App\Model\SynchronouslyManagementTaskModel;

//singleton=true, onOneServer=true, mutexPool="default", mutexExpires=120,

/**
 * @Crontab(name="SyncTaskSend", rule="*\/1 * * * * *", callback="execute", memo="")
 */
class SyncTaskSendCrontab
{
    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @Inject()
     * @var AccountingService
     */
    protected $service;


    public function execute()
    {
        // 并发数
        $synchronouslyManagement = SynchronouslyManagementTaskModel::query()->where([
            ['synchronously_method', "=", 0],
            ['synchronously_status', "=", 0],
            ['synchronously_day', "=", date("d", time())]],
        )->select("id", "uuid")->lockForUpdate()->get();
        $synchronouslyManagementCount = count($synchronouslyManagement);

        Log::getCrontabClient()->info("AddSyncTaskCrontab开始执行,执行条数： $synchronouslyManagementCount 条");
        //无数据
        if ($synchronouslyManagementCount <= 0) {
            return 0;
        }

        //并发数控制
        $wg = new WaitGroup();
        $wg->add($synchronouslyManagementCount);
        foreach ($synchronouslyManagement as $k => $v) {
            $request_data = array(
                'uuid' => $v['uuid'],
                'ids' => strval($v['id'])
            );
            $this->service->syncOps($request_data);
        }
        Log::getCrontabClient()->info("AddSyncTaskCrontab执行完毕,执行条数： $synchronouslyManagementCount 条");
        $wg->wait(120);
    }


}