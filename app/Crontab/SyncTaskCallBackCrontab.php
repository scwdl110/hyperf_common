<?php

namespace App\Crontab;


use App\Model\UserExtInfoModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Crontab\Annotation\Crontab;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Guzzle\ClientFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\WaitGroup;
use Captainbi\Hyperf\Util\Log;

use App\Service\AccountingService;
use App\Model\SynchronouslyManagementTaskModel;

//singleton=true, onOneServer=true, mutexPool="default", mutexExpires=120,

/**
 * @Crontab(name="SyncTaskCallBack", rule="*\/1 * * * * *", callback="execute", memo="")
 */
class SyncTaskCallBackCrontab
{
    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    public function execute()
    {
        Log::getCrontabClient()->info("SyncTaskCallBack开始执行");
        $synchronouslyManagementTaskList = SynchronouslyManagementTaskModel::query()->where([
            ['synchronously_status', "=", 1],
            ['synchronously_send_time', "<=", time() + 3600],
        ])->select("id")->lockForUpdate()->get();
        foreach ($synchronouslyManagementTaskList as $synchronouslyManagementTask) {
            $Host = env("OPEN_PLATFROM_URL");

            $httpClient = (new ClientFactory(ApplicationContext::getContainer()))->create();

            $params = array(
                'id' => $synchronouslyManagementTask['id'],
            );
            $resp = $httpClient->post($Host . "/yxc/sync/getSyncVirtualBillCount", ['form_params' => $params]);

            if ($resp->getStatusCode() == 200) {
                $rawResp = (string)$resp->getBody();

                $resp = @json_decode($rawResp, true);

                if ($resp['code'] == 1) {
                    $update_data = array(
                        'synchronously_status' => $resp['data']['synchronously_status'],
                        'synchronously_info' => $resp['data']['synchronously_info'],
                        'synchronously_time' => $resp['data']['synchronously_time']
                    );
                    SynchronouslyManagementTaskModel::query()->where([
                        ["id", "=", $resp['data']['id']]
                    ])->update($update_data);
                }
            }
        }
        Log::getCrontabClient()->info("SyncTaskCallBack执行完毕");
    }
}