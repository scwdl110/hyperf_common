<?php
namespace App\Crontab;

use App\CrontabService\SbvCrontabService;
use App\Model\CrontabModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Crontab\Annotation\Crontab;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\WaitGroup;

//singleton=true, onOneServer=true, mutexPool="default", mutexExpires=600,
/**
 * @Crontab(name="Sbv", rule="* * * * *", callback="execute", memo="这是一个crontab表生成的task任务")
 */
class SbvCrontab
{
    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    public function execute()
    {
        // 并发数
        $concurrencyNum = $this->config->get("crontab.sbv_concurrency_num");
        $where = [
            ['type', '=', '0'],
            ['is_success', '=', '0'],
        ];
        $crontabModel = CrontabModel::query()->where($where)->limit($concurrencyNum)->select("id", "json_info")->get();
        $crontabModelCount = count($crontabModel);

        //无数据
        if($crontabModelCount <= 0){
            return 0;
        }

        //并发数控制
        $wg = new WaitGroup();
        $wg->add($crontabModelCount);
        foreach ($crontabModel as $k=>$v){
            go(function () use ($wg, $v) {
                $sbv = new SbvCrontabService();
                $sbv->execute($v['id'], $v['json_info'], 0);
                $wg->done();
            });
        }

        $wg->wait();

    }


}