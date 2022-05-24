<?php

namespace App\Model\DataArk;

use App\Lib\Redis;
use App\Model\AbstractPrestoModel;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use App\Service\CommonService;
use Hyperf\Di\Annotation\Inject;
use function App\getUserInfo;

class GoodsDimReportPrestoModel extends AbstractPrestoModel
{
    /**
     * @Inject()
     * @var CommonService
     */
    protected $commonService;

    protected $table = 'table_goods_dim_report';

    public function getCateGory($where = '' , $field = '*' ,$group = ''){
        $lists = $this->select($where, $field,'','','',$group);
        if(empty($lists)){
            $lists = array() ;
        }
        return $lists ;
    }


}
