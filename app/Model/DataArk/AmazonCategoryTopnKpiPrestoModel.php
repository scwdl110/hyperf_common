<?php

namespace App\Model\DataArk;

use App\Lib\Redis;
use App\Model\AbstractPrestoModel;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use App\Service\CommonService;
use Hyperf\Di\Annotation\Inject;
use function App\getUserInfo;

class AmazonCategoryTopnKpiPrestoModel extends AbstractPrestoModel
{
    /**
     * @Inject()
     * @var CommonService
     */
    protected $commonService;

    protected $table = 'table_amazon_idm_category01_topn_kpi';

    /**
     * 获取行业数据对比数据
     * @param string $where
     * @param array $params
     * @param string $limit
     * @param array $currencyInfo
     * @param string $exchangeCode
     * @param int $userId
     * @return array
     * @author: 林志敏
     */
    public function getIndustryTopnKpi(
        $where = '',
        $params = [],
        $limit = '',
        array $currencyInfo = [],
        $exchangeCode = '1',
        int $userId = 0
    ) {
        $category_level = intval($params['category_level'] ?? 1);//1-一级类目 3-三级类目
        $product_category_name_1 = trim($params['product_category_name_1'] ?? '');//一级类目名称
        $product_category_name_2 = trim($params['product_category_name_2'] ?? '');//二级类目名称
        $product_category_name_3 = trim($params['product_category_name_3'] ?? '');//三级类目名称
        $site_id = intval($params['site_id'] ?? 0);//类目名称
        if($category_level == 1){
            $table = "{$this->table_dws_idm_category01_topn_kpi} AS report" ;
            $where .= sprintf(
                "%s report.product_category_name_1='%s' AND report.site_id=%d",
                $where ? ' AND' : '',
                htmlspecialchars(trim($product_category_name_1), ENT_NOQUOTES),
                $site_id
            );
        }else{
            $table = "{$this->table_dws_idm_category03_topn_kpi} AS report" ;
            $where .= sprintf(
                "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND report.product_category_name_3='%s' AND report.site_id=%d",
                $where ? ' AND' : '',
                htmlspecialchars(trim($product_category_name_1), ENT_NOQUOTES),
                htmlspecialchars(trim($product_category_name_2), ENT_NOQUOTES),
                htmlspecialchars(trim($product_category_name_3), ENT_NOQUOTES),
                $site_id
            );
        }

        $fields = $this->getIndustryFields($params);
        if (empty($fields)) {
            return [];
        }

        $rt = array();
        $fields_arr = array();
        foreach ($fields as $field_name => $field) {
            $fields_arr[] = $field . ' AS "' . $field_name . '"';
        }

        //行业数据金额存的是人民币
        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));//去除presto除法把数据只保留4位导致精度异常，如1/0.1288 = 7.7639751... presto=7.7640

        $where = str_replace("{:RATE}", $exchangeCode, $where ?? '');
        $orderby = "report.dt ASC";
        $count = $this->count($where, $table);;
        $lists = $this->select($where, $field_data, $table, $limit,$orderby);
        $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
        $logger->info('getListByGoods Request', [$this->getLastSql()]);

        $rt['lists'] = empty($lists) ? array() : $lists;
        $rt['count'] = intval($count);
        return $rt;
    }

    private function getIndustryFields($datas = array())
    {
        $fields = array();
        $targets = explode(',', $datas['target']);
        $topN = intval($datas['topn'] ?? 30);
        $topNUp = $topN + 5;
        $topNDown = $topN - 5;
        $periodsDay = intval($datas['periods_day'] ?? 1);
        $periodsDay = $periodsDay < 10 ? str_pad($periodsDay,2,"0",STR_PAD_LEFT) : $periodsDay;

        $fields['time'] = "report.dt";

        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors_up'] = "report.user_sessions_{$periodsDay}day_top{$topNUp}_avg";
            $fields['goods_visitors_down'] = "report.user_sessions_{$periodsDay}day_top{$topNDown}_avg";
        }
        if (in_array('sale_sales_volume', $targets)) {  // 销量
            $fields['sale_sales_volume_up'] = "report.sales_volume_{$periodsDay}day_top{$topNUp}_avg";
            $fields['sale_sales_volume_down'] = "report.sales_volume_{$periodsDay}day_top{$topNDown}_avg";
        }
        if (in_array('sale_sales_quota', $targets)) {  // 销售额
            $fields['sale_sales_quota_up'] = "report.sales_quota_{$periodsDay}day_top{$topNUp}_avg * {:RATE}";
            $fields['sale_sales_quota_down'] = "report.sales_quota_{$periodsDay}day_top{$topNDown}_avg * {:RATE}";
        }
        if (in_array('goods_conversion_rate', $targets)) { //转化率
            $fields['goods_conversion_rate_up'] = "report.user_sessions_{$periodsDay}day_top{$topNUp}_cr";
            $fields['goods_conversion_rate_down'] = "report.user_sessions_{$periodsDay}day_top{$topNDown}_cr";
        }
        return $fields;
    }
}
