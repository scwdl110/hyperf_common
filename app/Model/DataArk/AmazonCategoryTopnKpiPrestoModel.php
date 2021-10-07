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

    protected $coefficient = 50;//系数

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
        $category_level = intval($params['category_level'] ?? 1);//1-一级类目 2-二级类目 3-三级类目
        $product_category_name_1 = trim($params['product_category_name_1'] ?? '');//一级类目名称
        $product_category_name_2 = trim($params['product_category_name_2'] ?? '');//二级类目名称
        $product_category_name_3 = trim($params['product_category_name_3'] ?? '');//三级类目名称
        $site_id = intval($params['site_id'] ?? 0);//站点id
        $count_periods = intval($params['count_periods'] ?? 1);//1-按日 2-按月
        if($category_level == 1){
            if($count_periods == 1){
                $table = "{$this->table_dws_idm_category01_topn_kpi} AS report" ;
            }else{
                $table = "{$this->table_dws_arkdata_category01_month} AS report" ;
            }
            $where .= sprintf(
                "%s report.product_category_name_1='%s' AND report.site_id=%d",
                $where ? ' AND' : '',
                trim($product_category_name_1),
                $site_id
            );
        }elseif($category_level == 2){
            if($count_periods == 1){
                $table = "{$this->table_dws_idm_category02_topn_kpi} AS report" ;
            }else{
                $table = "{$this->table_dws_arkdata_category02_month} AS report" ;
            }
            $where .= sprintf(
                "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND report.site_id=%d",
                $where ? ' AND' : '',
                trim($product_category_name_1),
                trim($product_category_name_2),
                $site_id
            );
        }else{
            if($count_periods == 1){
                $table = "{$this->table_dws_idm_category03_topn_kpi} AS report" ;
            }else{
                $table = "{$this->table_dws_arkdata_category03_month} AS report" ;
            }
            $where .= sprintf(
                "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND report.product_category_name_3='%s' AND report.site_id=%d",
                $where ? ' AND' : '',
                trim($product_category_name_1),
                trim($product_category_name_2),
                trim($product_category_name_3),
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
        $count = $this->count($where, $table);
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
        $topNs = $datas['topns'] ?? '25';
        $countPeriods = intval($datas['count_periods'] ?? 1);//1-按日 2-按月
        $periodsDay = intval($datas['periods_day'] ?? 1);//1-当天 7-过去7天 30-过去30天
        $data_type = intval($datas['data_type'] ?? 1);// 1-临界点 2-平均 3-指数
        $periodsDay = $periodsDay < 10 ? str_pad($periodsDay,2,"0",STR_PAD_LEFT) : $periodsDay;

        $fields['time'] = "report.dt";

        $topNs = explode(',',$topNs);
        foreach ($topNs as $top){
            if (in_array('sale_sales_volume', $targets)) {  // 销量
                if($countPeriods == 1){
                    if($data_type == 1){
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_{$periodsDay}day_critical_top{$top}";
                    }elseif($data_type == 2){
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_{$periodsDay}day_top{$top}_avg";
                    }else{
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_{$periodsDay}day_top{$top} * {$this->coefficient}";
                    }
                }else{
                    if($data_type == 2){
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_top{$top}_avg_months";
                    }else{
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_top{$top}_months * {$this->coefficient}";
                    }
                }
            }
            if (in_array('sale_sales_quota', $targets)) {  // 销售额
                if($countPeriods == 1) {
                    if ($data_type == 1) {
                        $fields['sale_sales_quota_'.$top] = "report.sales_quota_{$periodsDay}day_critical_top{$top} * {:RATE}";
                    } elseif ($data_type == 2) {
                        $fields['sale_sales_quota_'.$top] = "report.sales_quota_{$periodsDay}day_top{$top}_avg * {:RATE}";
                    } else {
                        $fields['sale_sales_quota_'.$top] = "report.sales_quota_{$periodsDay}day_top{$top} * {$this->coefficient} * {:RATE}";
                    }
                }else{
                    if($data_type == 2){
                        $fields['sale_sales_volume_'.$top] = "report.sales_quota_top{$top}_avg_months";
                    }else{
                        $fields['sale_sales_volume_'.$top] = "report.sales_quota_top{$top}_months * {$this->coefficient}";
                    }
                }
            }
        }
        return $fields;
    }





}
