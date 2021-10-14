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
     */
    public function getIndustryTopnKpi(
        $where = '',
        $params = [],
        $limit = '',
        $exchangeCode = '1'
    ) {
        $category_level = intval($params['category_level'] ?? 1);//1-一级类目 2-二级类目 3-三级类目
        $product_category_name_1 = trim($params['product_category_name_1'] ?? '');//一级类目名称
        $product_category_name_2 = trim($params['product_category_name_2'] ?? '');//二级类目名称
        $product_category_name_3 = trim($params['product_category_name_3'] ?? '');//三级类目名称
        $is_count = intval($params['is_count'] ?? 0);// 总计
        $son = $params['son'] ?? [];//子级类目
        $tab_type = intval($params['tab_type'] ?? 1);//1-取本级 2-取子级
        $site_id = intval($params['site_id'] ?? 0);//站点id
        $count_periods = intval($params['count_periods'] ?? 1);//1-按日 2-按月
        $category_level = $tab_type == 2 ? $category_level + 1 : $category_level;
        $category_name_str = !empty($son) ? implode("','",array_values(array_column($son,'category_name'))) : [];
        $fields = $this->getIndustryFields($params);
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
            $group = "report.product_category_name_1,report.site_id";
            $fields['product_category_name'] = "report.product_category_name_1";
        }elseif($category_level == 2){
            if($count_periods == 1){
                $table = "{$this->table_dws_idm_category02_topn_kpi} AS report" ;
            }else{
                $table = "{$this->table_dws_arkdata_category02_month} AS report" ;
            }
            if($tab_type == 2){
                $where .= sprintf(
                    "%s report.product_category_name_1='%s' AND %s report.product_category_name_2 IN ('%s') AND report.site_id=%d",
                    $where ? ' AND' : '',
                    trim($product_category_name_1),
                    trim($category_name_str),
                    $site_id
                );
            }else {
                $where .= sprintf(
                    "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND report.site_id=%d",
                    $where ? ' AND' : '',
                    trim($product_category_name_1),
                    trim($product_category_name_2),
                    $site_id
                );
            }
            $group = "report.product_category_name_2,report.site_id";
            $fields['product_category_name'] = "report.product_category_name_2";
        }else{
            if($count_periods == 1){
                $table = "{$this->table_dws_idm_category03_topn_kpi} AS report" ;
            }else{
                $table = "{$this->table_dws_arkdata_category03_month} AS report" ;
            }
            if($tab_type == 2){
                $where .= sprintf(
                    "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND %s report.product_category_name_3 IN ('%s') AND report.site_id=%d",
                    $where ? ' AND' : '',
                    trim($product_category_name_1),
                    trim($product_category_name_2),
                    trim($category_name_str),
                    $site_id
                );
            }else {
                $where .= sprintf(
                    "%s report.product_category_name_1='%s' AND report.product_category_name_2='%s' AND report.product_category_name_3='%s' AND report.site_id=%d",
                    $where ? ' AND' : '',
                    trim($product_category_name_1),
                    trim($product_category_name_2),
                    trim($product_category_name_3),
                    $site_id
                );
            }
            $group = "report.product_category_name_3,report.site_id";
            $fields['product_category_name'] = "report.product_category_name_3";
        }

        $orderby = "";
        if(empty($is_count)){
            $group .= $count_periods == 2 ? ",report.year_to_date" : ",report.dt";
            $orderby = $count_periods == 2 ? "report.year_to_date ASC" : "report.dt ASC";
        }
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

        if(!empty($params['compare_data'])){
            $compareData = $this->getCompareDatas($params , $exchangeCode ) ;
        }else{
            $compareData = array();
        }

        $count = $this->count($where,$table,$group,'','',false ,null,300,false ,$compareData);
        $lists = $this->select($where, $field_data, $table, $limit,$orderby,$group, false , null, 300, false,$compareData);
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
        $is_count = intval($datas['is_count'] ?? 0);// 总计
        if($countPeriods == 1){
            $time_diff = strtotime($datas['search_start_time']) - strtotime($datas['search_end_time']);
            $time_diff = $time_diff / 86400;
        }else{
            $time_diff = $this->getMonthNum($datas['search_start_time'],$datas['search_end_time']);
        }
        $time_diff = $time_diff == 0 ? 1 : $time_diff;
        $periodsDay = $periodsDay < 10 ? str_pad($periodsDay,2,"0",STR_PAD_LEFT) : $periodsDay;

        $fields['time'] = $countPeriods == 2 ? "MAX(report.year_to_date)" : "MAX(report.dt)";

        $topNs = explode(',',$topNs);
        $filed_prefix = $is_count ? "SUM(" : "MAX(";
        $filed_suffix = $is_count ? ($data_type == 2 ? ") / {$time_diff}" : ")") : ")";
        foreach ($topNs as $top){
            if (in_array('sale_sales_volume', $targets)) {  // 销量
                if($countPeriods == 1){
                    if($data_type == 1){
                        $fields['sale_sales_volume_'.$top] = "report.sales_volume_{$periodsDay}day_critical_top{$top}";
                    }elseif($data_type == 2){
                        $fields['sale_sales_volume_'.$top] = $filed_prefix . "report.sales_volume_{$periodsDay}day_top{$top}_avg" . $filed_suffix;
                    }else{
                        $fields['sale_sales_volume_'.$top] = $filed_prefix . "report.sales_volume_{$periodsDay}day_top{$top} * {$this->coefficient}" . $filed_suffix;
                    }
                }else{
                    if($data_type == 2){
                        $fields['sale_sales_volume_'.$top] = $filed_prefix . "report.sales_volume_top{$top}_avg_months" . $filed_suffix;
                    }else{
                        $fields['sale_sales_volume_'.$top] = $filed_prefix . "report.sales_volume_top{$top}_months * {$this->coefficient}" . $filed_suffix;
                    }
                }
            }
            if (in_array('sale_sales_quota', $targets)) {  // 销售额
                if($countPeriods == 1) {
                    if ($data_type == 1) {
                        $fields['sale_sales_quota_'.$top] = "report.sales_quota_{$periodsDay}day_critical_top{$top} * {:RATE}";
                    } elseif ($data_type == 2) {
                        $fields['sale_sales_quota_'.$top] = $filed_prefix . "report.sales_quota_{$periodsDay}day_top{$top}_avg" . $filed_suffix ." * {:RATE}";
                    } else {
                        $fields['sale_sales_quota_'.$top] = $filed_prefix . "report.sales_quota_{$periodsDay}day_top{$top} * {$this->coefficient}" . $filed_suffix ." * {:RATE}";
                    }
                }else{
                    if($data_type == 2){
                        $fields['sale_sales_quota_'.$top] = $filed_prefix . "report.sales_quota_top{$top}_avg_months" . $filed_suffix ." * {:RATE}";
                    }else{
                        $fields['sale_sales_quota_'.$top] = $filed_prefix . "report.sales_quota_top{$top}_months * {$this->coefficient}" . $filed_suffix ." * {:RATE}";
                    }
                }
            }
        }
        return $fields;
    }

    /**
     * 计算两个日期相差几个月
     */
    public function getMonthNum($date1,$date2){
        $date1_stamp=strtotime($date1);
        $date2_stamp=strtotime($date2);
        list($date_1['y'],$date_1['m'])=explode("-",date('Y-m',$date1_stamp));
        list($date_2['y'],$date_2['m'])=explode("-",date('Y-m',$date2_stamp));
        return abs(($date_2['y']-$date_1['y'])*12 +$date_2['m']-$date_1['m']);
    }

    public function getCompareDatas($datas ,$exchangeCode = 1){
        if(empty($datas['compare_data'])){
            return [] ;
        }

        $count_periods = intval($datas['count_periods'] ?? 1);//1-按日 2-按月
        $newDatas = $datas ;
        $on_key = 1 ;

        $compare_on = [];
        foreach($datas['compare_data'] as $ck => $compare_data)   {
            $datas['compare_data'][$ck]['fields'] = $this->getIndustryFields($newDatas) ;

            //拼接对比表条件 及连表 ON 条件
            if($count_periods == 2){
                $compareWhere = " report.year_to_date>= '{$compare_data['compare_start_time']}' and report.year_to_date<= '{$compare_data['compare_end_time']}'";
            }else{
                $compareWhere = " report.dt>= '{$compare_data['compare_start_time']}' and report.dt<= '{$compare_data['compare_end_time']}'";
            }

            $datas['compare_data'][$ck]['compare_where'] = $compareWhere ;
            $compare_on_arr = [] ;
            foreach($compare_on as $con){
                $compare_on_arr[] = 'origin_table.'.$con . ' = compare_table'.$on_key.'.compare'.$on_key.'_'.$con ;
            }
            $on_key++ ;
            $datas['compare_data'][$ck]['on'] = implode(' AND ' , $compare_on_arr) ;
        }

        //处理对比数据 - 获取对比数据需要查询的字段
        foreach($datas['compare_data'] as $ck2 => $compare_data)   {
            if(!empty($compare_data['fields'])){
                $compare_fields_arr = array() ;
                $renames = empty($compare_data['rename']) ? [] : explode(',',$compare_data['rename']) ;
                $renameArr = [] ;
                $compare_target = explode(',' , $compare_data['target']) ;
                if(!empty($renames)){
                    foreach($compare_target as $ctk =>$ct){
                        $renameArr[$ct] = empty($renames[$ctk]) ? false : $renames[$ctk] ;
                    }
                }
                $i = 1 ;
                foreach($compare_data['fields'] as $compare_field_name => $compare_field){
                    if(in_array($compare_field_name , $compare_target)){
                        $compare_field_name2 = empty($renameArr[$compare_field_name]) ? ('compare'.($ck2+1).'_'.$compare_field_name) : $renameArr[$compare_field_name] ;
                        $compare_fields_arr[] = $compare_field . ' AS "' . $compare_field_name2 . '"';
                        $i++ ;
                    }else{
                        $compare_fields_arr[] = $compare_field . ' AS "compare'.($ck2+1).'_'.$compare_field_name . '"';
                    }
                }
                $compare_field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $compare_fields_arr));//去除
                $datas['compare_data'][$ck2]['field_data'] =  $compare_field_data ;
                unset($datas['compare_data'][$ck2]['fields']) ;
            }
        }

        //最终获取自定义字段拼接
        if(!empty($datas['compare_data'][0]['custom_target_set'])){
            $datas['compare_data'][0]['custom_target'] = [];
            $custom_set_order = $custom_set_where = [];
            foreach($datas['compare_data'][0]['custom_target_set'] as $custom_target_item){
                $compare_table = isset($custom_target_item['compare_table']) ? explode(',',$custom_target_item['compare_table']) : [];
                $field_arr = [];
                if($compare_table){
                    //fields
                    foreach($compare_table as $table_key => $table_item){
                        $avg = (!empty($table_item['avg']) && (int)$table_item['avg'] > 1) ? " * 1.0000 / {$table_item['avg']}" : '';//取的字段表
                        $table_tmp = $table_item['table'] == '-1' ? $table_item['table'] : (int)$table_item['table'] + 1;
                        $table_str = $table_tmp == '-1' ? 'origin_table.' : "compare_table{$table_tmp}.";//取的字段表
                        $target_prefix = $table_tmp == '-1' ? '' : "compare{$table_tmp}_";//取的字段表
                        $field_arr[$table_key] = '(' . $table_str . $target_prefix . $custom_target_item['target'] . '_' . $custom_target_item['topn'] . $avg . ')';
                    }
                    if (!empty($custom_target_item['type']) && $custom_target_item['type'] == 2) {
                        $field_str = "( CASE WHEN COALESCE ( {$field_arr[0]}, 0 ) = COALESCE ( {$field_arr[1]}, 0 ) THEN 0 ELSE ( CASE WHEN COALESCE ( {$field_arr[0]}, 0 ) = 0 THEN -1 ELSE ( CASE WHEN COALESCE ( {$field_arr[1]}, 0 ) = 0 THEN 1 ELSE ( {$field_arr[0]} - {$field_arr[1]} ) * 1.0000 / nullif({$field_arr[1]},0) END ) END ) END )";
                    } else {
                        if (!empty($field_arr[1])) {
                            $field_str = "( CASE WHEN COALESCE ( {$field_arr[0]}, 0 ) = COALESCE ( {$field_arr[1]}, 0 ) THEN 0 ELSE ( CASE WHEN COALESCE ( {$field_arr[0]}, 0 ) = 0 THEN -{$field_arr[1]} ELSE ( CASE WHEN COALESCE ( {$field_arr[1]}, 0 ) = 0 THEN {$field_arr[0]} ELSE ( {$field_arr[0]} - {$field_arr[1]} ) END ) END ) END )";
                        } else {
                            $field_str = "{$field_arr[0]}";
                        }
                    }
                    $datas['compare_data'][0]['custom_target'][] = !empty($custom_target_item['rename']) ? $field_str . " as {$custom_target_item['rename']}" : $field_str;

                    //where
                    if(!empty($custom_target_item['formula']) && !empty($custom_target_item['value'])){
                        if (strpos($custom_target_item['value'], '%') !== false) {
                            $custom_target_item['value'] = round((float)$custom_target_item['value'] / 100, 4);
                        }
                        $custom_set_where[] = '(' .  $field_str . ') ' . $custom_target_item['formula'] . $custom_target_item['value'];
                    }

                    //order by
                    if(!empty($custom_target_item['sort'])){
                        $custom_set_order[] = $field_str . " {$custom_target_item['sort']}";
                    }
                }
            }
            $datas['compare_data'][0]['where'] = !empty($custom_set_where) ? implode(' AND ',$custom_set_where) : [];
            $datas['compare_data'][0]['order'] = !empty($custom_set_order) ? implode(',',$custom_set_order) : [];
        }

        return $datas['compare_data'] ;
    }



}
