<?php

namespace App\Controller;

use Hyperf\DB\DB;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;

class DataArkController extends AbstractController
{
    const SEARCH_TYPE_PRESTO = 0;
    const SEARCH_TYPE_ES = 1;

    protected $user = [];

    public function __construct()
    {
        parent::__construct();

        $this->user = $this->request->getAttribute('userInfo');
    }

    protected function init($type = 1)
    {
        $req = $this->request->all();
        $searchVal = trim(strval($req['searchVal'] ?? ''));
        $searchType = intval($req['searchType'] ?? self::SEARCH_TYPE_PRESTO);
        $params = $req['params'] ?? [];
        $page = intval($req['page'] ?? 1);
        $limit = intval($req['rows'] ?? 100);
        $sort = trim(strval($req['sort'] ?? ''));
        $order = trim(strval($req['order'] ?? ''));
        $channelIds = $req['channelIds'] ?? [];
        $countTip = intval($req['countTip'] ?? 0);
        $currencyInfo = $req['currencyInfo'] ?? [];
        $exchangeCode = $req['exchangeCode'] ?? '1';
        $timeLine = $req['timeLine'] ?? [];
        $deparmentData = $req['deparmentData'] ?? [];
        $offset = ($page - 1) * $limit;

        $where = '';
        if (count($channelIds) > 1) {
            $where = "report.user_id={$this->user['user_id']} AND report.channel_id IN (" . implode(',' , $channelIds) .')';
        } else {
            $where = "report.user_id={$this->user['user_id']} AND report.channel_id={$channelIds[0]}";
        }

        if (!empty($searchVal)) {
            $likes = [
                'isku' => 'report.isku',
                'tags' => 'gtags.tag_name',
                'sku' => 'report.goods_sku',
                'asin' => 'report.goods_asin',
                'group' => 'report.goods_group_name',
                'parent_asin' => 'report.goods_parent_asin',
                'operators' => 'report.operation_user_admin_name',
                'class1' => 'report.goods_product_category_name_1',
            ];

            $eqs = [
                'site_id' => 'report.site_id',
                'site_group' => 'report.area_id',
                'channel_id' => 'report.channel_id',
            ];

            if (isset($likes[$params['count_dimension']])) {
                $where .= " AND {$likes[$params['count_dimension']]} LIKE '%{$searchVal}%' ";
            } elseif (isset($eqs[$params['count_dimension']])) {
                $where .= " AND {$eqs[$params['count_dimension']]}=" . intval($searchVal);
            }
        }

        if ($params['show_type'] == 2 && $params['limit_num'] > 0 && $params['count_periods'] == 0) {
            $offset = 0;
            $limit = (int)$params['limit_num'] ;
        }

        if ((int)$params['time_type'] === 99) {
            $where .= sprintf(
                '%s report.create_time>=%d and report.create_time<=%d',
                $where ? ' AND' : '',
                (int)$params['search_start_time'],
                (int)$params['search_end_time']
            );
        } else {
            $ors = [];
            foreach ($this->getSiteLocalTime(array_keys(\App\getAmazonSitesConfig()), $params['time_type']) as $times) {
                $ors[] = sprintf(
                    '(report.site_id in (%s) and report.create_time>=%d and report.create_time<=%d)',
                    $times['site_id'],
                    (int)$times['start'],
                    (int)$times['end']
                );
            }

            if (empty($ors)) {
                return [];
            }
            $ors = join(' OR ', $ors);
            $where .= $where ? " AND ({$ors})" : "({$ors})";
        }

        $method = $type === 1 ? 'getListByUnGoods' : 'getListByGoods';

        $limit = ($offset > 0 ? " OFFSET {$offset}" : '') . " LIMIT {$limit}";
        return $this->{$method}($where, $params, $limit, $sort, $order, 0, $channelIds, $currencyInfo, $exchangeCode, $timeLine, $deparmentData, $searchType);
    }

    public function getUnGoodsDatas()
    {
        return $this->init(1);
    }

    public function getGoodsDatas()
    {
        return $this->init(2);
    }

    protected function getSiteLocalTime(array $siteIds, int $timeType = 99): array
    {
        $result = [[
            'end' => 0,
            'start' => 0,
            'site_id' => '0',
        ]];

        if (!empty($siteIds)) {
            $siteIdsStr = implode(',', $siteIds);
            if ($timeType === 99) {
                $result[0]['site_id'] = $siteIdsStr;
            } else {
                $timeDatas = [];
                $siteTimes = \App\getStartAndEndTimeAllSite($timeType);
                foreach ($siteIds as $siteId) {
                    $key = "{$siteTimes[$siteId]['start']}_{$siteTimes[$siteId]['end']}";

                    if (empty($timeDatas[$key])) {
                        $timeDatas[$key] = [
                            'site_id' => (string)$siteId,
                            'end' => $siteTimes[$siteId]['end'],
                            'start' => $siteTimes[$siteId]['start'],
                        ];
                    } else {
                        $timeDatas[$key]['site_id'] = "{$timeDatas[$key]['site_id']},{$siteId}";
                    }
                }

                $result = array_values($timeDatas);
            }
        }

        return $result;
    }

    /**
     * 获取非商品维度统计列表(新增统计维度完成)
     * @param string $where
     * @param array $params
     * @param string $limit
     * @param string $sort
     * @param string $order
     * @param int $count_tip
     * @param array $channel_arr
     * @return array
     * @author: LWZ
     */
    protected function getListByUnGoods(
        $where = '',
        $params = [],
        $limit = 0,
        $sort = '',
        $order = '',
        $count_tip = 0,
        $channel_arr = [],
        $currencyInfo = [],
        $exchangeCode = '1',
        $timeLine,
        $deparmentData,
        $searchType
    ) {
        $fields = [];
        //没有按周期统计 ， 按指标展示
        if ($params['show_type'] == 2) {
            $fields = $this->getUnGoodsFields($params);
        } else {
            $fields = $this->getUnGoodsTimeFields($params, $timeLine);
        }

        if (empty($fields)) {
            return [];
        }

        if(($params['count_periods'] == 0 || $params['count_periods'] == 1) && $params['cost_count_type'] != 2){ //按天或无统计周期
            if ($searchType === self::SEARCH_TYPE_ES) {
                $table = "f_dw_channel_day_report_{$this->user['dbhost']} AS report";
            } else {
                $table = "dwd.dwd_dataark_f_dw_channel_day_report_{$this->user['dbhost']} AS report";
            }
        }else if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){  //按周
            $table = "dwd.dwd_dataark_f_dw_channel_week_report_{$this->user['dbhost']} AS report" ;
        }else if($params['count_periods'] == 3 || $params['count_periods'] == 4 || $params['count_periods'] == 5 ){
            $table = "dwd.dwd_dataark_f_dw_channel_month_report_{$this->user['dbhost']} AS report" ;
        }else if($params['cost_count_type'] == 2 ){
            $table = "dwd.dwd_dataark_f_dw_channel_month_report_{$this->user['dbhost']} AS report" ;
        } else {
            if ($searchType === self::SEARCH_TYPE_ES) {
                $table = "f_dw_channel_day_report_{$this->user['dbhost']} AS report";
            } else {
                $table = "dwd.dwd_dataark_f_dw_channel_day_report_{$this->user['dbhost']} AS report";
            }
        }

        $where .= " AND report.user_id_mod = '" . ($params['user_id'] % 20) . "'";

        //部门维度统计
        if ($params['count_dimension'] == 'department') {
            $table .= " LEFT JOIN dim.dim_dataark_b_department_channel as dc ON dc.user_id = report.user_id AND dc.channel_id = report.channel_id  LEFT JOIN ods.ods_dataark_b_user_department as ud ON ud.id = dc.user_department_id ";
            $where .= " AND ud.status < 3";
            $admin_info = DB::fetch(
                'select is_master,is_responsible,user_department_id from erp_base.b_user_admin where id=? and user_id=?',
                [$this->user['admin_id'], $this->user['user_id']]
            );
            if($admin_info['is_master'] != 1){
                if($admin_info['is_responsible'] == 0 ){ //非部门负责人
                    $rt['lists'] = array();
                    $rt['count'] = 0;
                    return $rt;
                }else{
                    $ids = $this->getMyAllDepartmentIds($deparmentData, $admin_info['user_department_id']);
                    if(empty($ids)){
                        $rt['lists'] = array();
                        $rt['count'] = 0;
                        return $rt;
                    }else{
                        $where .= " AND dc.user_department_id IN (".implode(',' ,$ids ).")" ;
                    }
                }
            }
            $level_where = array() ;
            if($params['is_check_department1'] == 1){
                $level_where[] = 1 ;
            }
            if($params['is_check_department2'] == 1){
                $level_where[] = 2 ;
            }
            if($params['is_check_department3'] == 1){
                $level_where[] = 3 ;
            }
            if(count($level_where) < 3){
                $where .= " AND dc.level IN (".implode(',' ,$level_where ).")" ;
            }
        }else if($params['count_dimension'] == 'admin_id'){
            $table .= " LEFT JOIN dim.dim_dataark_b_user_channel as uc ON uc.user_id = report.user_id AND uc.channel_id = report.channel_id ";
            $where .= " AND uc.status = 1 AND uc.is_master = 0 ";
        }

        $orderby = '';
        if( !empty($params['sort_target']) && !empty($fields[$params['sort_target']]) && !empty($params['sort_order']) ){
            $orderby = "(({$fields[$params['sort_target']]}) IS NULL), ({$fields[$params['sort_target']]}) {$params['sort_order']}";
        }

        if (!empty($order) && !empty($sort) && !empty($fields[$sort]) && $params['limit_num'] == 0 ) {
            $orderby =  "(({$fields[$sort]}) IS NULL), ({$fields[$sort]}) {$order}";
        }

        $rt = $fields_arr = [];
        foreach ($fields as $field_name => $field) {
            $fields_arr[] = $field . ' AS "' . $field_name . '"';
        }

        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));

        if ($params['currency_code'] != 'ORIGIN') {
            if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id ";
            }
        }

        $having = '';
        $where_detail = is_array($params['where_detail']) ? $params['where_detail'] : json_decode($params['where_detail'], true);
        if (empty($where_detail)) {
            $where_detail = [];
        }
        if ($params['count_dimension'] == 'channel_id') {
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if($params['count_periods'] == '4'){ //按季度
                    $group = 'report.channel_id , report.myear , report.mquarter ';
                    $orderby = 'report.channel_id , report.myear , report.mquarter ';
                }else if($params['count_periods'] == '5') { //年
                    $group = 'report.channel_id , report.myear' ;
                    $orderby = 'report.channel_id , report.myear ';
                }else{
                    $group = 'report.channel_id_group ';
                    $orderby = 'report.channel_id_group ';
                }
            }else{
                $group = 'report.channel_id ';

                $orderby = empty($orderby) ? 'report.channel_id ' : ($orderby . ' , report.channel_id ');
            }
        } else if ($params['count_dimension'] == 'site_id') {
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if($params['count_periods'] == '4'){ //按季度
                    $group = 'report.site_id , report.myear , report.mquarter ';
                    $orderby = 'report.site_id , report.myear , report.mquarter ';
                }else if($params['count_periods'] == '5') { //年
                    $group = 'report.site_id , report.myear' ;
                    $orderby = 'report.site_id , report.myear ';
                }else {
                    $group = 'report.site_id_group ';
                    $orderby = 'report.site_id_group ';
                }
            }else{
                $group = 'report.site_id ';
                $orderby = empty($orderby) ? 'report.site_id ' : ($orderby . ' , report.site_id ');
            }
        } else if ($params['count_dimension'] == 'site_group') {
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if($params['count_periods'] == '4'){ //按季度
                    $group = 'report.area_id , report.myear , report.mquarter ';
                    $orderby = 'report.area_id , report.myear , report.mquarter ';
                }else if($params['count_periods'] == '5') { //年
                    $group = 'report.area_id , report.myear' ;
                    $orderby = 'report.area_id , report.myear ';
                }else {
                    $orderby = 'report.area_id_group ';
                    $group = 'report.area_id_group ';
                }
            }else{
                $group = 'report.area_id ';
                $orderby = empty($orderby) ? 'report.area_id ' : ($orderby . ' , report.area_id ');
            }
        } else if ($params['count_dimension'] == 'department') {
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if($params['count_periods'] == '1'){ //按天
                    $group = 'dc.user_department_id , report.myear , report.mmonth , report.mday ';
                    $orderby = 'max(dc.level) , dc.user_department_id , report.myear , report.mmonth , report.mday ';
                }else if($params['count_periods'] == '2') { //按周
                    $group = 'dc.user_department_id , report.mweekyear ,report.mweek ';
                    $orderby = 'max(dc.level) , dc.user_department_id , report.mweekyear ,report.mweek ';
                }else if ($params['count_periods'] == '3' ) { //按月
                    $group = 'dc.user_department_id , report.myear , report.mmonth';
                    $orderby = 'max(dc.level) , dc.user_department_id , report.myear , report.mmonth';
                }else if ($params['count_periods'] == '4' ) {  //按季
                    $group = 'dc.user_department_id , report.myear , report.mquarter';
                    $orderby = 'max(dc.level) , dc.user_department_id , report.myear , report.mquarter';
                }else if ($params['count_periods'] == '5' ) { //按年
                    $group = 'dc.user_department_id , report.myear';
                    $orderby = 'max(dc.level) , dc.user_department_id , report.myear';
                }
            }else{
                $group = 'dc.user_department_id ';
                $orderby = empty($orderby) ? 'max(dc.level) , dc.user_department_id ' : ($orderby . ' , dc.user_department_id ');
            }
            $where .= " AND dc.user_department_id > 0";
        }else if($params['count_dimension'] == 'admin_id'){
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if ($params['count_periods'] == '1'){ //按天
                    $group = 'uc.admin_id, report.myear , report.mmonth , report.mday ';
                    $orderby = 'uc.admin_id , report.myear , report.mmonth , report.mday ';
                } else if($params['count_periods'] == '2') { //按周
                    $group = 'uc.admin_id , report.mweekyear ,report.mweek ';
                    $orderby = 'uc.admin_id , report.mweekyear ,report.mweek ';
                } else if ($params['count_periods'] == '3' ) { //按月
                    $group = 'uc.admin_id , report.myear , report.mmonth';
                    $orderby = 'uc.admin_id , report.myear , report.mmonth';
                } else if ($params['count_periods'] == '4' ) {  //按季
                    $group = 'uc.admin_id , report.myear , report.mquarter';
                    $orderby = 'uc.admin_id , report.myear , report.mquarter';
                } else if ($params['count_periods'] == '5' ) { //按年
                    $group = 'uc.admin_id , report.myear';
                    $orderby = 'uc.admin_id , report.myear';
                }
            } else {
                $group = 'uc.admin_id ';
                $orderby = empty($orderby) ? 'uc.admin_id  ' : ($orderby . ' , uc.admin_id ');
            }
            $where .= " AND uc.admin_id > 0";
        }

        if (!empty($where_detail)) {
            $target_wheres = $where_detail['target'];
            if (!empty($target_wheres)) {
                foreach ($target_wheres as $target_where) {
                    $where_value = $target_where['value'];
                    if (strpos($where_value, '%') !== false) {
                        $where_value = round($where_value / 100, 4);
                    }
                    if (empty($having)) {
                        $having .= '(' . $fields[$target_where['key']] . ') ' . $target_where['formula'] . $where_value;
                    } else {
                        $having .= ' AND (' . $fields[$target_where['key']] . ') ' . $target_where['formula'] . $where_value;
                    }
                }
            }
        }

        if (!empty($having)) {
            $group .= " having " . $having;
        }

        $group = str_replace("{:RATE}", $exchangeCode, $group);
        $orderby = str_replace("{:RATE}", $exchangeCode, $orderby);
        $limit_num = 0 ;
        if($params['show_type'] == 2 && $params['limit_num'] > 0 ){
            $limit_num = $params['limit_num'] ;
        }
        if ($count_tip == 2) { //仅统计总条数
            $count = $this->getTotalNum($where, $table, $group);
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        } else if ($count_tip == 1) {  //仅仅统计列表
            if ($params['is_count'] == 1){
                $where = $this->getLimitWhere($where,$params,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table, $limit);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                if($params['show_type'] = 2 && ( !empty($fields['fba_goods_value']) || !empty($fields['fba_stock']) || !empty($fields['fba_need_replenish']) || !empty($fields['fba_predundancy_number']) )){
                    $lists = $this->getUnGoodsFbaData($lists , $fields , $params,$channel_arr, $currencyInfo, $exchangeCode) ;
                }
            }
        } else {  //统计列表和总条数
            if ($params['is_count'] == 1){
                $where = $this->getLimitWhere($where,$params,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table, $limit);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                if($params['show_type'] = 2 && ( !empty($fields['fba_goods_value']) || !empty($fields['fba_stock']) || !empty($fields['fba_need_replenish']) || !empty($fields['fba_predundancy_number']) )){
                    $lists = $this->getUnGoodsFbaData($lists , $fields , $params,$channel_arr, $currencyInfo, $exchangeCode) ;
                }
            }
            if (empty($lists)) {
                $count = 0;
            } else {
                $count = $this->getTotalNum($where, $table, $group);
                if($limit_num > 0 && $count > $limit_num){
                    $count = $limit_num ;
                }
            }
        }
        if(!empty($lists) && $params['show_type'] = 2 && $params['limit_num'] > 0 && !empty($order) && !empty($sort) && !empty($fields[$sort]) && !empty($fields[$params['sort_target']]) && !empty($params['sort_target']) && !empty($params['sort_order'])){
            //根据字段对数组$lists进行排列
            $sort_names = array_column($lists,$sort);
            $order2  =  $order == 'desc' ? \SORT_DESC : \SORT_ASC ;
            array_multisort($sort_names,$order2,$lists);
        }
        $rt['lists'] = empty($lists) ? [] : $lists;
        $rt['count'] = (int)$count;
        return $rt;
    }


    // 获取非商品维度指标字段（新增统计维度完成）
    private function getUnGoodsFields($datas)
    {
        $fields = [];
        $fields['user_id'] = 'max(report.user_id)';
        $fields['site_country_id'] = 'max(report.site_id)';

        if ($datas['count_dimension'] === 'channel_id') {
            $fields['site_id'] = 'max(report.site_id)';
            $fields['channel_id'] = 'max(report.channel_id)';
            $fields['operators'] = 'max(report.operation_user_admin_name)';
            $fields['operation_user_admin_id'] = 'max(report.channel_operation_user_admin_id)';
        } elseif ($datas['count_dimension'] === 'site_id') {
            $fields['site_id'] = 'max(report.site_id)';
        } elseif ($datas['count_dimension'] === 'site_group') {
            $fields['site_group'] = 'max(report.area_id)';
        } elseif ($datas['count_dimension'] === 'department') {
            $fields['user_department_id'] = 'max(dc.user_department_id)';
        } elseif ($datas['count_dimension'] === 'admin_id') {
            $fields['admin_id'] = 'max(uc.admin_id)';
            $fields['user_admin_id'] = 'max(uc.admin_id)';
        }

        if ($datas['count_periods'] == '1' && $datas['show_type'] == '2') { //按天
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-', cast(max(report.mmonth) as varchar), '-', cast(max(report.mday) as varchar))";
        } else if ($datas['count_periods'] == '2' && $datas['show_type'] == '2') { //按周
            $fields['time'] = "concat(cast(max(report.mweekyear) as varchar), '-', cast(max(report.mweek) as varchar))";
        } else if ($datas['count_periods'] == '3' && $datas['show_type'] == '2') { //按月
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-', cast(max(report.mmonth) as varchar))";
        } else if ($datas['count_periods'] == '4' && $datas['show_type'] == '2') {  //按季
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-', cast(max(report.mquarter) as varchar))";
        } else if ($datas['count_periods'] == '5' && $datas['show_type'] == '2') { //按年
            $fields['time'] = "cast(max(report.myear) as varchar)";
        }

        $targets = explode(',', $datas['target']);
        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }
        if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
            $fields['goods_conversion_rate'] = 'SUM ( report.byorder_quantity_of_goods_ordered ) / nullif(SUM ( report.byorder_user_sessions ) ,0)';
        }
        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_volume'] = " SUM ( report.byorder_sales_volume +  report.byorder_group_id ) ";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_volume'] = " SUM ( report.report_sales_volume + report.report_group_id ) ";
            }
        }
        if (in_array('sale_many_channel_sales_volume', $targets)) { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_many_channel_sales_volume'] = "SUM ( report.byorder_group_id )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_many_channel_sales_volume'] = "SUM ( report.report_group_id )";
            }
        }
        //订单数
        if (in_array('sale_order_number', $targets)) {
            $fields['sale_order_number'] = "SUM ( report.bychannel_sales_volume )";
        }

        if (in_array('sale_sales_quota', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('amazon_fee_rate', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('operate_fee_rate', $targets) || in_array('evaluation_fee_rate', $targets) || in_array('cpc_turnover_rate', $targets)) {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }

        //订单金额
        if (in_array('sale_sales_dollars', $targets) || in_array('cpc_cost_rate', $targets)) {
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['sale_sales_dollars'] = "SUM ( report.bychannel_sales_quota )";
            } else {
                $fields['sale_sales_dollars'] = "SUM ( report.bychannel_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
            }
        }

        if (in_array('sale_return_goods_number', $targets) || in_array('sale_refund_rate', $targets)) {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['sale_return_goods_number'] = "SUM (report.byorder_refund_num )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_return_goods_number'] = "SUM (report.report_refund_num )";
            }
        }
        if (in_array('sale_refund', $targets)) {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "SUM ( 0 - report.byorder_refund )";
                } else {
                    $fields['sale_refund'] = "SUM ( ( 0 - report.byorder_refund ) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "SUM ( (0 - report.report_refund) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = '('.$fields['sale_return_goods_number'] . ") * 1.0 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('promote_discount', $targets)) {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }

        if (in_array('promote_store_fee', $targets)) { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE})';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE})';
                }
            }
        }

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE}) ) ";
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE}) ) ";
                    }
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( ( report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} )) ";
                    }

                }
            }

        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM(report.byorder_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                } else {
                    $fields['cost_profit_profit'] = "SUM(report.byorder_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE}) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM(report.report_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                } else {
                    $fields['cost_profit_profit'] = "SUM(report.report_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE}) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                }
            }
        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = "({$fields['cost_profit_profit']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM(report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM(report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            }
        }
        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} -report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.report_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} - report.report_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM ( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM ( report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM ( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM ( report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = "({$fields['amazon_fee']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }


        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = "({$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "SUM ( report.bychannel_operating_fee ) ";
            } else {
                $fields['operate_fee'] = "SUM ( report.bychannel_operating_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = "({$fields['operate_fee']})/nullif({$fields['sale_sales_quota']}, 0)";
        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1'){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = "({$fields['evaluation_fee']})/nullif({$fields['sale_sales_quota']}, 0) ";
        }

        if (in_array('other_vat_fee', $targets)) {//VAT
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.byorder_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }

        }

        /*if(in_array('other_remark_fee' , $targets)){ //备注费用
            if($datas['currency_code'] == 'ORIGIN'){
                $fields['other_remark_fee'] = "SUM(monthreport.report_note_cost)" ;
            }else{
                $fields['other_remark_fee'] = "SUM(monthreport.report_note_cost / COALESCE(rates.rate ,1) * {:RATE})" ;
            }
        }*/

        if (in_array('other_other_fee', $targets)) { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  + report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_review_enrollment_fee  / COALESCE(rates.rate ,1) * {:RATE})";
            }
        }

        if (in_array('other_review_enrollment_fee', $targets)) { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee  / COALESCE(rates.rate ,1) * {:RATE})";
            }
        }

        if (in_array('cpc_ad_settlement', $targets)) { //广告结款
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_ad_settlement'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
            } else {
                $fields['cpc_ad_settlement'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_product_ads_payment_eventlist_refund / COALESCE(rates.rate ,1) * {:RATE})";
            }
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
            } else {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} -  COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} )";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = "({$fields['cpc_cost']})/nullif({$fields['sale_sales_dollars']}, 0) ";
        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3)";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = "({$fields['cpc_click_number']})/nullif({$fields['cpc_exposure']}, 0) ";
        }
        // 注！此处将字段名用引号包起来是为避免报错，有些数据库会自动将字段大小写转换，会导致报字段不存在的错误
        if (in_array('cpc_order_number', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
            $fields['cpc_order_rate'] = "({$fields['cpc_order_number']})/nullif(SUM(report.bychannel_sales_volume), 0) ";
        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = "({$fields['cpc_order_number']})/nullif({$fields['cpc_click_number']}, 0) ";
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )';
            } else {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field5" / COALESCE(rates.rate ,1) * {:RATE}  )';
            }
        }
        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = "({$fields['cpc_turnover']})/nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = "({$fields['cpc_cost']})/nullif({$fields['cpc_click_number']}, 0) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = "({$fields['cpc_cost']})/nullif({$fields['cpc_turnover']}, 0) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field6" / COALESCE(rates.rate ,1) * {:RATE}  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = "({$fields['cpc_direct_sales_volume']})/nullif({$fields['sale_sales_volume']}, 0) ";
        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report.bychannel_reserved_field5 - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report.bychannel_reserved_field6 )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_reserved_field5 / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} - report.bychannel_reserved_field6 / COALESCE(rates.rate ,1) * {:RATE}   )';
            }
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = "({$fields['cpc_indirect_sales_volume']})/nullif({$fields['sale_sales_volume']}, 0) ";
        }

        if (in_array('fba_goods_value', $targets)) {  //在库总成本
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['fba_goods_value'] = '1';
            } else {
                $fields['fba_goods_value'] = '1';
            }

        }
        if (in_array('fba_stock', $targets)) {  //FBA 库存
            $fields['fba_stock'] = '1';
        }
        if (in_array('fba_sales_volume', $targets)) {  //FBA销量
            $fields['fba_sales_volume'] = 'SUM ( report.bychannel_fba_sales_volume )';
        }
        if (in_array('fba_need_replenish', $targets)) {  //需补货sku
            $fields['fba_need_replenish'] = '1';
        }
        if (in_array('fba_predundancy_number', $targets)) {  //冗余FBA 数
            $fields['fba_predundancy_number'] = '1';
        }

        if (in_array('promote_coupon', $targets)) { //coupon优惠券
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
            } else {
                $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_coupon_payment_eventList_tax" / COALESCE(rates.rate ,1) * {:RATE}  )';
            }
        }
        if (in_array('promote_run_lightning_deal_fee', $targets)) {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
            } else {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee / COALESCE(rates.rate ,1) * {:RATE} )';
            }
        }
        if (in_array('amazon_order_fee', $targets)) {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }

            }
        }
        if (in_array('amazon_refund_fee', $targets)) { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }
            }
        }
        if (in_array('amazon_stock_fee', $targets)) { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE})';
                }
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            }
        }
        if (in_array('goods_adjust_fee', $targets)) { //商品调整费用

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            }

        }

        $this->getUnTimeFields($fields, $datas, $targets, 2);

        return $fields;
    }

    /**
     * 获取不是时间类型的字段
     * @author json.qiu 2021/03/04
     *
     * @param $fields
     * @param $datas
     * @param $targets
     * @param int $type 类型，1商品，2店铺，3运营人员
     */
    private function getUnTimeFields(&$fields, $datas, $targets, $type = 1)
    {
        if (in_array('fba_sales_quota', $targets)) { //FBA商品销售额
            if($datas['sale_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_sales_quota'] = "SUM(report.byorder_fba_sales_quota)";
                } else {
                    $fields['fba_sales_quota'] = "SUM(report.byorder_fba_sales_quota / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_sales_quota'] = "SUM(report.report_fba_sales_quota)";
                } else {
                    $fields['fba_sales_quota'] = "SUM((0-report.report_fba_sales_quota) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('fbm_sales_quota', $targets)) { //FBM商品销售额
            if($datas['sale_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_sales_quota'] = "SUM(report.byorder_fbm_sales_quota)";
                } else {
                    $fields['fbm_sales_quota'] = "SUM(report.byorder_fbm_sales_quota / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_sales_quota'] = "SUM(report.report_fbm_sales_quota)";
                } else {
                    $fields['fbm_sales_quota'] = "SUM((0-report.report_fbm_sales_quota) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('fba_refund_num', $targets)) { //FBA退款量
            if($datas['refund_datas_origin'] == 1){
                $fields['fba_refund_num'] = "SUM(report.byorder_fba_refund_num)";
            }else{
                $fields['fba_refund_num'] = "SUM(report.report_fba_refund_num)";
            }
        }

        if (in_array('fbm_refund_num', $targets)) { //FBM退款量
            if($datas['refund_datas_origin'] == 1){
                $fields['fbm_refund_num'] = "SUM(report.byorder_fbm_refund_num)";
            }else{
                $fields['fbm_refund_num'] = "SUM(report.report_fbm_refund_num)";
            }
        }

        if (in_array('fba_logistics_head_course', $targets)) { //FBA头程物流
            if($datas['cost_count_type'] == 1){
                if ($datas['finance_datas_origin'] == '1'){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fba_logistics_head_course'] = "SUM(report.byorder_fba_logistics_head_course)";
                    } else {
                        $fields['fba_logistics_head_course'] = "SUM(report.byorder_fba_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})";
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fba_logistics_head_course'] = "SUM(report.report_fba_logistics_head_course)";
                    } else {
                        $fields['fba_logistics_head_course'] = "SUM((report.report_fba_logistics_head_course) / COALESCE(rates.rate ,1) * {:RATE})";
                    }
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_logistics_head_course'] = "SUM(report.fba_first_logistics_head_course)";
                } else {
                    $fields['fba_logistics_head_course'] = "SUM((report.fba_first_logistics_head_course) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }

        }

        if (in_array('fbm_logistics_head_course', $targets)) { //fbm物流
            if($datas['cost_count_type'] == 1){
                if($datas['finance_datas_origin'] == 1){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = "SUM(report.byorder_fbm_logistics_head_course)";
                    } else {
                        $fields['fbm_logistics_head_course'] = "SUM(report.byorder_fbm_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})";
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = "SUM(report.report_fbm_logistics_head_course)";
                    } else {
                        $fields['fbm_logistics_head_course'] = "SUM((report.report_fbm_logistics_head_course) / COALESCE(rates.rate ,1) * {:RATE})";
                    }
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_logistics_head_course'] = "SUM(report.fbm_first_logistics_head_course)";
                } else {
                    $fields['fbm_logistics_head_course'] = "SUM((report.fbm_first_logistics_head_course) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('shipping_charge', $targets)) { //运费
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['shipping_charge'] = "SUM(report.byorder_shipping_charge)";
                } else {
                    $fields['shipping_charge'] = "SUM(report.byorder_shipping_charge / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['shipping_charge'] = "SUM(report.report_shipping_charge)";
                } else {
                    $fields['shipping_charge'] = "SUM((report.report_shipping_charge) / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('tax', $targets)) { //TAX（销售）
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['tax'] = "SUM(report.byorder_tax)";
                } else {
                    $fields['tax'] = "SUM(report.byorder_tax / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['tax'] = "SUM(report.report_tax)";
                } else {
                    $fields['tax'] = "SUM(report.report_tax / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('ware_house_lost', $targets)) { //FBA仓丢失赔款
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_lost'] = "SUM(report.byorder_ware_house_lost)";
                } else {
                    $fields['ware_house_lost'] = "SUM(report.byorder_ware_house_lost / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_lost'] = "SUM(report.report_ware_house_lost)";
                } else {
                    $fields['ware_house_lost'] = "SUM(report.report_ware_house_lost / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if (in_array('ware_house_damage', $targets)) { //FBA仓损坏赔款
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_damage'] = "SUM(report.byorder_ware_house_damage)";
                } else {
                    $fields['ware_house_damage'] = "SUM(report.byorder_ware_house_damage / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_damage'] = "SUM(report.report_ware_house_damage)";
                } else {
                    $fields['ware_house_damage'] = "SUM(report.report_ware_house_damage / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }

        if ($type === 2 || $type === 3) {//店铺和运营人员才有的
            if (in_array('channel_fbm_safe_t_claim_demage', $targets)) { //SAF-T
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage)";
                } else {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }
    }

    //按非商品维度,时间展示字段（新增统计维度完成）
    protected function getUnGoodsTimeFields($datas, $timeLine)
    {
        $fields = [];
        $fields['user_id'] = 'max(report.user_id)';
        $fields['site_country_id'] = 'max(report.site_id)';
        if ($datas['count_dimension'] == 'channel_id') {
            $fields['site_id'] = 'max(report.site_id)';
            $fields['channel_id'] = 'max(report.channel_id)';
            $fields['operators'] = 'max(report.operation_user_admin_name)';
            $fields['operation_user_admin_id'] = 'max(report.channel_operation_user_admin_id)';
        } else if ($datas['count_dimension'] == 'site_id') {
            $fields['site_id'] = 'max(report.site_id)';
        } else if ($datas['count_dimension'] == 'site_group') {
            $fields['site_group'] = 'max(report.area_id)';
        }else if($datas['count_dimension'] == 'department'){
            $fields['user_department_id'] = 'max(dc.user_department_id)';
        }else if($datas['count_dimension'] == 'admin_id'){
            $fields['admin_id'] = 'max(uc.admin_id)';
        }

        $time_fields = [];
        if ($datas['time_target'] == 'goods_visitors') {  // 买家访问次数
            $fields['count_total'] = "SUM(report.byorder_user_sessions)";
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_user_sessions');
        } else if ($datas['time_target'] == 'goods_conversion_rate') { //订单商品数量转化率
            $fields['count_total'] = "SUM(report.byorder_quantity_of_goods_ordered) / nullif(SUM ( report.byorder_user_sessions ) ,0)";
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
        } else if ($datas['time_target'] == 'sale_sales_volume') { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = " SUM ( report.byorder_sales_volume + report.byorder_group_id ) ";
                $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_volume + report.byorder_group_id ");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = " SUM ( report.report_sales_volume + report.report_group_id) ";
                $time_fields = $this->getTimeFields($timeLine, "report.report_sales_volume + report.report_group_id");
            }
        } else if ($datas['time_target'] == 'sale_many_channel_sales_volume') { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM ( report.byorder_group_id )";
                $time_fields = $this->getTimeFields($timeLine, "report.byorder_group_id");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = "SUM ( report.report_group_id )";
                $time_fields = $this->getTimeFields($timeLine, "report.report_group_id");
            }
        } else if ($datas['time_target'] == 'sale_order_number') {//订单数
            $fields['count_total'] = "SUM ( report.bychannel_sales_volume )";
            $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_volume");
        } else if ($datas['time_target'] == 'sale_sales_quota') {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE}");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE}");
                }
            }
        } else if ($datas['time_target'] == 'sale_sales_dollars') { //订单金额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM ( report.bychannel_sales_quota )";
                $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota");
            } else {
                $fields['count_total'] = "SUM ( report.bychannel_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota / COALESCE(rates.rate ,1) * {:RATE}");
            }
        } else if ($datas['time_target'] == 'sale_return_goods_number') {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['count_total'] = "SUM (report.byorder_refund_num )";
                $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num");
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['count_total'] = "SUM (report.report_refund_num )";
                $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num");
            }
        } else if ($datas['time_target'] == 'sale_refund') {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( 0 - report.byorder_refund )";
                    $time_fields = $this->getTimeFields($timeLine, " 0 - report.byorder_refund ");
                } else {
                    $fields['count_total'] = "SUM ( ( 0 - report.byorder_refund) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, " (0 - report.byorder_refund) / COALESCE(rates.rate ,1) * {:RATE} ");
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_refund )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund ");
                } else {
                    $fields['count_total'] = "SUM ( report.report_refund / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund / COALESCE(rates.rate ,1) * {:RATE} ");
                }
            }
        } else if ($datas['time_target'] == 'sale_refund_rate') {  //退款率
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM (report.byorder_refund_num ) * 1.0 / nullif(SUM(report.byorder_sales_volume + report.byorder_group_id),0)";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num * 1.0", "report.byorder_sales_volume+ report.byorder_group_id");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM (report.report_refund_num  ) * 1.0 / nullif(SUM(report.byorder_sales_volume+ report.byorder_group_id),0)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num * 1.0 ", "report.byorder_sales_volume+ report.byorder_group_id");
                }
            }else{
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM (report.byorder_refund_num ) * 1.0  / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM (report.report_refund_num ) * 1.0 / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                }
            }
        } else if ($datas['time_target'] == 'promote_discount') {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
                }
            }
        } else if ($datas['time_target'] == 'promote_refund_discount') {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} ");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
                }
            }
        } else if ($datas['time_target'] == 'promote_store_fee') { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee +  report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}+ report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_coupon_redemption_fee /  COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_run_lightning_deal_fee /  COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'cost_profit_profit') {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)';
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = 'SUM((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit )';
                        $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.byorder_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})';
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    } else {
                        $fields['count_total'] = 'SUM(  (report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}) +  report.byorder_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} )';
                        $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}) +  report.byorder_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course)';
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit )';
                        $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.report_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})';
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    } else {
                        $fields['count_total'] = 'SUM((report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}) + report.report_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE}  )';
                        $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}) + report.report_channel_profit / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_profit / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                }
            }
        } else if ($datas['time_target'] == 'cost_profit_profit_rate') {  //毛利率
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( (report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  )) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit ', 'report.byorder_sales_quota');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course)) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(( report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  )) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, '( report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit', 'report.byorder_sales_quota');
                    }
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(( report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_channel_profit + report.bychannel_channel_profit)) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, '( report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_channel_profit + report.bychannel_channel_profit', 'report.report_sales_quota');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course)) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( (report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_channel_profit + report.bychannel_channel_profit)) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_channel_profit + report.bychannel_channel_profit', 'report.report_sales_quota');
                    }
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee') {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}  + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_sales_commission') {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_delivery_fee') {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} -report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} -report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.report_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} - report.report_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.report_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} - report.report_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_profit ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_profit ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_settlement_fee') {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }

        } else if ($datas['time_target'] == 'amazon_other_fee') {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_shipping_fee') {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_returnshipping / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_returnshipping )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.report_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_returnshipping / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_refund_deducted_commission') {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee_rate') {  //亚马逊费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee', 'report.byorder_sales_quota');

                } elseif ($datas['finance_datas_origin'] == '2') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee', 'report.report_sales_quota');

                } elseif ($datas['finance_datas_origin'] == '2') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee', 'report.report_sales_quota');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_purchase_cost') {  //采购成本
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == 1) {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                }

            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (( report.first_purchasing_cost )) ";
                    $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_purchasing_cost / COALESCE(rates.rate ,1)) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE}) ');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_logistics_cost') {  // 物流/头程
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == 1) {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                }

            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}) ) ";
                    $time_fields = $this->getTimeFields($timeLine, '(report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.byorder_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost) ) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course + report.first_purchasing_cost)', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.report_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost)) / nullif(SUM ( report.report_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course + report.first_purchasing_cost)', 'report.report_sales_quota');
                }
            }
        } else if ($datas['time_target'] == 'operate_fee') {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM ( report.bychannel_operating_fee ) ";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee');
            } else {
                $fields['count_total'] = "SUM ( report.bychannel_operating_fee / COALESCE(rates.rate ,1) * {:RATE} ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.bychannel_operating_fee / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } else if ($datas['time_target'] == 'operate_fee_rate') {  //运营费用占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM(report.bychannel_operating_fee ) /nullif(SUM(report.byorder_sales_quota),0)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_operating_fee ) /nullif(SUM(report.report_sales_quota),0)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'evaluation_fee') {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } else if ($datas['time_target'] == 'evaluation_fee_rate') {  //测评费用占比
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10', 'report.byorder_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10', 'report.byorder_sales_quota');
                }

            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10', 'report.report_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10', 'report.report_sales_quota');
                }
            }
        } else if ($datas['time_target'] == 'other_vat_fee') {//VAT
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                    $time_fields = $this->getTimeFields($timeLine, '0-report.byorder_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, '(0-report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                    $time_fields = $this->getTimeFields($timeLine, '0-report.report_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, '(0-report.report_reserved_field17 )/ COALESCE(rates.rate ,1) * {:RATE}');
                }
            }

        } else if ($datas['time_target'] == 'other_other_fee') { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment + report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_review_enrollment_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_review_enrollment_fee / COALESCE(rates.rate ,1) * {:RATE}');
            }
        } else if ($datas['time_target'] == 'other_review_enrollment_fee') { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee / COALESCE(rates.rate ,1) * {:RATE}  )";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee / COALESCE(rates.rate ,1) * {:RATE}');
            }
        } else if ($datas['time_target'] == 'cpc_ad_settlement') { //广告结款
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund ');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_product_ads_payment_eventlist_refund / COALESCE(rates.rate ,1) * {:RATE})";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_product_ads_payment_eventlist_refund / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } else if ($datas['time_target'] == 'cpc_cost') {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} -  COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} -  COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } else if ($datas['time_target'] == 'cpc_cost_rate') {  //CPC花费占比
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) )  /  nullif( SUM (report.bychannel_sales_quota ) , 0 )";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)', 'report.bychannel_sales_quota');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} -  COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} )  /  nullif( SUM (report.bychannel_sales_quota ) , 0 ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} -  COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE}  ', 'report.bychannel_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_exposure') {  //CPC曝光量
            $fields['count_total'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3)";
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3  ');
        } else if ($datas['time_target'] == 'cpc_click_number') {  //CPC点击次数
            $fields['count_total'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)";
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');

        } else if ($datas['time_target'] == 'cpc_click_rate') {  //CPC点击率
            $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)) / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3), 0 ) ";
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4', 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');
        } else if ($datas['time_target'] == 'cpc_order_number') {  //CPC订单数
            $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7) ';
            $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7');
        } else if ($datas['time_target'] == 'cpc_order_rate') {  //cpc订单占比
            $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7) / nullif( SUM(report.bychannel_sales_volume) , 0 )';
            $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.bychannel_sales_volume');
        } else if ($datas['time_target'] == 'cpc_click_conversion_rate') {  //cpc点击转化率
            $fields['count_total'] = '(SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7)) / nullif( SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4) , 0 )';
            $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
        } else if ($datas['time_target'] == 'cpc_turnover') {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5"');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field5" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field5" / COALESCE(rates.rate ,1) * {:RATE}');
            }
        } else if ($datas['time_target'] == 'cpc_turnover_rate') {  //CPC成交额占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = '(SUM (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" ))/nullif( SUM(report.byorder_sales_quota),0)';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" ', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = '(SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" ))/nullif( SUM(report.report_sales_quota),0)';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" ', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_avg_click_cost') {  //CPC平均点击花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = '(SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0))) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            } else {
                $fields['count_total'] = '(SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} - COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} )) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} - COALESCE(report.bychannel_cpc_sb_cost,0) / COALESCE(rates.rate ,1) * {:RATE} ', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            }
        } else if ($datas['time_target'] == 'cpc_acos') {  // ACOS
            $fields['count_total'] = '(SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost -  COALESCE(report.bychannel_cpc_sb_cost,0) )) / nullif( SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5"  ) , 0 ) ';
            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost -  COALESCE(report.bychannel_cpc_sb_cost,0)', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  + report."bychannel_reserved_field5" ');
        } else if ($datas['time_target'] == 'cpc_direct_sales_volume') {  //CPC直接销量

            $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
            $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ');
        } else if ($datas['time_target'] == 'cpc_direct_sales_quota') {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6"  )';
                $time_fields = $this->getTimeFields($timeLine, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" ');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field6" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field6" / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } else if ($datas['time_target'] == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  ) /nullif(SUM ( report.byorder_sales_volume+ report.byorder_group_id ) ,0)';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.byorder_sales_volume+ report.byorder_group_id');
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ) /nullif(SUM ( report.report_sales_volume + report.report_group_id  ) ,0)';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.report_sales_volume+ report.report_group_id');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume') {  //CPC间接销量
            $fields['count_total'] = ' SUM(report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8 )';
            $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8');
        } else if ($datas['time_target'] == 'cpc_indirect_sales_quota') {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5" - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6" )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6"');
            } else {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field5" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} - report."bychannel_reserved_field6" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_reserved_field5" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} - report."bychannel_reserved_field6" / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" ) /nullif(SUM ( report.byorder_sales_volume + report.byorder_group_id) ,0)';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.byorder_sales_volume + report.byorder_group_id');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" ) /nullif(SUM ( report.report_sales_volume + report.report_group_id) ,0)';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.report_sales_volume+ report.report_group_id');
            }
        }else if ($datas['time_target'] == 'fba_sales_volume') {  //FBA销量
            $fields['count_total'] = 'SUM ( report.bychannel_fba_sales_volume )';
            $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_sales_volume');
        } else if ($datas['time_target'] == 'promote_coupon') { //coupon优惠券
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax"');
            } else {
                $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_coupon_payment_eventList_tax" / COALESCE(rates.rate ,1) * {:RATE} )';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_coupon_redemption_fee / COALESCE(rates.rate ,1) * {:RATE} + report."bychannel_coupon_payment_eventList_tax" / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } elseif ($datas['time_target'] == 'promote_run_lightning_deal_fee') {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee');
            } else {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee / COALESCE(rates.rate ,1) * {:RATE} ');
            }
        } elseif ($datas['time_target'] == 'amazon_order_fee') {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee ');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_order_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }

            }
        } elseif ($datas['time_target'] == 'amazon_refund_fee') { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_refund_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_stock_fee') { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE})';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE} + report.bychannel_channel_amazon_storage_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_long_term_storage_fee') { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } elseif ($datas['time_target'] == 'goods_adjust_fee') { //商品调整费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_goods_adjustment_fee  / COALESCE(rates.rate ,1) * {:RATE} +  report.bychannel_channel_goods_adjustment_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        }else{
            $fields_tmp = $this->getTimeField($datas,$timeLine,2);
            $fields['count_total']  = $fields_tmp['count_total'];
            $time_fields            = $fields_tmp['time_fields'];

        }

        $fields[$datas['time_target']] = $fields['count_total'] ;
        if(!empty($time_fields)){
            foreach($time_fields as $kt=>$time_field){
                $fields[$kt] = $time_field ;
            }
        }
        return $fields;
    }





    /**
     * 获取非商品维度FBA 的指标数据（新增统计维度完成）
     * @author: LWZ
     *
     * @param array $lists
     * @param array $fields
     * @param array $datas
     * @param array $channel_arr
     * @return array
     */
    protected function getUnGoodsFbaData($lists = [], $fields = [], $datas = [], $channel_arr = [], $currencyInfo, $exchangeCode)
    {
        if(empty($lists)){
            return $lists ;
        } else {
            $table = "ods.ods_dataark_f_amazon_fba_inventory_by_channel_{$this->user['codeno']} as c";
            $where = 'c.user_id = ' . $lists[0]['user_id'];
            if (!empty($channel_arr)){
                if (count($channel_arr)==1){
                    $where .= " AND c.channel_id = ".intval(implode(",",$channel_arr));
                }else{
                    $where .= " AND c.channel_id IN (".implode(",",$channel_arr).")";
                }
            }
            if($datas['count_dimension'] == 'channel_id'){
                $fba_fields = $group = 'c.channel_id' ;
            }else if($datas['count_dimension'] == 'site_id'){
                $fba_fields = $group = 'c.site_id' ;
            }else if($datas['count_dimension'] == 'department') { //部门
                $fba_fields = $group = 'dc.user_department_id ,c.area_id' ;
                $table .= " LEFT JOIN dim.dim_dataark_b_department_channel as dc ON dc.user_id = c.user_id and dc.channel_id = c.channel_id " ;
            }else if($datas['count_dimension'] == 'admin_id'){ //子账号
                $fba_fields = $group = 'uc.admin_id , c.area_id' ;
                $table .= " LEFT JOIN dim.dim_dataark_b_user_channel as uc ON uc.user_id = c.user_id and uc.channel_id = c.channel_id " ;
            }
            $where_arr = array() ;
            foreach($lists as $list1){
                if($datas['count_dimension'] == 'channel_id'){
                    $where_arr[] = array( 'channel_id'=>$list1['channel_id'] , 'site_id'=>$list1['site_id']) ;
                }else if($datas['count_dimension'] == 'site_id'){
                    $where_arr[] = array('site_id'=>$list1['site_id']) ;
                }else if($datas['count_dimension'] == 'department'){
                    $where_arr[] = array('user_department_id'=>$list1['user_department_id']) ;
                }else if($datas['count_dimension'] == 'admin_id'){
                    $where_arr[] = array('admin_id'=>$list1['admin_id']) ;
                }
            }
            if ($datas['currency_code'] != 'ORIGIN') {
                if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                    $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = c.site_id AND rates.user_id = 0 ";
                } else {
                    $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = c.site_id AND rates.user_id = c.user_id  ";
                }
            }

            if($datas['count_dimension'] == 'channel_id'){
                $where_strs = array_unique(array_column($where_arr , 'channel_id')) ;
                $where_str = 'c.channel_id IN (' . implode(',' , $where_strs) . ")" ;
            }else if($datas['count_dimension'] == 'site_id'){
                $where_strs = array_unique(array_column($where_arr , 'site_id')) ;
                $where_str = 'c.site_id IN (' . implode(',' , $where_strs) . ")" ;
            }else if($datas['count_dimension'] == 'department'){
                $where_strs = array_unique(array_column($where_arr , 'user_department_id')) ;
                $where_str = 'dc.user_department_id IN (' . implode(',' , $where_strs) . ")" ;

            }else if($datas['count_dimension'] == 'admin_id'){
                $where_strs = array_unique(array_column($where_arr , 'admin_id')) ;
                $where_str = 'uc.admin_id IN (' . implode(',' , $where_strs) . ")" ;
            }
        }

        $amazon_fba_inventory_by_channel_md = new AmazonFbaInventoryByChannelPrestoModel($this->dbhost, $this->codeno);
        $where.= ' AND ' . $where_str ;
        if ($datas['currency_code'] == 'ORIGIN') {
            $fba_fields .= " , SUM ( DISTINCT (c.yjzhz) )  as fba_goods_value";
        } else {
            $fba_fields .= " , SUM (DISTINCT(c.yjzhz / COALESCE(rates.rate ,1) * {:RATE}))  as fba_goods_value";
        }
        $fba_fields.= ' ,SUM(DISTINCT(c.total_fulfillable_quantity)) as fba_stock , SUM(DISTINCT(c.replenishment_sku_nums)) as fba_need_replenish ,SUM(DISTINCT(c.redundancy_sku)) as fba_predundancy_number';
        $fba_fields = str_replace("{:RATE}", $exchangeCode, $fba_fields);
        $fbaData =$amazon_fba_inventory_by_channel_md->select($where , $fba_fields ,$table ,'' , '' ,$group);

        $fbaDatas = array() ;
        foreach($fbaData as $fba){
            if($datas['count_dimension'] == 'channel_id'){
                $fbaDatas[$fba['channel_id']] = $fba ;
            }else if($datas['count_dimension'] == 'site_id'){
                $fbaDatas[$fba['site_id']] = $fba ;
            }else if($datas['count_dimension'] == 'department'){
                $fbaDatas[$fba['user_department_id']]['fba_goods_value']+= $fba['fba_goods_value'] ;
                $fbaDatas[$fba['user_department_id']]['fba_stock']+= $fba['fba_stock'] ;
                $fbaDatas[$fba['user_department_id']]['fba_need_replenish']+= $fba['fba_need_replenish'] ;
                $fbaDatas[$fba['user_department_id']]['fba_predundancy_number']+= $fba['fba_predundancy_number'] ;
            }else if($datas['count_dimension'] == 'admin_id'){
                $fbaDatas[$fba['admin_id']]['fba_goods_value']+= $fba['fba_goods_value'] ;
                $fbaDatas[$fba['admin_id']]['fba_stock']+= $fba['fba_stock'] ;
                $fbaDatas[$fba['admin_id']]['fba_need_replenish']+= $fba['fba_need_replenish'] ;
                $fbaDatas[$fba['admin_id']]['fba_predundancy_number']+= $fba['fba_predundancy_number'] ;
            }
        }
        foreach($lists as $k=>$list2){
            if($datas['count_dimension'] == 'channel_id'){
                $fba_data = $fbaDatas[$list2['channel_id']] ;
            }else if($datas['count_dimension'] == 'site_id'){
                $fba_data = $fbaDatas[$list2['site_id']] ;
            }else if($datas['count_dimension'] == 'department'){
                $fba_data = $fbaDatas[$list2['user_department_id']] ;
            }else if($datas['count_dimension'] == 'admin_id'){
                $fba_data = $fbaDatas[$list2['admin_id']] ;
            }
            if (!empty($fields['fba_goods_value'])) {  //在库总成本
                $lists[$k]['fba_goods_value'] = empty($fba_data) ? null : $fba_data['fba_goods_value'] ;
            }
            if (!empty($fields['fba_stock'])) {  //FBA 库存
                $lists[$k]['fba_stock'] = empty($fba_data) ? null : $fba_data['fba_stock'] ;
            }
            if (!empty($fields['fba_need_replenish'])) {  //需补货sku
                $lists[$k]['fba_need_replenish'] = empty($fba_data) ? null : $fba_data['fba_need_replenish'] ;
            }
            if (!empty($fields['fba_predundancy_number'])) {  //冗余FBA 数
                $lists[$k]['fba_predundancy_number'] = empty($fba_data) ? null : $fba_data['fba_predundancy_number'] ;
            }
        }
        return $lists;
    }

    /**
     * 获取负责的所有部门的id
     * @param int $user_id
     * @param int $department_id
     * @return array
     * @author: LWZ
     */
    protected function getMyAllDepartmentIds($deparmentData, $department_id = 0)
    {
        $ids = array() ;
        $ids[] = $department_id ;
        if(!empty($deparmentData[$department_id])){
            if($deparmentData[$department_id]['level'] == 3){

            }else if($deparmentData[$department_id]['level'] == 2){
                foreach($deparmentData as $deparment2){
                    if($deparment2['parent_id'] == $department_id){
                        $ids[] = $deparment2['id'] ;
                    }
                }
            }else if($deparmentData[$department_id]['level'] == 1){
                foreach($deparmentData as $deparment2){
                    if($deparment2['parent_id'] == $department_id){
                        $ids[] = $deparment2['id'] ;
                        foreach($deparmentData as $deparment3){
                            if($deparment2['id'] == $deparment3['parent_id']){
                                $ids[] = $deparment3['id'] ;
                            }
                        }
                    }
                }
            }
        }
        return $ids ;
    }



}
