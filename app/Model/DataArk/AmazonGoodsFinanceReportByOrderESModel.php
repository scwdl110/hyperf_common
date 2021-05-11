<?php

namespace App\Model\DataArk;

use App\Model\AbstractESModel;

class AmazonGoodsFinanceReportByOrderESModel extends AbstractESModel
{
    protected $table = 'f_amazon_goods_finance_report_by_order_';

    /**
     * 获取商品维度统计列表(新增统计维度完成)
     * @param string $where
     * @param array $datas
     * @param string $limit
     * @param string $order
     * @param int $count_tip 获取统计的数据信息 0-获取列表和总条数 1-仅仅获取列表 2-仅获取总条数
     * @param array $channel_arr
     * @return array
     * @author: LWZ
     */
    public function getListByGoods(
        $where = '',
        $datas = [],
        $limit = '',
        $sort = '',
        $order = '',
        $count_tip = 0,
        array $channel_arr = [],
        array $currencyInfo = [],
        $exchangeCode = '1',
        array $timeLine = [],
        array $deparmentData = [],
        int $userId = 0,
        int $adminId = 0
    ) {
        //没有按周期统计 ， 按指标展示
        $fields = $this->getGoodsFields($datas);
        if (empty($fields)) {
            return [];
        }

        $where_detail = is_array($datas['where_detail']) ? $datas['where_detail'] : json_decode($datas['where_detail'], true);
        if (empty($where_detail)) {
            $where_detail = array();
        }
        $orderby = '';
        if( !empty($datas['sort_target']) && !empty($fields[$datas['sort_target']]) && !empty($datas['sort_order']) ){
            $orderby = '(('.$fields[$datas['sort_target']].') IS NULL) ,  (' . $fields[$datas['sort_target']] . ' ) ' . $datas['sort_order'];
        }

        if (!empty($order) && !empty($sort) && !empty($fields[$sort]) && $datas['limit_num'] == 0 ) {
            $orderby =  '(('.$fields[$sort].') IS NULL) ,  (' . $fields[$sort] . ' ) ' . $order;
        }

        $rt = array();
        $fields_arr = array();
        foreach ($fields as $field_name => $field) {
            $fields_arr[] = $field . ' AS "' . $field_name . '"';
        }

        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));

        $table = "f_dw_goods_day_report_{$this->dbhost} AS report" ;
        $mod_where = "report.user_id_mod = " . ($datas['user_id'] % 20);
        if (!empty($mod_where)) {
            $where .= ' AND ' . $mod_where;
        }

        $having = '';
        if (in_array($datas['count_dimension'], ['parent_asin', 'asin', 'sku'])) {
            if($datas['is_distinct_channel'] == 1){ //有区分店铺
                if ($datas['count_periods'] > 0 && $datas['show_type'] == '2' ) {
                    if($datas['count_periods'] == '4'){ //按季度
                        $group = 'report.goods_' . $datas['count_dimension'] . ' , report.channel_id ,report.myear , report.mquarter ';
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ' , report.channel_id ,report.myear , report.mquarter ';
                    }else if($datas['count_periods'] == '5') { //年
                        $group = 'report.goods_' . $datas['count_dimension'] . '  , report.channel_id ,report.myear' ;
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ' , report.channel_id,report.myear ';
                    }else {
                        $group = 'report.' . $datas['count_dimension'] . '_group, report.channel_id  ';
                        $orderby = "report." . $datas['count_dimension'] . "_group, report.channel_id ";
                    }

                }else{
                    $group = 'report.goods_' . $datas['count_dimension'] . ' ,report.channel_id ';
                    $orderby = empty($orderby) ? ('report.goods_' . $datas['count_dimension'] . ' ,report.channel_id ') : ($orderby . ' , report.goods_'. $datas['count_dimension'] . ' ,report.channel_id ');
                }
            }else{  //不区分店铺
                if ($datas['count_periods'] > 0 && $datas['show_type'] == '2' ) {
                    if($datas['count_periods'] == '4'){ //按季度
                        $group = 'report.goods_' . $datas['count_dimension'] . '  ,report.myear , report.mquarter ';
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ' ,report.myear , report.mquarter ';
                    }else if($datas['count_periods'] == '5') { //年
                        $group = 'report.goods_' . $datas['count_dimension'] . ' ,report.myear' ;
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ',report.myear ';
                    }else if($datas['count_periods'] == '3'){ //按月
                        $group = 'report.goods_' . $datas['count_dimension'] . ' ,report.myear ,report.mmonth' ;
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ',report.myear ,report.mmonth';
                    }else if($datas['count_periods'] == '2'){  //按周
                        $group = 'report.goods_' . $datas['count_dimension'] . ' ,report.mweekyear ,report.mweek' ;
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ',report.mweekyear ,report.mweek';
                    }else if($datas['count_periods'] == '1') {  //按天
                        $group = 'report.goods_' . $datas['count_dimension'] . ' ,report.myear ,report.mmonth ,report.mday' ;
                        $orderby = 'report.goods_' . $datas['count_dimension'] . ',report.myear ,report.mmonth,report.mday';
                    }
                }else{
                    $group = 'report.goods_' . $datas['count_dimension'] . ' ';
                    $orderby = empty($orderby) ? ('report.goods_' . $datas['count_dimension']) : ($orderby . ' , report.goods_'. $datas['count_dimension'] );
                }
            }

            $where .= " AND report.goods_" . $datas['count_dimension'] . " != '' ";
        } else if ($datas['count_dimension'] == 'isku') {
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                if($datas['count_periods'] == '4'){ //按季度
                    $group = 'report.goods_isku_id  , report.myear , report.mquarter ';
                    $orderby = 'report.goods_isku_id  , report.myear , report.mquarter ';
                }else if($datas['count_periods'] == '5') { //年
                    $group = 'report.goods_isku_id  , report.myear' ;
                    $orderby = 'report.goods_isku_id  , report.myear ';
                }else {
                    $group = "report.isku_group ";
                    $orderby = "report.isku_group ";
                }

            }else{
                $group = 'report.goods_isku_id ';
                $orderby = empty($orderby) ? ('report.goods_isku_id ') : ($orderby . ' , report.goods_isku_id ');
            }
            $where .= " AND report.goods_isku_id > 0";
        } else if ($datas['count_dimension'] == 'group') {
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                if($datas['count_periods'] == '4'){ //按季度
                    $group = 'report.goods_group_id , report.myear , report.mquarter ';
                    $orderby = 'report.goods_group_id ,report.myear , report.mquarter ';
                }else if($datas['count_periods'] == '5') { //年
                    $group = 'report.goods_group_id , report.myear' ;
                    $orderby = 'report.goods_group_id , report.myear ';
                }else {
                    $group = 'report.group_id_group  ';
                    $orderby = "report.group_id_group ";
                }

            }else{
                $group = 'report.goods_group_id  ';
                $orderby = empty($orderby) ? ('report.goods_group_id ') : ($orderby . ' , report.goods_group_id');
            }
            $where .= " AND report.goods_group_id > 0";
        } else if ($datas['count_dimension'] == 'class1') {
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {

                if ($datas['count_periods'] == '1' ) { //按天
                    $group = 'report.goods_product_category_name_1  , report.myear , report.mmonth  , report.mday';
                    $orderby = 'report.goods_product_category_name_1 , report.myear , report.mmonth  , report.mday';
                } else if ($datas['count_periods'] == '2' ) { //按周
                    $group = 'report.goods_product_category_name_1 , report.myear , report.mweek';
                    $orderby = 'report.goods_product_category_name_1, report.myear , report.mweek';
                } else if ($datas['count_periods'] == '3' ) { //按月
                    $group = 'report.goods_product_category_name_1  , report.myear , report.mmonth';
                    $orderby = 'report.goods_product_category_name_1 , report.myear , report.mmonth';
                } else if ($datas['count_periods'] == '4' ) {  //按季
                    $group = 'report.goods_product_category_name_1 , report.myear , report.mquarter';
                    $orderby = 'report.goods_product_category_name_1  , report.myear , report.mquarter';
                } else if ($datas['count_periods'] == '5' ) { //按年
                    $group = 'report.goods_product_category_name_1 , report.myear';
                    $orderby = 'report.goods_product_category_name_1  , report.myear';
                }

            }else{
                $group = 'report.goods_product_category_name_1 ';
                $orderby = empty($orderby) ? ('max(report.goods_product_category_name_1) ') : ($orderby . ' , max(report.goods_product_category_name_1) ');
            }
            $where .= " AND report.goods_product_category_name_1 != ''";

        } else if($datas['count_dimension'] == 'head_id'){ //按负责人维度统计
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                if($datas['count_periods'] == '4'){ //按季度
                    $group = 'report.isku_head_id , report.myear , report.mquarter ';
                    $orderby = 'report.isku_head_id ,report.myear , report.mquarter ';
                }else if($datas['count_periods'] == '5') { //年
                    $group = 'report.isku_head_id , report.myear' ;
                    $orderby = 'report.isku_head_id , report.myear ';
                }else {
                    $group = 'report.isku_head_id_group  ';
                    $orderby = "report.isku_head_id_group ";
                }
            }else{
                $group = 'report.isku_head_id  ';
                $orderby = empty($orderby) ? ('report.isku_head_id ') : ($orderby . ' , report.isku_head_id');
            }
            $where.= " AND report.isku_head_id > 0";
        }else if($datas['count_dimension'] == 'developer_id'){ //按开发人维度统计
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                if($datas['count_periods'] == '4'){ //按季度
                    $group = 'report.isku_developer_id , report.myear , report.mquarter ';
                    $orderby = 'report.isku_developer_id ,report.myear , report.mquarter ';
                }else if($datas['count_periods'] == '5') { //年
                    $group = 'report.isku_developer_id , report.myear' ;
                    $orderby = 'report.isku_developer_id , report.myear ';
                }else {
                    $group = 'report.isku_developer_id_group  ';
                    $orderby = "report.isku_developer_id_group ";
                }
            }else{
                $group = 'report.isku_developer_id  ';
                $orderby = empty($orderby) ? ('report.isku_developer_id ') : ($orderby . ' , report.isku_developer_id');
            }
            $where.= " AND report.isku_developer_id > 0";
        }else if($datas['count_dimension'] == 'all_goods'){ //按全部商品维度统计
            if($datas['is_distinct_channel'] == 1) { //有区分店铺
                if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                    if ($datas['count_periods'] == '1' ) { //按天
                        $group = 'report.channel_id , report.myear , report.mmonth  , report.mday';
                        $orderby = 'report.channel_id ,report.myear , report.mmonth  , report.mday';
                    } else if ($datas['count_periods'] == '2' ) { //按周
                        $group = 'report.channel_id ,report.mweekyear , report.mweek';
                        $orderby = 'report.channel_id ,report.mweekyear , report.mweek';
                    } else if ($datas['count_periods'] == '3' ) { //按月
                        $group = 'report.channel_id ,report.myear , report.mmonth';
                        $orderby = 'report.channel_id ,report.myear , report.mmonth';
                    } else if ($datas['count_periods'] == '4' ) {  //按季
                        $group = 'report.channel_id ,report.myear , report.mquarter';
                        $orderby = 'report.channel_id ,report.myear , report.mquarter';
                    } else if ($datas['count_periods'] == '5' ) { //按年
                        $group = 'report.channel_id ,report.myear';
                        $orderby = 'report.channel_id ,report.myear';
                    }
                }else{
                    $group = 'report.channel_id ,report.user_id  ';
                }
            }else{
                if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                    if ($datas['count_periods'] == '1' ) { //按天
                        $group = 'report.myear , report.mmonth  , report.mday';
                        $orderby = 'report.myear , report.mmonth  , report.mday';
                    } else if ($datas['count_periods'] == '2' ) { //按周
                        $group = 'report.mweekyear , report.mweek';
                        $orderby = 'report.mweekyear , report.mweek';
                    } else if ($datas['count_periods'] == '3' ) { //按月
                        $group = 'report.myear , report.mmonth';
                        $orderby = 'report.myear , report.mmonth';
                    } else if ($datas['count_periods'] == '4' ) {  //按季
                        $group = 'report.myear , report.mquarter';
                        $orderby = 'report.myear , report.mquarter';
                    } else if ($datas['count_periods'] == '5' ) { //按年
                        $group = 'report.myear';
                        $orderby = 'report.myear';
                    }
                }else{
                    $group = 'report.user_id  ';
                }
            }

        }

        if (!empty($where_detail)) {
            if (!empty($where_detail['transport_mode'])) {
                if(!is_array($where_detail['transport_mode'])){
                    $transport_modes = explode(',' , $where_detail['transport_mode']) ;
                }else{
                    $transport_modes = $where_detail['transport_mode'] ;
                }
                if(count($transport_modes) == 1){
                    $where .= ' AND report.goods_transport_mode = ' . ($transport_modes[0] == 'FBM' ? 1 : 2);
                }
            }
            if(!empty($where_detail['up_status'])){
                $where.= " AND report.goods_up_status = " . (intval($where_detail['up_status']) == 1 ? 1 : 2 );
            }
            if(!empty($where_detail['is_new'])){
                $where.= " AND report.goods_is_new = " . (intval($where_detail['is_new']) == 1 ? 1 : 0 );
            }
            if(!empty($where_detail['is_care'])){
                $where.= " AND report.goods_is_care = " . (intval($where_detail['is_care']) == 1 ? 1 : 0 );
            }
            if (!empty($where_detail['group_id'])) {
                if(is_array($where_detail['group_id'])){
                    $group_str = implode(',', $where_detail['group_id']);
                }else{
                    $group_str = $where_detail['group_id'] ;
                }

                if (!empty($group_str)) {
                    $where .= " AND report.goods_group_id  IN ( " . $group_str . ")";
                }
            }
            if (!empty($where_detail['operators_id'])) {
                if(is_array($where_detail['operators_id'])){
                    $operators_str = implode(',', $where_detail['operators_id']);
                }else{
                    $operators_str = $where_detail['operators_id'] ;
                }
                $where .= " AND report.goods_operation_user_admin_id  IN ( " . $operators_str . " ) ";
            }

            $target_wheres = $where_detail['target'] ?? '';
            if (!empty($target_wheres)) {
                foreach ($target_wheres as $target_where) {
                    if(!empty($fields[$target_where['key']])){
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
        }

        if (!empty($having)) {
            $group .= " having " . $having;
        }

        $group = str_replace("{:RATE}", $exchangeCode, $group);
        $where = str_replace("{:RATE}", $exchangeCode, $where);
        $orderby = str_replace("{:RATE}", $exchangeCode, $orderby);
        $limit_num = 0 ;
        if($datas['show_type'] == 2 && $datas['limit_num'] > 0 ){
            $limit_num = $datas['limit_num'] ;
        }
        $count = 0;
        $lists  = array() ;
        if ($count_tip == 2) { //仅统计总条数
            $count = $this->getTotalNum($where, $table, $group);
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        } else if ($count_tip == 1) {  //仅仅统计列表
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                if($datas['show_type'] = 2 && ( !empty($fields['fba_sales_stock']) || !empty($fields['fba_sales_day']) || !empty($fields['fba_reserve_stock']) || !empty($fields['fba_recommended_replenishment']) || !empty($fields['fba_special_purpose']) )){
                    $lists = $this->getGoodsFbaDataTmp($lists , $fields , $datas,$channel_arr) ;
                }
            }
        } else {  //统计列表和总条数
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                if($datas['show_type'] = 2 && ( !empty($fields['fba_sales_stock']) || !empty($fields['fba_sales_day']) || !empty($fields['fba_reserve_stock']) || !empty($fields['fba_recommended_replenishment']) || !empty($fields['fba_special_purpose']) )){
                    $lists = $this->getGoodsFbaDataTmp($lists , $fields , $datas,$channel_arr) ;
                }
            }

            if (empty($lists) or $datas['is_count'] == 1) {
                $count = 0;
            } else {
                $count = $this->getTotalNum($where, $table, $group);
                if($limit_num > 0 && $count > $limit_num){
                    $count = $limit_num ;
                }
            }
        }
        if(!empty($lists) && $datas['show_type'] = 2 && $datas['limit_num'] > 0 && !empty($order) && !empty($sort) && !empty($fields[$sort]) && !empty($fields[$datas['sort_target']]) && !empty($datas['sort_target']) && !empty($datas['sort_order'])){
            //根据字段对数组$lists进行排列
            $sort_names = array_column($lists,$sort);
            $order2  =  $order == 'desc' ? \SORT_DESC : \SORT_ASC;
            array_multisort($sort_names,$order2,$lists);
        }
        if(!empty($lists)){
            //获取部分需要先获取出汇总数据再计算的值
            $this->getOtherCountDatas($lists , $datas) ;
        }

        $rt['lists'] = empty($lists) ? array() : $lists;
        $rt['count'] = intval($count);
        return $rt;
    }

    //获取部分需要先获取出汇总数据再计算的值
    public function getOtherCountDatas($lists = array() , $datas = array()){
        $targets = explode(',', $datas['target']);
        foreach($lists as $k=>$fields){
            if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
                $lists[$k]['goods_conversion_rate'] = empty($fields['goods_visitors']) ? 0 : round($fields['byorder_quantity_of_goods_ordered'] / $fields['goods_visitors'],2) ;
            }


            if (in_array('goods_buybox_rate', $targets)) { //购买按钮赢得率
                $lists[$k]['goods_buybox_rate']  = empty($fields['goods_views_number']) ? 0 : round($fields['byorder_buy_button_winning_num'] / $fields['goods_views_number'],2) ;
            }


            if (in_array('sale_refund_rate', $targets)) {  //退款率
                $lists[$k]['sale_refund_rate'] = empty($fields['sale_sales_volume']) ? 0 : round($fields['sale_return_goods_number'] / $fields['sale_sales_volume'],2) ;
            }

            if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
                $lists[$k]['cost_profit_profit_rate'] = empty($fields['sale_sales_quota']) ? 0 : round($fields['cost_profit_profit'] / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
                $lists[$k]['amazon_fee_rate'] =  empty($fields['sale_sales_quota']) ? 0 : round($fields['amazon_fee'] / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
                $lists[$k]['purchase_logistics_cost_rate'] =  empty($fields['sale_sales_quota']) ? 0 : round(($fields['purchase_logistics_purchase_cost'] + $fields['purchase_logistics_logistics_cost']) / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
                $lists[$k]['operate_fee_rate'] =  empty($fields['sale_sales_quota']) ? 0 : round($fields['operate_fee'] / $fields['sale_sales_quota'],2) ;
            }


            if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
                $lists[$k]['evaluation_fee_rate'] =  empty($fields['sale_sales_quota']) ? 0 : round($fields['evaluation_fee'] / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
                $lists[$k]['evaluation_fee_rate'] =  empty($fields['sale_sales_quota']) ? 0 : round($fields['cpc_cost'] / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
                $lists[$k]['cpc_order_rate'] =  empty($fields['sale_sales_volume']) ? 0 : round($fields['cpc_order_number'] / $fields['sale_sales_volume'],2) ;
            }
            if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
                $lists[$k]['cost_profit_profit_rate'] = empty($fields['cpc_click_number']) ? 0 : round($fields['cpc_order_number'] / $fields['cpc_click_number'],2) ;
            }

            if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
                $lists[$k]['cpc_turnover_rate'] = empty($fields['sale_sales_quota']) ? 0 : round($fields['cpc_turnover'] / $fields['sale_sales_quota'],2) ;
            }

            if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
                $lists[$k]['cpc_avg_click_cost'] = empty($fields['cpc_click_number']) ? 0 : round($fields['cpc_cost'] / $fields['cpc_click_number'],2) ;
            }
            if (in_array('cpc_acos', $targets)) {  // ACOS
                $lists[$k]['cpc_acos'] =  empty($fields['cpc_turnover']) ? 0 : round($fields['cpc_cost'] / $fields['cpc_turnover'],2) ;
            }

            if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
                $lists[$k]['cpc_direct_sales_volume_rate'] = empty($fields['sale_sales_volume']) ? 0 : round($fields['cpc_direct_sales_volume'] / $fields['sale_sales_volume'],2) ;
            }

            if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
                $fields['cpc_indirect_sales_volume_rate'] = empty($fields['sale_sales_volume']) ? 0 : round($fields['cpc_indirect_sales_volume'] / $fields['sale_sales_volume'],2) ;
            }

        }
    }



    protected function getTotalNum($where = '', $table = '', $group = '')
    {
        return $this->count($where, $table, $group, '', '', true);
    }

    /**
     * 获取商品级限制排名的总计where
     * @author json.qiu 20210317
     *
     * @param $where
     * @param $datas
     * @param $table
     * @param $limit
     * @param $orderby
     * @param $group
     * @return string
     */
    protected function getLimitWhere($where,$datas,$table,$limit,$orderby,$group)
    {
        if ($datas['limit_num'] <= 0){
            return $where;
        }

        switch ($datas['count_dimension']){
            //商品级
            case "parent_asin":
                if($datas['is_distinct_channel'] == '1'){
                    $field_data = "max(report.channel_id) as channel_id,max(report.goods_parent_asin) as goods_parent_asin";
                }else{
                    $field_data = "max(report.goods_parent_asin) as goods_parent_asin";
                }
                break;
            case "asin":
                if($datas['is_distinct_channel'] == '1'){
                    $field_data = "max(report.channel_id) as channel_id,max(report.goods_asin) as goods_asin";
                }else{
                    $field_data = "max(report.goods_asin) as goods_asin";
                }
                break;
            case "sku":
                if($datas['is_distinct_channel'] == '1'){
                    $field_data = "max(report.amazon_goods_id) as amazon_goods_id";
                }else{
                    $field_data = "max(report.goods_sku) as goods_sku";
                }

                break;
            case "isku":
                $field_data = "max(report.goods_isku_id) as goods_isku_id";
                break;
            case "class1":
                $field_data = "max(report.goods_product_category_name_1) as goods_product_category_name_1";
                break;
            case "group":
                $field_data = "max(report.goods_group_id) as goods_group_id";
                break;
            case "head_id":
                $field_data = "max(report.isku_head_id) as isku_head_id";
                break;
            case "developer_id":
                $field_data = "max(report.isku_developer_id) as isku_developer_id";
                break;
                //店铺级
            case "site_id":
                $field_data = "max(report.site_id) as site_id";
                break;
            case "channel_id":
                $field_data = "max(report.channel_id) as channel_id";
                break;
                //运营人员
            case "operators":
                $field_data = "max(report.goods_operation_user_admin_id) as goods_operation_user_admin_id";
                break;

            default:
                return $where;
        }
        $lists = $this->select($where,$field_data , $table, $limit, $orderby, $group);
        if (!empty($lists)){
            switch ($datas['count_dimension']){
                //商品级
                case "parent_asin":
                    if($datas['is_distinct_channel'] == '1'){
                        $channel_arr = array();
                        foreach ($lists as $v){
                            $channel_arr[$v['channel_id']][] = $v['goods_parent_asin'];
                        }
                        $where_tmp = array();
                        foreach ($channel_arr as $key => $value){
                            $where_tmp[] = " (report.channel_id = {$key} AND report.goods_parent_asin IN ( '".implode("','",$value)."' )) ";
                        }
                        $where .= " AND (".implode(" OR ",$where_tmp).")";
                    }else{
                        $where .=  " AND report.goods_parent_asin IN ( '".implode("','",array_column($lists,'goods_parent_asin'))."' )";
                    }
                    break;
                case "asin":
                    if($datas['is_distinct_channel'] == '1'){
                        $channel_arr = array();
                        foreach ($lists as $v){
                            $channel_arr[$v['channel_id']][] = $v['goods_asin'];
                        }
                        $where_tmp = array();
                        foreach ($channel_arr as $key => $value){
                            $where_tmp[] = " (report.channel_id = {$key} AND report.goods_asin IN ( '".implode("','",$value)."' )) ";
                        }
                        $where .= " AND (".implode(" OR ",$where_tmp).")";
                    }else{
                        $where .=  " AND report.goods_asin IN ( '".implode("','",array_column($lists,'goods_asin'))."' )";
                    }
                    break;
                case "sku":
                    if($datas['is_distinct_channel'] == '1'){
                        $where .=  " AND report.amazon_goods_id IN (".implode(",",array_column($lists,'amazon_goods_id')).")";
                    }else{
                        $where .=  " AND report.goods_sku IN ( '".implode("','",array_column($lists,'goods_sku'))."' ')";
                    }
                    break;
                case "isku":
                    $where .=  " AND report.goods_isku_id IN (".implode(",",array_column($lists,'goods_isku_id')).")";
                    break;
                case "class1":
                    $where .=  " AND report.goods_product_category_name_1 IN ( '".implode("','",array_column($lists,'goods_product_category_name_1'))."' )";
                    break;
                case "group":
                    $where .=  " AND report.goods_group_id IN (".implode(",",array_column($lists,'goods_group_id')).")";
                    break;
                case "head_id":
                    $where .=  " AND report.isku_head_id IN (".implode(",",array_column($lists,'isku_head_id')).")";
                    break;
                case "developer_id":
                    $where .=  " AND report.isku_developer_id IN (".implode(",",array_column($lists,'isku_developer_id')).")";
                    break;
                //店铺级
                case "site_id":
                    $where .=  " AND report.site_id IN (".implode(",",array_column($lists,'site_id')).")";
                    break;
                case "channel_id":
                    $where .=  " AND report.channel_id IN (".implode(",",array_column($lists,'channel_id')).")";
                    break;
                    //运营人员
                case "operators":
                    $where .=  " AND report.goods_operation_user_admin_id IN (".implode(",",array_column($lists,'goods_operation_user_admin_id')).")";
                    break;

                default:
                    return $where;
            }
        }
        return $where;
    }

    protected function getGoodsFbaDataTmp($lists = array() , $fields = array() , $datas = array(),$channel_arr = array())
    {
        if(empty($lists)){
            return $lists ;
        }else{
            $where = 'g.user_id = ' . $lists[0]['user_id']   ;
            if (!empty($channel_arr)){
                if (count($channel_arr)==1){
                    $where .= " AND g.channel_id = ".intval(implode(",",$channel_arr));
                }else{
                    $where .= " AND g.channel_id IN (".implode(",",$channel_arr).")";
                }
            }
            $table = "f_amazon_goods_finance_{$this->codeno} as g " ;
            if($datas['count_dimension'] == 'sku'){
                if($datas['is_distinct_channel'] == 1){
                    $fba_fields = $group = 'g.sku , g.channel_id' ;
                }else{
                    $fba_fields = $group = 'g.sku, g.fba_inventory_v3_id' ;
                }
            }else if($datas['count_dimension'] == 'asin'){
                if($datas['is_distinct_channel'] == 1){
                    $fba_fields = $group = 'g.asin , g.channel_id' ;
                }else{
                    $fba_fields = $group = 'g.asin ,g.fba_inventory_v3_id ' ;
                }
            }else if($datas['count_dimension'] == 'parent_asin'){
                if($datas['is_distinct_channel'] == 1){
                    $fba_fields = $group = 'g.parent_asin , g.channel_id' ;
                }else{
                    $fba_fields = $group = 'g.parent_asin ,g.fba_inventory_v3_id ' ;
                }
            }else if($datas['count_dimension'] == 'isku'){
                $fba_fields = $group = 'g.isku_id ,g.fba_inventory_v3_id' ;
            }else if($datas['count_dimension'] == 'class1'){
                $fba_fields = $group = 'g.product_category_name_1 ,g.fba_inventory_v3_id' ;
            }else if($datas['count_dimension'] == 'group'){ //分组
                $fba_fields = $group = 'g.group_id ,g.fba_inventory_v3_id' ;
            }

            $where_arr = array() ;
            foreach($lists as $list1){
                if($datas['count_dimension'] == 'sku'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('sku' => AmazonGoodsFinanceMysqlModel::escape($list1['sku']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('sku' => AmazonGoodsFinanceMysqlModel::escape($list1['sku']));
                    }
                }else if($datas['count_dimension'] == 'asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('asin' => AmazonGoodsFinanceMysqlModel::escape($list1['asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('asin' => AmazonGoodsFinanceMysqlModel::escape($list1['asin']));
                    }
                }else if($datas['count_dimension'] == 'parent_asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('parent_asin' => AmazonGoodsFinanceMysqlModel::escape($list1['parent_asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('parent_asin' => AmazonGoodsFinanceMysqlModel::escape($list1['parent_asin']));
                    }
                }else if($datas['count_dimension'] == 'class1'){
                    $where_arr[] = array('goods_product_category_name_1'=>$list1['class1'] ,  'site_id'=>$list1['site_id']) ;
                }else if($datas['count_dimension'] == 'group'){
                    $where_arr[] = array('group_id'=>$list1['group_id'] ,  'site_id'=>$list1['site_id']) ;
                }else if($datas['count_dimension'] == 'tags'){  //标签
                    $where_arr[] = array('tags_id'=>$list1['tags_id']) ;
                }else if($datas['count_dimension'] == 'head_id'){  //负责人
                    $where_arr[] = array('head_id'=>$list1['head_id']) ;
                }else if($datas['count_dimension'] == 'developer_id'){ //开发人
                    $where_arr[] = array('developer_id'=>$list1['developer_id']) ;
                }else if($datas['count_dimension'] == 'isku'){ //开发人
                    $where_arr[] = array('isku_id'=>$list1['isku_id']) ;
                }
            }

            if($datas['count_dimension'] == 'sku' || $datas['count_dimension'] == 'asin' || $datas['count_dimension'] == 'parent_asin'){
                if($datas['is_distinct_channel'] == 1) {
                    $whereDatas = array() ;
                    foreach($where_arr as $wheres){
                        $whereDatas[$wheres['channel_id']][] = $wheres[$datas['count_dimension']] ;
                    }
                    $where_strs = array() ;
                    foreach($whereDatas as $cid => $wd){
                        $str = "'" . implode("','" , $wd) . "'" ;
                        $where_strs[] = '( g.channel_id = ' . $cid . ' AND g.'.$datas['count_dimension'] . ' IN (' . $str . '))' ;
                    }
                    $where_str = "(".implode(' OR ' , $where_strs).")" ;

                }else{
                    $where_strs = array_unique(array_column($where_arr , $datas['count_dimension'])) ;
                    $str = "'" . implode("','" , $where_strs) . "'" ;
                    $where_str = 'g.'.$datas['count_dimension'].' IN (' . $str . ') ' ;
                }
            }else if($datas['count_dimension'] == 'class1'){
                $where_strs = array_unique(array_column($where_arr , 'goods_product_category_name_1')) ;
                $str = "'" . implode("','" , $where_strs) . "'" ;
                $where_str = 'g.product_category_name_1 IN (' . $str . ') ' ;
            }else if($datas['count_dimension'] == 'group'){
                $where_strs = array_unique(array_column($where_arr , 'group_id')) ;
                $where_str = 'g.group_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'isku'){
                $where_strs = array_unique(array_column($where_arr , 'isku_id')) ;
                $where_str = 'g.isku_id IN (' . implode(',' , $where_strs) . ' ) ';
            }
        }
        $where.= ' AND ' . $where_str." AND g.fba_inventory_v3_id > 0  AND g.\"Transport_mode\" = 2" ;
        if(isset($datas['where_detail']) && $datas['where_detail']){
            if (!is_array($datas['where_detail'])){
                $datas['where_detail'] = json_decode($datas['where_detail'],true);
            }
            if ($datas['where_detail']['group_id'] && !empty(trim($datas['where_detail']['group_id']))){
                $where .= ' AND g.group_id IN (' . $datas['where_detail']['group_id'] . ') ' ;
            }
            if ($datas['where_detail']['transport_mode'] && !empty(trim($datas['where_detail']['transport_mode']))){
                $where .= ' AND g.Transport_mode = ' . ($datas['where_detail']['transport_mode'] == 'FBM' ? 1 : 2);
            }
            if ($datas['where_detail']['is_care'] && !empty(trim($datas['where_detail']['is_care']))){
                $where .= ' AND g.is_care = ' . (intval($datas['where_detail']['is_care'])==1?1:0);
            }
        }

        $fba_fields .= ' , SUM(DISTINCT(CASE WHEN g.fulfillable_quantity < 0 THEN 0 ELSE g.fulfillable_quantity END )) as fba_sales_stock ,MAX(DISTINCT( CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as  fba_sales_day , MAX(DISTINCT(g.available_days) ) as max_fba_sales_day , MIN( DISTINCT(g.available_days) ) as min_fba_sales_day , MIN(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END ))  as min_egt0_fba_sales_day , MAX(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as max_egt0_fba_sales_day , SUM(DISTINCT(CASE WHEN g.reserved_quantity < 0 THEN 0 ELSE g.reserved_quantity END )) as fba_reserve_stock  , SUM(DISTINCT( CASE WHEN g.replenishment_quantity < 0 THEN 0 ELSE g.replenishment_quantity END ))  as fba_recommended_replenishment , MAX( DISTINCT(g.replenishment_quantity) ) as max_fba_recommended_replenishment ,MIN( DISTINCT(g.replenishment_quantity) ) as min_fba_recommended_replenishment , SUM(DISTINCT( CASE WHEN g.available_stock < 0 THEN 0 ELSE g.available_stock END )) as fba_special_purpose , MAX( DISTINCT(g.available_stock)) as  max_fba_special_purpose , MIN(DISTINCT( g.available_stock) )  as min_fba_special_purpose ';

        $goods_finance_md = new AmazonGoodsFinanceMysqlModel([], $this->dbhost, $this->codeno);
        $goods_finance_md->dryRun(env('APP_TEST_RUNNING', false));
        $fbaData =$goods_finance_md->select($where, $fba_fields, $table, '', '', $group);
        $fbaDatas = array() ;
        if (!empty($fbaData)){
            foreach($fbaData as $fba){
                if($datas['count_dimension'] == 'sku'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'sku',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'asin'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'asin',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'parent_asin'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'parent_asin',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'class1'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'product_category_name_1',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'group'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'group_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'tags'){  //标签（需要刷数据）
                    $fbaDatas = $this->handleGoodsFbaData($fba,'tags_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'head_id'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'head_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'developer_id'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'developer_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'isku'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'isku_id',$datas['is_distinct_channel'],$fbaDatas);
                }

            }
        }

        foreach($lists as $k=>$list2){
            if($datas['count_dimension'] == 'sku'){
                if($datas['is_distinct_channel'] == 1) {
                    $fba_data = $fbaDatas[$list2['sku'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = $fbaDatas[$list2['sku']];
                }
            }else if($datas['count_dimension'] == 'asin'){
                if($datas['is_distinct_channel'] == 1) {
                    $fba_data = $fbaDatas[$list2['asin'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = $fbaDatas[$list2['asin']];
                }
            }else if($datas['count_dimension'] == 'parent_asin'){
                if($datas['is_distinct_channel'] == 1) {
                    $fba_data = $fbaDatas[$list2['parent_asin'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = $fbaDatas[$list2['parent_asin']];
                }
            }else if($datas['count_dimension'] == 'class1'){
                $fba_data = $fbaDatas[$list2['class1']] ;
            }else if($datas['count_dimension'] == 'group'){
                $fba_data = $fbaDatas[$list2['group_id']] ;
            }else if($datas['count_dimension'] == 'tags'){  //标签（需要刷数据）
                $fba_data = $fbaDatas[$list2['tags_id']] ;
            }else if($datas['count_dimension'] == 'head_id'){
                $fba_data = $fbaDatas[$list2['head_id']] ;
            }else if($datas['count_dimension'] == 'developer_id'){
                $fba_data = $fbaDatas[$list2['developer_id']] ;
            }else if($datas['count_dimension'] == 'isku'){
                $fba_data = $fbaDatas[$list2['isku_id']] ;
            }

            if (!empty($fields['fba_sales_stock'])) {  //可售库存
                $lists[$k]['fba_sales_stock'] = empty($fba_data) ? null : $fba_data['fba_sales_stock'] ;
            }
            if (!empty($fields['fba_sales_day'])) {  //可售天数
                $lists[$k]['fba_sales_day'] = empty($fba_data) ? null : $fba_data['fba_sales_day'] ;
                $lists[$k]['max_fba_sales_day'] = empty($fba_data) ? null : $fba_data['max_fba_sales_day'] ;
                $lists[$k]['min_fba_sales_day'] = empty($fba_data) ? null : $fba_data['min_fba_sales_day'] ;
                $lists[$k]['min_egt0_fba_sales_day'] = empty($fba_data) ? null : $fba_data['min_egt0_fba_sales_day'] ;
                $lists[$k]['max_egt0_fba_sales_day'] = empty($fba_data) ? null : $fba_data['max_egt0_fba_sales_day'] ;
            }
            if (!empty($fields['fba_reserve_stock'])) {  //预留库存
                $lists[$k]['fba_reserve_stock'] = empty($fba_data) ? null : $fba_data['fba_reserve_stock'] ;
            }
            if (!empty($fields['fba_recommended_replenishment'])) {  //建议补货量
                $lists[$k]['fba_recommended_replenishment'] = empty($fba_data) ? null : round($fba_data['fba_recommended_replenishment'],2) ;
                $lists[$k]['max_fba_recommended_replenishment'] = empty($fba_data) ? null : $fba_data['max_fba_recommended_replenishment'] ;
                $lists[$k]['min_fba_recommended_replenishment'] = empty($fba_data) ? null : $fba_data['min_fba_recommended_replenishment'] ;
            }
            if (!empty($fields['fba_special_purpose'])) {  //FBA专用
                $lists[$k]['fba_special_purpose'] = empty($fba_data) ? null : $fba_data['fba_special_purpose'] ;
                $lists[$k]['max_fba_special_purpose'] = empty($fba_data) ? null : $fba_data['max_fba_special_purpose'] ;
                $lists[$k]['min_fba_special_purpose'] = empty($fba_data) ? null : $fba_data['min_fba_special_purpose'] ;
            }

        }
        return $lists;
    }

    protected function handleGoodsFbaData($fba, $field, $is_distinct_channel = 0, $fbaDatas = array())
    {
        if($is_distinct_channel == 1 && ($field == 'sku' || $field == 'asin' || $field == 'parent_asin')){
            $fbaDatas[$fba[$field].'-'.$fba['channel_id']] = $fba ;
        } else {
            $fbaDatas[$fba[$field]]['fba_sales_stock']+= $fba['fba_sales_stock'] ;

            $fbaDatas[$fba[$field]]['fba_sales_day'] = ($fbaDatas[$fba[$field]]['fba_sales_day'] > $fba['fba_sales_day']) ? $fbaDatas[$fba[$field]]['fba_sales_day'] : $fba['fba_sales_day'] ;

            $fbaDatas[$fba[$field]]['max_fba_sales_day'] = ($fbaDatas[$fba[$field]]['max_fba_sales_day'] > $fba['max_fba_sales_day']) ? $fbaDatas[$fba[$field]]['max_fba_sales_day'] : $fba['max_fba_sales_day'] ;

            $fbaDatas[$fba[$field]]['min_fba_sales_day'] = ($fbaDatas[$fba[$field]]['min_fba_sales_day'] < $fba['min_fba_sales_day']) ? $fbaDatas[$fba[$field]]['min_fba_sales_day'] : $fba['min_fba_sales_day'] ;

            $fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] = ($fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] > $fba['max_egt0_fba_sales_day']) ? $fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] : $fba['max_egt0_fba_sales_day'] ;

            $fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] = ($fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] < $fba['min_egt0_fba_sales_day']) ? $fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] : $fba['min_egt0_fba_sales_day'] ;

            $fbaDatas[$fba[$field]]['fba_reserve_stock'] += $fba['fba_reserve_stock'] ;
            $fbaDatas[$fba[$field]]['fba_recommended_replenishment'] += $fba['fba_recommended_replenishment'] ;
            $fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'] = ($fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'] < $fba['max_fba_recommended_replenishment']) ? $fba['max_fba_recommended_replenishment']:$fbaDatas[$fba[$field]]['max_fba_recommended_replenishment']   ;
            $fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] = ($fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] < $fba['min_fba_recommended_replenishment']) ? $fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] : $fba['min_fba_recommended_replenishment'] ;
            $fbaDatas[$fba[$field]]['fba_special_purpose'] += $fba['fba_special_purpose'] ;
            $fbaDatas[$fba[$field]]['max_fba_special_purpose'] = ($fbaDatas[$fba[$field]]['max_fba_special_purpose'] > $fba['max_fba_special_purpose']) ? $fbaDatas[$fba[$field]]['max_fba_special_purpose'] : $fba['max_fba_special_purpose'] ;
            $fbaDatas[$fba[$field]]['min_fba_special_purpose'] = ($fbaDatas[$fba[$field]]['min_fba_special_purpose'] < $fba['min_fba_special_purpose']) ? $fbaDatas[$fba[$field]]['min_fba_special_purpose'] : $fba['min_fba_special_purpose'] ;
        }

        return $fbaDatas;
    }

    //获取商品维度指标字段(新增统计维度完成)
    private function getGoodsFields($datas = array())
    {
        $fields = array();
        $fields = $this->getGoodsTheSameFields($datas,$fields);

        if ($datas['count_periods'] == '1' && $datas['show_type'] == '2') { //按天
            $fields['time'] = "concat_ws('-', report.myear, report.mmonth, report.mday)";
        } else if ($datas['count_periods'] == '2' && $datas['show_type'] == '2') { //按周
            $fields['time'] = "concat_ws('-', report.mweekyear, report.mweek)";
        } else if ($datas['count_periods'] == '3' && $datas['show_type'] == '2') { //按月
            $fields['time'] = "concat_ws('-', report.myear, report.mmonth)";
        } else if ($datas['count_periods'] == '4' && $datas['show_type'] == '2') {  //按季
            $fields['time'] = "concat_ws('-', report.myear, report.mquarter)";
        } else if ($datas['count_periods'] == '5' && $datas['show_type'] == '2') { //按年
            $fields['time'] = "report.myear";
        }

        $targets = explode(',', $datas['target']);
        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }
        if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
            $fields['byorder_quantity_of_goods_ordered'] = 'SUM(byorder_quantity_of_goods_ordered)' ;
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }

        if (in_array('goods_rank', $targets)) { //大类目rank
            $fields['goods_rank'] = "min(nullif(report.goods_rank,0))";
        }
        if (in_array('goods_min_rank', $targets)) { //小类目rank
            $fields['goods_min_rank'] = " min(nullif(report.goods_min_rank,0))";
        }
        if (in_array('goods_views_number', $targets)) { //页面浏览次数
            $fields['goods_views_number'] = " SUM ( report.byorder_number_of_visits ) ";
        }

        if (in_array('goods_buybox_rate', $targets)) { //购买按钮赢得率
            $fields['byorder_buy_button_winning_num'] = "SUM(byorder_buy_button_winning_num)" ;
            $fields['goods_views_number'] = " SUM ( report.byorder_number_of_visits ) ";
        }
        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_volume'] = " SUM ( report.byorder_sales_volume +  report.byorder_group_id ) ";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_volume'] = " SUM ( report.report_sales_volume +  report.report_group_id ) ";
            }
        }
        if (in_array('sale_many_channel_sales_volume', $targets)) { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_many_channel_sales_volume'] = "SUM ( report.byorder_group_id )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_many_channel_sales_volume'] = "SUM ( report.report_group_id )";
            }
        }
        if (in_array('sale_sales_quota', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('amazon_fee_rate', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('operate_fee_rate', $targets) || in_array('evaluation_fee_rate', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_turnover_rate', $targets)) {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
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
                $fields['sale_refund'] = "SUM ( 0 - report.byorder_refund )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率

        }

        if (in_array('promote_discount', $targets)) {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                $fields['promote_discount'] = "SUM(report.byorder_promote_discount)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['promote_discount'] = "SUM(report.report_promote_discount)";
            }
        }
        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
            }
        }

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost ) ";
                } else {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost ) ";
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost ) ";
                } else {
                    $fields['purchase_logistics_purchase_cost'] = " SUM (report.first_purchasing_cost ) ";
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course ) ";
                } else {
                    $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course ) ";
                } else {
                    $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                }
            }
        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
            } else {
                $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
            }

        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率

        }

        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets) || in_array('cost_profit_total_income',$targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fee'] = 'SUM (report.byorder_goods_amazon_fee)';
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fee'] = 'SUM (report.report_goods_amazon_fee)';
            }
        }

        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission + report.byorder_reserved_field21) ";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission + report.report_reserved_field21 ) ";
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_other_fee'] = "SUM(report.byorder_goods_amazon_other_fee)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_other_fee'] = "SUM(report.report_goods_amazon_other_fee)";
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.byorder_estimated_monthly_storage_fee )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.report_estimated_monthly_storage_fee )";
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee)';
            } else {
                $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee)';
            }
        }
        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
        }

        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比

        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            $fields['operate_fee'] = "SUM ( 0- report.byorder_reserved_field16 ) ";
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比

        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 ) ";
            }else{
                $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比

        }

        if (in_array('cpc_sp_cost', $targets)) {  //CPC_SP花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE}) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets)) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE}) ";
            }
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比

        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 )";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks )";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = '('.$fields['cpc_click_number'].')' . " / nullif( " . $fields['cpc_exposure'] . " , 0 ) ";
        }
        if (in_array('cpc_order_number', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'SUM ( report.byorder_sp_attributedconversions7d + report.byorder_sd_attributedconversions7d ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比

        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            $fields['cpc_turnover'] = 'SUM ( report.byorder_sp_attributedsales7d + report.byorder_sd_attributedsales7d  )';
        }
        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'SUM ( report.byorder_sd_attributedconversions7dsamesku + report.byorder_sp_attributedconversions7dsamesku )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            $fields['cpc_direct_sales_quota'] = 'SUM ( report.byorder_sd_attributedsales7dsamesku + report.byorder_sp_attributedsales7dsamesku )';
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比

        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'SUM ( report.byorder_sp_attributedconversions7d + report.byorder_sd_attributedconversions7d - report.byorder_sd_attributedconversions7dsamesku - report.byorder_sp_attributedconversions7dsamesku ) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            $fields['cpc_indirect_sales_quota'] = 'SUM (report.byorder_sd_attributedsales7d + report.byorder_sp_attributedsales7d  - report.byorder_sd_attributedsales7dsamesku - report.byorder_sp_attributedsales7dsamesku  )';
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = '(' . $fields['cpc_indirect_sales_volume'] . ") / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('other_vat_fee', $targets)) { //VAT
            if($datas['finance_datas_origin'] == 1){
                $fields['other_vat_fee'] = "SUM(0-report.byorder_reserved_field17)";
            }else{
                $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
            }
        }

        if (in_array('fba_sales_stock', $targets)) {  //可售库存
            $fields['fba_sales_stock'] = '1';
        }
        if (in_array('fba_sales_day', $targets)) {  //可售天数
            $fields['fba_sales_day'] = '1';
        }
        if (in_array('fba_reserve_stock', $targets)) {  //预留库存
            $fields['fba_reserve_stock'] = '1';
        }
        if (in_array('fba_recommended_replenishment', $targets)) {  //建议补货量
            $fields['fba_recommended_replenishment'] = '1';
        }
        if (in_array('fba_special_purpose', $targets)) {  //FBA专用
            $fields['fba_special_purpose'] = '1';
        }

        if (in_array('cost_profit_total_pay', $targets) ) {   //总支出
            if ($datas['finance_datas_origin'] == '1') {
                if($datas['cost_count_type'] == '1'){
                    $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.byorder_purchasing_cost +  report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                }else{
                    $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                }
            } else {
                if($datas['cost_count_type'] == '1'){
                    $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.report_purchasing_cost +  report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                }else{
                    $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                }
            }
        }

        if (in_array('cost_profit_total_income', $targets) ) {   //总收入
            if ($datas['sale_datas_origin'] == '1') {
                $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota )";
            }

            if ($datas['finance_datas_origin'] == '1') {
                $fields['cost_profit_total_income'] = $fields['cost_profit_total_income'] . " + SUM(report.byorder_refund_promote_discount)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] ." + SUM(report.report_refund_promote_discount)";
            }

        }
        $this->getUnTimeFields($fields,$datas,$targets);

        return $fields;
    }

    private function getGoodsTheSameFields($datas,$fields){
        $fields['user_id'] = 'max(report.user_id)';
        $fields['goods_id'] = 'max(report.amazon_goods_id)';
        $fields['site_country_id'] = 'max(report.site_id)';

        if (in_array($datas['count_dimension'],['parent_asin','asin','sku','isku'])){
            $fields['goods_price_min'] = 'min(report.goods_price)';
            $fields['goods_price_max'] = 'max(report.goods_price)';
            $fields['min_transport_mode'] = ' min(report.goods_transport_mode) ' ;
            $fields['max_transport_mode'] = ' max(report.goods_transport_mode) ' ;
        }

        if ($datas['count_dimension'] == 'parent_asin') {
            $fields['parent_asin'] = "max(report.goods_parent_asin)";
            if($datas['is_distinct_channel'] == '1'){
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
            }
            $fields['goods_is_care']                 = 'max(report.goods_is_care)';
            $fields['goods_is_new']                  = 'max(report.goods_is_new)';
            $fields['up_status']                  = 'max(report.goods_up_status)';
            $fields['is_remarks']       = 'max(report.goods_is_remarks)';
            $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
        }else if ($datas['count_dimension'] == 'asin') {
            $fields['asin'] = "max(report.goods_asin)";
            if($datas['is_distinct_channel'] == '1'){
                $fields['parent_asin'] = "max(report.goods_parent_asin)";
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
            }
            $fields['goods_is_care']                 = 'max(report.goods_is_care)';
            $fields['goods_is_new']                  = 'max(report.goods_is_new)';
            $fields['up_status']                  = 'max(report.goods_up_status)';
            $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
            $fields['is_remarks']       = 'max(report.goods_is_remarks)';
        }else if ($datas['count_dimension'] == 'sku') {
            $fields['sku'] = "max(report.goods_sku)";
            if($datas['is_distinct_channel'] == '1'){
                $fields['asin'] = "max(report.goods_asin)";
                $fields['parent_asin'] = "max(report.goods_parent_asin)";
                $fields['goods_product_category_name_1'] = 'max(report.goods_product_category_name_1)';
                $fields['goods_product_category_name_2'] = 'max(report.goods_product_category_name_2)';
                $fields['goods_product_category_name_3'] = 'max(report.goods_product_category_name_3)';
                $fields['goods_is_care']                 = 'max(report.goods_is_care)';
                $fields['goods_is_new']                  = 'max(report.goods_is_new)';
                $fields['up_status']                  = 'max(report.goods_up_status)';

                $fields['isku_id']                       = 'max(report.goods_isku_id)';
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
                $fields['class1'] = 'max(report.goods_product_category_name_1)';
                $fields['goods_operation_user_admin_id'] = 'max(report.goods_operation_user_admin_id)';
            }
            $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
            $fields['is_remarks']       = 'max(report.goods_is_remarks)';
        } else if ($datas['count_dimension'] == 'isku') {
            $fields['isku_id'] = 'max(report.goods_isku_id)';
        }else if ($datas['count_dimension'] == 'class1') {
            $fields['class1'] = 'max(report.goods_product_category_name_1)';
            $fields['class1_id'] = 'max(report.goods_product_category_id_1)';
        } else if ($datas['count_dimension'] == 'group') {
            $fields['group_id'] = 'max(report.goods_group_id)';
            $fields['group'] = 'max(report.goods_group_name)';
        } else if ($datas['count_dimension'] == 'head_id') {
            $fields['head_id'] = 'max(report.isku_head_id)';
        } else if ($datas['count_dimension'] == 'developer_id') {
            $fields['developer_id'] = 'max(report.isku_developer_id)';
        }

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
    private function getUnTimeFields(&$fields,$datas,$targets,$type = 1){
        if (in_array('fba_sales_quota', $targets)) { //FBA商品销售额
            if($datas['sale_datas_origin'] == 1){
                $fields['fba_sales_quota'] = "SUM(report.byorder_fba_sales_quota)";
            }else{
                $fields['fba_sales_quota'] = "SUM(report.report_fba_sales_quota)";
            }
        }

        if (in_array('fbm_sales_quota', $targets)) { //FBM商品销售额
            if($datas['sale_datas_origin'] == 1){
                $fields['fbm_sales_quota'] = "SUM(report.byorder_fbm_sales_quota)";
            }else{
                $fields['fbm_sales_quota'] = "SUM(report.report_fbm_sales_quota)";
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
                    $fields['fba_logistics_head_course'] = "SUM(report.byorder_fba_logistics_head_course)";
                }else{
                    $fields['fba_logistics_head_course'] = "SUM(report.report_fba_logistics_head_course)";
                }
            }else{
                $fields['fba_logistics_head_course'] = "SUM(report.fba_first_logistics_head_course)";
            }

        }

        if (in_array('fbm_logistics_head_course', $targets)) { //fbm物流
            if($datas['cost_count_type'] == 1){
                if($datas['finance_datas_origin'] == 1){
                    $fields['fbm_logistics_head_course'] = "SUM(report.byorder_fbm_logistics_head_course)";
                }else{
                    $fields['fbm_logistics_head_course'] = "SUM(report.report_fbm_logistics_head_course)";
                }
            }else{
                $fields['fbm_logistics_head_course'] = "SUM(report.fbm_first_logistics_head_course)";
            }

        }

        if (in_array('shipping_charge', $targets)) { //运费
            if($datas['finance_datas_origin'] == 1){
                $fields['shipping_charge'] = "SUM(report.byorder_shipping_charge)";
            }else{
                $fields['shipping_charge'] = "SUM(report.report_shipping_charge)";
            }
        }


        if (in_array('tax', $targets)) { //TAX（销售）
            if($datas['finance_datas_origin'] == 1){
                $fields['tax'] = "SUM(report.byorder_tax)";
            }else{
                $fields['tax'] = "SUM(report.report_tax)";
            }
        }

        if (in_array('ware_house_lost', $targets)) { //FBA仓丢失赔款
            if($datas['finance_datas_origin'] == 1){
                $fields['ware_house_lost'] = "SUM(report.byorder_ware_house_lost)";
            }else{
                $fields['ware_house_lost'] = "SUM(report.report_ware_house_lost)";
            }
        }

        if (in_array('ware_house_damage', $targets)) { //FBA仓损坏赔款
            if($datas['finance_datas_origin'] == 1){
                $fields['ware_house_damage'] = "SUM(report.byorder_ware_house_damage)";
            }else{
                $fields['ware_house_damage'] = "SUM(report.report_ware_house_damage)";
            }
        }

        if ($type == 2 or $type == 3){//店铺和运营人员才有的
            if (in_array('channel_fbm_safe_t_claim_demage', $targets)) { //SAF-T
                $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage)";
            }
        }
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
    public function getListByUnGoods(
        $where = '',
        $params = [],
        $limit = 0,
        $sort = '',
        $order = '',
        $count_tip = 0,
        array $channel_arr = [],
        array $currencyInfo = [],
        $exchangeCode = '1',
        array $timeLine = [],
        array $deparmentData = [],
        int $userId = 0,
        int $adminId = 0
    ) {
        $fields = [];
        //没有按周期统计 ， 按指标展示
        if ($params['show_type'] == 2) {
            $fields = $this->getUnGoodsFields($params);
        }

        if (empty($fields)) {
            return [];
        }

        $table = "f_dw_channel_day_report_{$this->dbhost} AS report";

        $where .= " AND report.user_id_mod = " . ($params['user_id'] % 20);

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
        }else if($params['count_dimension'] == 'all_channels') { //按全部店铺维度统计
            if ($params['count_periods'] > 0 && $params['show_type'] == '2') {
                if ($params['count_periods'] == '1') { //按天
                    $group = 'report.myear , report.mmonth  , report.mday';
                    $orderby = 'report.myear , report.mmonth  , report.mday';
                } else if ($params['count_periods'] == '2') { //按周
                    $group = 'report.mweekyear , report.mweek';
                    $orderby = 'report.mweekyear , report.mweek';
                } else if ($params['count_periods'] == '3') { //按月
                    $group = 'report.myear , report.mmonth';
                    $orderby = 'report.myear , report.mmonth';
                } else if ($params['count_periods'] == '4') {  //按季
                    $group = 'report.myear , report.mquarter';
                    $orderby = 'report.myear , report.mquarter';
                } else if ($params['count_periods'] == '5') { //按年
                    $group = 'report.myear';
                    $orderby = 'report.myear';
                }
            } else {
                $group = 'report.user_id  ';
            }
        }

        if (!empty($where_detail)) {
            $target_wheres = $where_detail['target'] ?? '';
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
        }

        if ($datas['count_periods'] == '1' && $datas['show_type'] == '2') { //按天
            $fields['time'] = "concat_ws('-', report.myear, report.mmonth, report.mday)";
        } else if ($datas['count_periods'] == '2' && $datas['show_type'] == '2') { //按周
            $fields['time'] = "concat_ws('-', report.mweekyear, report.mweek)";
        } else if ($datas['count_periods'] == '3' && $datas['show_type'] == '2') { //按月
            $fields['time'] = "concat_ws('-', report.myear, report.mmonth)";
        } else if ($datas['count_periods'] == '4' && $datas['show_type'] == '2') {  //按季
            $fields['time'] = "concat_ws('-', report.myear, report.mquarter)";
        } else if ($datas['count_periods'] == '5' && $datas['show_type'] == '2') { //按年
            $fields['time'] = "report.myear";
        }

        if (is_array($datas['target'])) {
            $targets = $datas['target'];
        } else {
            $targets = explode(',', $datas['target']);
        }
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
                $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
            }
        }

        //订单金额
        if (in_array('sale_sales_dollars', $targets) || in_array('cpc_cost_rate', $targets)) {
            $fields['sale_sales_dollars'] = "SUM ( report.bychannel_sales_quota )";
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
                $fields['sale_refund'] = "SUM ( 0 - report.byorder_refund )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = '('.$fields['sale_return_goods_number'] . ") * 1.0 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('promote_discount', $targets)) {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                $fields['promote_discount'] = "SUM(report.byorder_promote_discount)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['promote_discount'] = "SUM(report.report_promote_discount)";
            }
        }
        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
            }
        }

        if (in_array('promote_store_fee', $targets)) { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
            }
        }

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost ) ";
                } else {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ((report.first_purchasing_cost) ) ";
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost ) ";
                } else {
                    $fields['purchase_logistics_purchase_cost'] = " SUM ((report.first_purchasing_cost) ) ";
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course ) ";
                } else {
                    $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course ) ";
                } else {
                    $fields['purchase_logistics_logistics_cost'] = " SUM ( ( report.first_logistics_head_course) ) ";
                }
            }
        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                $fields['cost_profit_profit'] = "SUM(report.byorder_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
            } else {
                $fields['cost_profit_profit'] = "SUM(report.report_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
            }
        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = "({$fields['cost_profit_profit']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
            }
        }
        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission ) ";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission ) ";
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_other_fee'] = "SUM ( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_other_fee'] = "SUM ( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
            } elseif ($datas['finance_datas_origin'] == '2') {
                $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
            }
        }
        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = "({$fields['amazon_fee']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }


        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = "({$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}) / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            $fields['operate_fee'] = "SUM ( report.bychannel_operating_fee ) ";
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = "({$fields['operate_fee']})/nullif({$fields['sale_sales_quota']}, 0)";
        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1'){
                $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 ) ";
            }else{
                $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = "({$fields['evaluation_fee']})/nullif({$fields['sale_sales_quota']}, 0) ";
        }

        if (in_array('other_vat_fee', $targets)) {//VAT
            if($datas['finance_datas_origin'] == 1){
                $fields['other_vat_fee'] = "SUM(0-report.byorder_reserved_field17)";
            }else{
                $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
            }

        }

        if (in_array('other_other_fee', $targets)) { //其他
            $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  + report.bychannel_review_enrollment_fee)";
        }

        if (in_array('other_review_enrollment_fee', $targets)) { //早期评论者计划
            $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee)";
        }

        if (in_array('cpc_ad_settlement', $targets)) { //广告结款
            $fields['cpc_ad_settlement'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
        }

        if (in_array('cpc_sp_cost', $targets)) {  //CPC_SP花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE}) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets)) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE}) ";
            }
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
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
            $fields['cpc_order_number'] = 'SUM ( report.byorder_sp_attributedconversions7d + report.byorder_sd_attributedconversions7d + report.bychannel_reserved_field7 ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
            $fields['cpc_order_rate'] = "({$fields['cpc_order_number']})/nullif(SUM(report.bychannel_sales_volume), 0) ";
        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = "({$fields['cpc_order_number']})/nullif({$fields['cpc_click_number']}, 0) ";
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            $fields['cpc_turnover'] = 'SUM ( report.byorder_sp_attributedsales7d + report.byorder_sd_attributedsales7d + report.bychannel_reserved_field5 )';
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
            $fields['cpc_direct_sales_volume'] = 'SUM ( report.byorder_sd_attributedconversions7dsamesku + report.byorder_sp_attributedconversions7dsamesku + report.bychannel_reserved_field8 )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            $fields['cpc_direct_sales_quota'] = 'SUM ( report.byorder_sd_attributedsales7dsamesku + report.byorder_sp_attributedsales7dsamesku + report.bychannel_reserved_field6 )';
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = "({$fields['cpc_direct_sales_volume']})/nullif({$fields['sale_sales_volume']}, 0) ";
        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'SUM ( report.byorder_sp_attributedconversions7d + report.byorder_sd_attributedconversions7d + report.bychannel_reserved_field7 - report.byorder_sd_attributedconversions7dsamesku - report.byorder_sp_attributedconversions7dsamesku - report.bychannel_reserved_field8) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            $fields['cpc_indirect_sales_quota'] = 'SUM (report.byorder_sd_attributedsales7d + report.byorder_sp_attributedsales7d + report.bychannel_reserved_field5 - report.byorder_sd_attributedsales7dsamesku - report.byorder_sp_attributedsales7dsamesku - report.bychannel_reserved_field6 )';
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = "({$fields['cpc_indirect_sales_volume']})/nullif({$fields['sale_sales_volume']}, 0) ";
        }

        if (in_array('fba_goods_value', $targets)) {  //在库总成本
            $fields['fba_goods_value'] = '1';

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
            $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee + report.bychannel_coupon_payment_eventlist_tax)';
        }
        if (in_array('promote_run_lightning_deal_fee', $targets)) {  //RunLightningDealFee';
            $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
        }
        if (in_array('amazon_order_fee', $targets)) {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
            } else {
                $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';

            }
        }
        if (in_array('amazon_refund_fee', $targets)) { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
            } else {
                $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
            }
        }
        if (in_array('amazon_stock_fee', $targets)) { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_stock_fee'] = 'SUM(report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
            } else {
                $fields['amazon_stock_fee'] = 'SUM(report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
            } else {
                $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
            }
        }
        if (in_array('goods_adjust_fee', $targets)) { //商品调整费用

            if ($datas['finance_datas_origin'] == '1') {
                $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
            } else {
                $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
            }

        }

        $this->getUnTimeFields($fields, $datas, $targets, 2);

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
    protected function getUnGoodsFbaData($lists = [], $fields = [], $datas = [], $channel_arr = [], $currencyInfo = [], $exchangeCode = '1')
    {
        if(empty($lists)){
            return $lists ;
        } else {
            $table = "f_amazon_fba_inventory_by_channel_{$this->codeno} as c";
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

            if($datas['count_dimension'] == 'channel_id'){
                $where_strs = array_unique(array_column($where_arr , 'channel_id')) ;
                $where_str = 'c.channel_id IN (' . implode(',' , $where_strs) . ")" ;
            }else if($datas['count_dimension'] == 'site_id'){
                $where_strs = array_unique(array_column($where_arr , 'site_id')) ;
                $where_str = 'c.site_id IN (' . implode(',' , $where_strs) . ")" ;
            }
        }

        $amazon_fba_inventory_by_channel_md = new AmazonFbaInventoryByChannelMySQLModel([], $this->dbhost, $this->codeno);
        $amazon_fba_inventory_by_channel_md->dryRun(env('APP_TEST_RUNNING', false));
        $where.= ' AND ' . $where_str ;
        $fba_fields .= " , SUM ( DISTINCT (c.yjzhz) )  as fba_goods_value";
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


}
