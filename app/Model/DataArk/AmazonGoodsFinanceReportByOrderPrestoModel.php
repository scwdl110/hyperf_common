<?php

namespace App\Model\DataArk;

use App\Model\UserAdminModel;
use App\Model\AbstractPrestoModel;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;

class AmazonGoodsFinanceReportByOrderPrestoModel extends AbstractPrestoModel
{
    protected $table = 'table_amazon_goods_finance_report_by_order';

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
        int $adminId = 0 ,
        array $rateInfo = []
    ) {
        //没有按周期统计 ， 按指标展示
        if ($datas['show_type'] == 2) {
            $fields = $this->getGoodsFields($datas);
        } else {
            $fields = $this->getGoodsTimeFields($datas, $timeLine);
        }

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

        $mod_where = "report.user_id_mod = " . ($datas['user_id'] % 20);

        $ym_where = $this->getYnWhere($datas['max_ym'] , $datas['min_ym'] ) ;

        if(($datas['count_periods'] == 0 || $datas['count_periods'] == 1) && $datas['cost_count_type'] != 2){ //按天或无统计周期
            $table = "{$this->table_goods_day_report} AS report" ;
            $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;
        }else if($datas['count_periods'] == 2 && $datas['cost_count_type'] != 2){  //按周
            $table = "{$this->table_goods_week_report} AS report" ;
            $where = $ym_where   . (empty($where) ? "" : " AND " . $where) ;
        }else if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
            $where = $ym_where . (empty($where) ? "" : " AND " . $where) ;
            $table = "{$this->table_goods_month_report} AS report" ;
        }else if($datas['cost_count_type'] == 2 ){
            $where = $ym_where .  (empty($where) ? "" : " AND " . $where) ;
            $table = "{$this->table_goods_month_report} AS report" ;
        }else{
            return [];
        }



        if ($datas['currency_code'] != 'ORIGIN') {
            if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id  ";
            }
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

        } else if($datas['count_dimension'] == 'tags'){
            $table.= " LEFT JOIN {$this->table_amazon_goods_tags_rel} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id and  tags_rel.status = 1 LEFT JOIN {$this->table_amazon_goods_tags} AS gtags ON gtags.id = tags_rel.tags_id AND gtags.status = 1" ;
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                if ($datas['count_periods'] == '1' ) { //按天
                    $group = 'tags_rel.tags_id  , report.myear , report.mmonth  , report.mday';
                    $orderby = 'tags_rel.tags_id , report.myear , report.mmonth  , report.mday';
                } else if ($datas['count_periods'] == '2' ) { //按周
                    $group = 'tags_rel.tags_id  , report.mweekyear , report.mweek';
                    $orderby = 'tags_rel.tags_id , report.mweekyear , report.mweek';
                } else if ($datas['count_periods'] == '3' ) { //按月
                    $group = 'tags_rel.tags_id  , report.myear , report.mmonth';
                    $orderby = 'tags_rel.tags_id , report.myear , report.mmonth';
                } else if ($datas['count_periods'] == '4' ) {  //按季
                    $group = 'tags_rel.tags_id  , report.myear , report.mquarter';
                    $orderby = 'tags_rel.tags_id  , report.myear , report.mquarter';
                } else if ($datas['count_periods'] == '5' ) { //按年
                    $group = 'tags_rel.tags_id  , report.myear';
                    $orderby = 'tags_rel.tags_id  , report.myear';
                }
            }else{
                $group = 'tags_rel.tags_id  ' ;
                $orderby = empty($orderby) ? ('tags_rel.tags_id ') : ($orderby . ' , tags_rel.tags_id');
            }
            $where.= " AND CAST(tags_rel.tags_id  as bigint) > 0";
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
        } else if($datas['count_dimension'] == 'all_goods'){ //按全部商品维度统计
            if($datas['is_distinct_channel'] == 1) { //有区分店铺
                if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
                    if ($datas['count_periods'] == '1' ) { //按天
                        $group = ' report.myear , report.mmonth  , report.mday';
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

        }else if($datas['count_dimension'] == 'goods_channel'){  //统计商品数据里的店铺维度
            if ($datas['count_periods'] > 0 && $datas['show_type'] == '2' ) {
                if ($datas['count_periods'] == '1' ) { //按天
                    $group = 'report.channel_id ,report.myear , report.mmonth  , report.mday';
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
                $group = 'report.channel_id ';
                $orderby = empty($orderby) ? ('report.channel_id ') : ($orderby . ' ,report.channel_id ');
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
            if(!empty($where_detail['is_care'])){
                $where.= " AND report.goods_is_care = " . (intval($where_detail['is_care']) == 1 ? 1 : 0 );
            }
            if(!empty($where_detail['is_new'])){
                $where.= " AND report.goods_is_new = " . (intval($where_detail['is_new']) == 1 ? 1 : 0 );
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

            if (!empty($where_detail['tag_id'])) {
                if (strpos($group, 'tags_rel.tags_id') === false) {
                    $table .= " LEFT JOIN {$this->table_amazon_goods_tags_rel} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id LEFT JOIN {$this->table_amazon_goods_tags} AS gtags ON gtags.id = tags_rel.tags_id";
                }
                if(is_array($where_detail['group_id'])){
                    $tag_str = implode(',', $where_detail['tag_id']);
                }else{
                    $tag_str = $where_detail['tag_id'] ;
                }
                if (!empty($tag_str)) {
                    $where .= " AND tags_rel.tags_id  IN ( " . $tag_str . " ) ";
                }
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

        $group = str_replace("{:RATE}", $exchangeCode, $group ?? '');
        $where = str_replace("{:RATE}", $exchangeCode, $where ?? '');
        $orderby = str_replace("{:RATE}", $exchangeCode, $orderby ?? '');
        $limit_num = 0 ;
        if($datas['show_type'] == 2 && $datas['limit_num'] > 0 ){
            $limit_num = $datas['limit_num'] ;
        }
        $count = 0;
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
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByGoods Total Request', [$this->getLastSql()]);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByGoods Request', [$this->getLastSql()]);
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

        $rt['lists'] = empty($lists) ? array() : $lists;
        $rt['count'] = intval($count);
        return $rt;
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
            case "tags":
                $field_data = "max(tags_rel.tags_id) as tags_id";
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
            case "department":
                $field_data = "max(dc.user_department_id) as user_department_id";
                break;
            case "admin_id":
                $field_data = "max(uc.admin_id) as admin_id";
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
                case "tags":
                    $where .=  " AND tags_rel.tags_id IN (".implode(",",array_column($lists,'tags_id')).")";
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
                case "department":
                    $where .=  " AND dc.user_department_id IN (".implode(",",array_column($lists,'user_department_id')).")";
                    break;
                case "admin_id":
                    $where .=  " AND uc.admin_id IN (".implode(",",array_column($lists,'admin_id')).")";
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
            $where = "g.db_num='{$this->dbhost}' AND g.user_id={$lists[0]['user_id']}";
            if (!empty($channel_arr)){
                if (count($channel_arr)==1){
                    $where .= " AND g.channel_id = ".intval(implode(",",$channel_arr));
                }else{
                    $where .= " AND g.channel_id IN (".implode(",",$channel_arr).")";
                }
            }
            $table = "{$this->table_amazon_goods_finance} as g " ;
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
            }else if($datas['count_dimension'] == 'tags'){ //标签（需要刷数据）
                $fba_fields = $group = 'rel.tags_id,g.fba_inventory_v3_id' ;
                $table .= "  LEFT JOIN {$this->table_amazon_goods_tags_rel} AS rel ON g.g_amazon_goods_id = rel.goods_id ";
            }else if($datas['count_dimension'] == 'head_id') { //负责人
                $fba_fields = $group = 'i.head_id ,g.fba_inventory_v3_id' ;
                $table .= "  LEFT JOIN {$this->table_amazon_goods_isku} AS i ON i.db_num='{$this->dbhost}' AND g.isku_id = i.id  ";
            }else if($datas['count_dimension'] == 'developer_id') { //开发人员
                $fba_fields = $group = 'i.developer_id ,g.fba_inventory_v3_id' ;
                $table .= "  LEFT JOIN {$this->table_amazon_goods_isku} AS i ON i.db_num='{$this->dbhost}' AND g.isku_id = i.id  ";
            }else if($datas['count_dimension'] == 'all_goods'){
                if($datas['is_distinct_channel'] == 1) { //有区分店铺
                    $fba_fields = $group = 'g.channel_id' ;
                }else{
                    $fba_fields = $group = 'g.fba_inventory_v3_id' ;
                }
            }else if($datas['count_dimension'] == 'goods_channel'){
                $fba_fields = $group = 'g.channel_id' ;
            }


            $where_arr = array() ;
            foreach($lists as $list1){
                if($datas['count_dimension'] == 'sku'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('sku' => self::escape($list1['sku']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('sku' => self::escape($list1['sku']));
                    }
                }else if($datas['count_dimension'] == 'asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('asin' => self::escape($list1['asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('asin' => self::escape($list1['asin']));
                    }
                }else if($datas['count_dimension'] == 'parent_asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('parent_asin' => self::escape($list1['parent_asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('parent_asin' => self::escape($list1['parent_asin']));
                    }
                }else if($datas['count_dimension'] == 'class1'){
                    $where_arr[] = array('goods_product_category_name_1'=>$list1['class1'] ) ;
                }else if($datas['count_dimension'] == 'group'){
                    $where_arr[] = array('group_id'=>$list1['group_id']) ;
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
            }else if($datas['count_dimension'] == 'tags'){ //标签
                $where_strs = array_unique(array_column($where_arr , 'tags_id')) ;
                $where_str = 'rel.tags_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'head_id'){ //标签
                $where_strs = array_unique(array_column($where_arr , 'head_id')) ;
                $where_str = 'i.head_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'developer_id'){
                $where_strs = array_unique(array_column($where_arr , 'developer_id')) ;
                $where_str = 'i.developer_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'isku'){
                $where_strs = array_unique(array_column($where_arr , 'isku_id')) ;
                $where_str = 'g.isku_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else{
                $where_str = '1=1' ;
            }
        }
        $where.= ' AND ' . $where_str." AND g.fba_inventory_v3_id > 0  AND g.Transport_mode = 2" ;
        if(isset($datas['where_detail']) && $datas['where_detail']){
            if (!is_array($datas['where_detail'])){
                $datas['where_detail'] = json_decode($datas['where_detail'],true);
            }
            if (!empty($datas['where_detail']['group_id']) && !empty(trim($datas['where_detail']['group_id']))){
                $where .= ' AND g.group_id IN (' . $datas['where_detail']['group_id'] . ') ' ;
            }
            if (!empty($datas['where_detail']['transport_mode']) && !empty(trim($datas['where_detail']['transport_mode']))){
                $where .= ' AND g.Transport_mode = ' . ($datas['where_detail']['transport_mode'] == 'FBM' ? 1 : 2);
            }
            if (!empty($datas['where_detail']['is_care']) && !empty(trim($datas['where_detail']['is_care']))){
                $where .= ' AND g.is_care = ' . (intval($datas['where_detail']['is_care'])==1?1:0);
            }
            if (!empty($datas['where_detail']['tag_id']) && !empty(trim($datas['where_detail']['tag_id']))){
                if ($datas['count_dimension'] != 'tags'){
                    $table .= "  LEFT JOIN {$this->table_amazon_goods_tags_rel} AS rel ON g.g_amazon_goods_id = rel.goods_id ";
                }
                $where .=' AND rel.tags_id IN (' .  trim($datas['where_detail']['tag_id']) . ' ) ';
            }
            if (!empty($datas['where_detail']['operators_id']) && !empty(trim($datas['where_detail']['operators_id']))){

                $table .= "  LEFT JOIN {$this->table_channel} AS c ON g.channel_id = c.id  ";

                $where .=' AND (g.operation_user_admin_id IN (' .  trim($datas['where_detail']['operators_id']) . ' ) OR c.operation_user_admin_id IN (' .  trim($datas['where_detail']['operators_id']) . ' ) )';
            }

        }

        $fba_fields .= ' , SUM(DISTINCT(CASE WHEN g.fulfillable_quantity < 0 THEN 0 ELSE g.fulfillable_quantity END )) as fba_sales_stock ,MAX(DISTINCT( CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as  fba_sales_day , MAX(DISTINCT(g.available_days) ) as max_fba_sales_day , MIN( DISTINCT(g.available_days) ) as min_fba_sales_day , MIN(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END ))  as min_egt0_fba_sales_day , MAX(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as max_egt0_fba_sales_day , SUM(DISTINCT(CASE WHEN g.reserved_quantity < 0 THEN 0 ELSE g.reserved_quantity END )) as fba_reserve_stock  , SUM(DISTINCT( CASE WHEN g.replenishment_quantity < 0 THEN 0 ELSE g.replenishment_quantity END ))  as fba_recommended_replenishment , MAX( DISTINCT(g.replenishment_quantity) ) as max_fba_recommended_replenishment ,MIN( DISTINCT(g.replenishment_quantity) ) as min_fba_recommended_replenishment , SUM(DISTINCT( CASE WHEN g.available_stock < 0 THEN 0 ELSE g.available_stock END )) as fba_special_purpose , MAX( DISTINCT(g.available_stock)) as  max_fba_special_purpose , MIN(DISTINCT( g.available_stock) )  as min_fba_special_purpose ';

        $goods_finance_md = new AmazonGoodsFinancePrestoModel($this->dbhost, $this->codeno);
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
                    $fba_data = empty($fbaDatas[$list2['sku'] . '-' . $list2['channel_id']]) ? array() : $fbaDatas[$list2['sku'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = empty($fbaDatas[$list2['sku']]) ? array() : $fbaDatas[$list2['sku']];
                }
            }else if($datas['count_dimension'] == 'asin'){
                if($datas['is_distinct_channel'] == 1) {
                    $fba_data = empty($fbaDatas[$list2['asin'] . '-' . $list2['channel_id']]) ? array() : $fbaDatas[$list2['asin'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = empty($fbaDatas[$list2['asin']]) ? array() : $fbaDatas[$list2['asin']];
                }
            }else if($datas['count_dimension'] == 'parent_asin'){
                if($datas['is_distinct_channel'] == 1) {
                    $fba_data = empty($fbaDatas[$list2['parent_asin'] . '-' . $list2['channel_id']]) ? array() : $fbaDatas[$list2['parent_asin'] . '-' . $list2['channel_id']];
                }else{
                    $fba_data = empty($fbaDatas[$list2['parent_asin']]) ? array() : $fbaDatas[$list2['parent_asin']];
                }
            }else if($datas['count_dimension'] == 'class1'){
                $fba_data = empty($fbaDatas[$list2['class1']]) ? array() :  $fbaDatas[$list2['class1']];
            }else if($datas['count_dimension'] == 'group'){
                $fba_data = empty($fbaDatas[$list2['group_id']]) ? array() :  $fbaDatas[$list2['group_id']];
            }else if($datas['count_dimension'] == 'tags'){  //标签（需要刷数据）
                $fba_data = empty($fbaDatas[$list2['tags_id']]) ? array() :  $fbaDatas[$list2['tags_id']];
            }else if($datas['count_dimension'] == 'head_id'){
                $fba_data = empty($fbaDatas[$list2['head_id']]) ? array() : $fbaDatas[$list2['head_id']] ;
            }else if($datas['count_dimension'] == 'developer_id'){
                $fba_data = empty($fbaDatas[$list2['developer_id']]) ? array() : $fbaDatas[$list2['developer_id']] ;
            }else if($datas['count_dimension'] == 'isku'){
                $fba_data = empty($fbaDatas[$list2['isku_id']]) ? array() : $fbaDatas[$list2['isku_id']] ;
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
            if(empty($fbaDatas[$fba[$field]]['fba_sales_stock'])){
                $fbaDatas[$fba[$field]]['fba_sales_stock'] = $fba['fba_sales_stock'] ;
            }else{
                $fbaDatas[$fba[$field]]['fba_sales_stock']+= $fba['fba_sales_stock'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['fba_sales_day'])){
                $fbaDatas[$fba[$field]]['fba_sales_day'] = $fba['fba_sales_day'] ;
            }else{
                $fbaDatas[$fba[$field]]['fba_sales_day'] = ($fbaDatas[$fba[$field]]['fba_sales_day'] > $fba['fba_sales_day']) ? $fbaDatas[$fba[$field]]['fba_sales_day'] : $fba['fba_sales_day'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['max_fba_sales_day'])){
                $fbaDatas[$fba[$field]]['max_fba_sales_day'] = $fba['max_fba_sales_day'] ;
            }else{
                $fbaDatas[$fba[$field]]['max_fba_sales_day'] = ($fbaDatas[$fba[$field]]['max_fba_sales_day'] > $fba['max_fba_sales_day']) ? $fbaDatas[$fba[$field]]['max_fba_sales_day'] : $fba['max_fba_sales_day'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['min_fba_sales_day'])){
                $fbaDatas[$fba[$field]]['min_fba_sales_day'] = $fba['min_fba_sales_day'] ;
            }else{
                $fbaDatas[$fba[$field]]['min_fba_sales_day'] = ($fbaDatas[$fba[$field]]['min_fba_sales_day'] < $fba['min_fba_sales_day']) ? $fbaDatas[$fba[$field]]['min_fba_sales_day'] : $fba['min_fba_sales_day'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'])){
                $fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] = $fba['max_egt0_fba_sales_day'] ;
            }else{
                $fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] = ($fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] > $fba['max_egt0_fba_sales_day']) ? $fbaDatas[$fba[$field]]['max_egt0_fba_sales_day'] : $fba['max_egt0_fba_sales_day'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'])){
                $fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] = $fba['min_egt0_fba_sales_day'] ;
            }else{
                $fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] = ($fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] < $fba['min_egt0_fba_sales_day']) ? $fbaDatas[$fba[$field]]['min_egt0_fba_sales_day'] : $fba['min_egt0_fba_sales_day'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['fba_reserve_stock'])){
                $fbaDatas[$fba[$field]]['fba_reserve_stock'] = $fba['fba_reserve_stock'] ;
            }else{
                $fbaDatas[$fba[$field]]['fba_reserve_stock'] += $fba['fba_reserve_stock'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['fba_recommended_replenishment'])){
                $fbaDatas[$fba[$field]]['fba_recommended_replenishment'] = $fba['fba_recommended_replenishment'] ;
            }else{
                $fbaDatas[$fba[$field]]['fba_recommended_replenishment'] += $fba['fba_recommended_replenishment'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'])){
                $fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'] = $fba['max_fba_recommended_replenishment'] ;
            }else{
                $fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'] = ($fbaDatas[$fba[$field]]['max_fba_recommended_replenishment'] < $fba['max_fba_recommended_replenishment']) ? $fba['max_fba_recommended_replenishment']:$fbaDatas[$fba[$field]]['max_fba_recommended_replenishment']   ;
            }

            if(empty($fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'])){
                $fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] = $fba['min_fba_recommended_replenishment'] ;
            }else{
                $fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] = ($fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] < $fba['min_fba_recommended_replenishment']) ? $fbaDatas[$fba[$field]]['min_fba_recommended_replenishment'] : $fba['min_fba_recommended_replenishment'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['fba_special_purpose'])){
                $fbaDatas[$fba[$field]]['fba_special_purpose'] = $fba['fba_special_purpose'] ;
            }else{
                $fbaDatas[$fba[$field]]['fba_special_purpose'] += $fba['fba_special_purpose'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['max_fba_special_purpose'])){
                $fbaDatas[$fba[$field]]['max_fba_special_purpose'] = $fba['max_fba_special_purpose'] ;
            }else{
                $fbaDatas[$fba[$field]]['max_fba_special_purpose'] = ($fbaDatas[$fba[$field]]['max_fba_special_purpose'] > $fba['max_fba_special_purpose']) ? $fbaDatas[$fba[$field]]['max_fba_special_purpose'] : $fba['max_fba_special_purpose'] ;
            }

            if(empty($fbaDatas[$fba[$field]]['min_fba_special_purpose'])){
                $fbaDatas[$fba[$field]]['min_fba_special_purpose'] = $fba['min_fba_special_purpose'] ;
            }else{
                $fbaDatas[$fba[$field]]['min_fba_special_purpose'] = ($fbaDatas[$fba[$field]]['min_fba_special_purpose'] < $fba['min_fba_special_purpose']) ? $fbaDatas[$fba[$field]]['min_fba_special_purpose'] : $fba['min_fba_special_purpose'] ;
            }
        }

        return $fbaDatas;
    }

    //获取商品维度指标字段(新增统计维度完成)
    private function getGoodsFields($datas = array())
    {
        $fields = array();
        $fields = $this->getGoodsTheSameFields($datas,$fields);

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
        if (in_array('goods_rank', $targets)) { //大类目rank
            $fields['goods_rank'] = "min(nullif(report.goods_rank,0))";
        }
        if (in_array('goods_min_rank', $targets)) { //小类目rank
            $fields['goods_min_rank'] = " min(nullif(report.goods_min_rank,0))";
        }
        if (in_array('goods_views_number', $targets)) { //页面浏览次数
            $fields['goods_views_number'] = " SUM ( report.byorder_number_of_visits ) ";
        }

        if(in_array('goods_views_rate', $targets) || in_array('goods_buyer_visit_rate', $targets)){
            $table = "{$this->table_goods_day_report} AS report ";
            if($datas['min_ym'] == $datas['max_ym']){
                $where  = "report.ym = '" . $datas['min_ym'] . "' AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
            }else{
                $where  = "report.ym >= '" . $datas['min_ym'] . "' AND report.ym <= '" .$datas['max_ym'] . "' AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
            }

            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                 $totals_view_session_lists = $this->select($where." AND byorder_number_of_visits>0", 'report.channel_id,SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table,'','',"report.channel_id");
            }else{
                $total_views_session_numbers = $this->get_one($where, 'SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table);
            }
        }
        if (in_array('goods_views_rate', $targets)) { //页面浏览次数百分比 (需要计算)
            //总流量次数
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                if (!empty($totals_view_session_lists)){
                    $case = " CASE ";
                    foreach ($totals_view_session_lists as $total_views_numbers_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_views_numbers_list['channel_id']." THEN SUM ( report.byorder_number_of_visits ) / round( " . $total_views_numbers_list['total_views_number'].",2) ";
                    }
                    $case .= "ELSE 0 END";
                    $fields['goods_views_rate'] = $case ;
                }else{
                    $fields['goods_views_rate'] = 0 ;
                }
            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_views_number']) > 0) {
                    $fields['goods_views_rate'] = " SUM ( report.byorder_number_of_visits ) / round(" . intval($total_views_session_numbers['total_views_number']) .' , 2)';
                }else{
                    $fields['goods_views_rate'] = 0 ;
                }
            }
        }
        if (in_array('goods_buyer_visit_rate', $targets)) { //买家访问次数百分比 （需要计算）
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                if (!empty($totals_view_session_lists)){
                    $case = " CASE ";
                    foreach ($totals_view_session_lists as $total_user_sessions_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_user_sessions_list['channel_id']." THEN SUM ( report.byorder_user_sessions ) / round(" . $total_user_sessions_list['total_user_sessions'].",2)";
                    }
                    $case .= " ELSE 0 END";
                    $fields['goods_buyer_visit_rate'] =  $case  ;
                }else{
                    $fields['goods_buyer_visit_rate'] = 0 ;
                }
            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_user_sessions']) > 0) {
                    $fields['goods_buyer_visit_rate'] = " SUM ( report.byorder_user_sessions ) / round(" . intval($total_views_session_numbers['total_user_sessions']).',2)';
                }else{
                    $fields['goods_buyer_visit_rate'] =0 ;
                }
            }
        }
        if (in_array('goods_buybox_rate', $targets)) { //购买按钮赢得率
            $fields['goods_buybox_rate'] = " (SUM ( byorder_buy_button_winning_num ) * 1.0 /  nullif(SUM ( report.byorder_number_of_visits ) ,0) ) ";
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
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
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
                    $fields['sale_refund'] = "SUM ( (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "SUM ( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = $fields['sale_return_goods_number'] . " * 1.0 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('promote_discount', $targets)) {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM (report.first_purchasing_cost ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    }
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }
                }
            }
        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets) ||  in_array('cost_profit_total_pay', $targets) ) {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)))' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)))' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                }
            }

        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = $fields['cost_profit_profit'] . " /  nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets) || in_array('cost_profit_total_income',$targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM (report.byorder_goods_amazon_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM (report.report_goods_amazon_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM (report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }

        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission + report.byorder_reserved_field21) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( (report.byorder_platform_sales_commission + report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission + report.report_reserved_field21 ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( (report.report_platform_sales_commission + report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM(report.byorder_goods_amazon_other_fee)";
                } else {
                    $fields['amazon_other_fee'] = "SUM(report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM(report.report_goods_amazon_other_fee)";
                } else {
                    $fields['amazon_other_fee'] = "SUM(report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.byorder_estimated_monthly_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.report_estimated_monthly_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }
        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = '(' . $fields['amazon_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = '(' . $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "SUM ( 0- report.byorder_reserved_field16 ) ";
            } else {
                $fields['operate_fee'] = "SUM ( (0 -  report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = '(' . $fields['operate_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = '(' . $fields['evaluation_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('cpc_sp_cost', $targets)) {  //CPC_SP花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets)) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
            } else {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = '(' . $fields['cpc_cost'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
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
            $fields['cpc_order_number'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['cpc_order_rate'] = '(' . $fields['cpc_order_number'] . ") / nullif( SUM(report.byorder_sales_volume+report.byorder_group_id ) , 0 )  ";
            }else{
                $fields['cpc_order_rate'] = '(' . $fields['cpc_order_number'] . ") / nullif( SUM(report.report_sales_volume +report.report_group_id  ) , 0 ) ";
            }

        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = '('.$fields['cpc_order_number'] . ") / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  )';
            } else {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = '(' . $fields['cpc_turnover'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = '('.$fields['cpc_cost'] . ") / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = '('.$fields['cpc_cost'] . ") / nullif( " . $fields['cpc_turnover'] . " , 0 ) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = '(' . $fields['cpc_direct_sales_volume'] . ") / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU"  )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = '(' . $fields['cpc_indirect_sales_volume'] . ") / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('other_vat_fee', $targets)) { //VAT
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.byorder_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
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

        if (in_array('cost_profit_total_income', $targets) || in_array('cost_profit_total_pay', $targets)) {   //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = $fields['cost_profit_total_income'] . " + SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] . " + SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] ." + SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] ." + SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }

        }

        if (in_array('cost_profit_total_pay', $targets) ) {   //总支出
            $fields['cost_profit_total_income'] = $fields['cost_profit_profit'] . '-' . $fields['cost_profit_total_income']  ;
        }


        $this->getUnTimeFields($fields,$datas,$targets);

        return $fields;
    }

    private function getGoodsTheSameFields($datas,$fields){
        $fields['user_id'] = 'max(report.user_id)';
        $fields['goods_id'] = 'max(report.amazon_goods_id)';
        $fields['site_country_id'] = 'max(report.site_id)';

        if (in_array($datas['count_dimension'],['parent_asin','asin','sku','isku'])){
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['goods_price_min'] = 'min(report.goods_price)';
                $fields['goods_price_max'] = 'max(report.goods_price)';
            } else {
                $fields['goods_price_min'] = 'min(report.goods_price* ({:RATE} / COALESCE(rates.rate ,1)))';
                $fields['goods_price_max'] = 'max(report.goods_price* ({:RATE} / COALESCE(rates.rate ,1)))';

            }

            $fields['min_transport_mode'] = ' min(report.goods_transport_mode) ' ;
            $fields['max_transport_mode'] = ' max(report.goods_transport_mode) ' ;
        }

        if ($datas['count_dimension'] == 'parent_asin') {
            $fields['parent_asin'] = "max(report.goods_parent_asin)";
            $fields['image'] = 'max(report.goods_image)';
            $fields['title'] = 'max(report.goods_title)';
            if($datas['is_distinct_channel'] == '1'){
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
            }
            $fields['goods_is_care']                 = 'max(report.goods_is_care)';
            $fields['is_keyword']                 = 'max(report.goods_is_keyword)';
            $fields['goods_is_new']                  = 'max(report.goods_is_new)';
            $fields['up_status']                  = 'max(report.goods_up_status)';
            $fields['is_remarks']       = 'max(report.goods_is_remarks)';
            $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
        }else if ($datas['count_dimension'] == 'asin') {
            $fields['asin'] = "max(report.goods_asin)";
            $fields['image'] = 'max(report.goods_image)';
            $fields['title'] = 'max(report.goods_title)';
            if($datas['is_distinct_channel'] == '1'){
                $fields['parent_asin'] = "max(report.goods_parent_asin)";
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';

            }
            $fields['goods_is_care']                 = 'max(report.goods_is_care)';
            $fields['is_keyword']                 = 'max(report.goods_is_keyword)';
            $fields['goods_is_new']                  = 'max(report.goods_is_new)';
            $fields['up_status']                  = 'max(report.goods_up_status)';
            $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
            $fields['is_remarks']       = 'max(report.goods_is_remarks)';
        }else if ($datas['count_dimension'] == 'sku') {
            $fields['sku'] = "max(report.goods_sku)";
            $fields['image'] = 'max(report.goods_image)';
            $fields['title'] = 'max(report.goods_title)';
            if($datas['is_distinct_channel'] == '1'){

                $fields['asin'] = "max(report.goods_asin)";
                $fields['parent_asin'] = "max(report.goods_parent_asin)";

                $fields['goods_product_category_name_1'] = 'max(report.goods_product_category_name_1)';
                $fields['goods_product_category_name_2'] = 'max(report.goods_product_category_name_2)';
                $fields['goods_product_category_name_3'] = 'max(report.goods_product_category_name_3)';
                $fields['goods_is_care']                 = 'max(report.goods_is_care)';
                $fields['is_keyword']                 = 'max(report.goods_is_keyword)';
                $fields['goods_is_new']                  = 'max(report.goods_is_new)';
                $fields['up_status']                  = 'max(report.goods_up_status)';
                $fields['isku_id']                       = 'max(report.goods_isku_id)';
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
                $fields['class1'] = 'max(report.goods_product_category_name_1)';
                $fields['group'] = 'max(report.goods_group_name)';
                $fields['operators'] = 'max(report.goods_operation_user_admin_name)';
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
        }else if ($datas['count_dimension'] == 'tags') {
            $fields['tags_id'] = 'max(tags_rel.tags_id)';
            $fields['tags'] = 'max(gtags.tag_name)';
        } else if ($datas['count_dimension'] == 'head_id') {
            $fields['head_id'] = 'max(report.isku_head_id)';
        } else if ($datas['count_dimension'] == 'developer_id') {
            $fields['developer_id'] = 'max(report.isku_developer_id)';
        } elseif($datas['count_dimension'] == 'all_goods') {
            if($datas['is_distinct_channel'] == '1'){
                $fields['channel_id'] = 'max(report.channel_id)';
            }
        } else if($datas['count_dimension'] == 'goods_channel'){
            $fields['channel_id'] = 'max(report.channel_id)';
        }

        return $fields;
    }

    //按商品维度,时间展示字段（新增统计维度完成）
    private function getGoodsTimeFields($datas = [], $time_line)
    {
        $fields = [];
        $fields = $this->getGoodsTheSameFields($datas,$fields);

        $time_fields = [];

        if($datas['time_target'] == 'goods_views_rate' || $datas['time_target'] == 'goods_buyer_visit_rate'){
            $table = "{$this->table_goods_day_report} AS report ";
            if($datas['min_ym'] == $datas['max_ym']){
                $where  = "report.ym = '" . $datas['min_ym'] . "' AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
            }else{
                $where  = "report.ym >= '" . $datas['min_ym'] . "' AND report.ym <= '" .$datas['max_ym'] . "' AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
            }
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                $totals_view_session_lists = $this->select($where." AND byorder_number_of_visits>0", 'report.channel_id,SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table,'','',"report.channel_id");
            }else{
                $total_views_session_numbers = $this->get_one($where, 'SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table);
            }
        }

        if ($datas['time_target'] == 'goods_visitors') {  // 买家访问次数
            $fields['count_total'] = "SUM(report.byorder_user_sessions)";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_user_sessions');
        } else if ($datas['time_target'] == 'goods_conversion_rate') { //订单商品数量转化率
            $fields['count_total'] = 'SUM ( report.byorder_quantity_of_goods_ordered ) / nullif(SUM ( report.byorder_user_sessions ) ,0)';
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
        } else if ($datas['time_target'] == 'goods_rank') { //大类目rank
            $fields['count_total'] = "min(nullif(report.goods_rank,0))";
            $time_fields = $this->getTimeFields($time_line, 'nullif(report.goods_rank,0)', '', 'MIN');
        } else if ($datas['time_target'] == 'goods_min_rank') { //小类目rank
            $fields['count_total'] = "min(nullif(report.goods_min_rank,0))";
            $time_fields = $this->getTimeFields($time_line, 'nullif(report.goods_min_rank,0)', '', 'MIN');
        } else if ($datas['time_target'] == 'goods_views_number') { //页面浏览次数
            $fields['count_total'] = "SUM(report.byorder_number_of_visits)";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_number_of_visits');
        } else if ($datas['time_target'] == 'goods_views_rate') { //页面浏览次数百分比 (需要计算)

            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                if (!empty($totals_view_session_lists)){
                    $case = "CASE ";
                    foreach ($totals_view_session_lists as $total_views_numbers_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_views_numbers_list['channel_id']." THEN SUM ( report.byorder_number_of_visits ) / round(" . $total_views_numbers_list['total_views_number'].",2) ";
                    }
                    $case .= " ELSE 0 END";
                    $fields['goods_views_rate'] = $case ;
                    $time_fields = $this->getTimeFields($time_line, $case);
                }else{
                    $fields['count_total'] = 0;
                    $time_fields = $this->getTimeFields($time_line, 0);
                }

            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_views_number']) > 0) {
                    $fields['count_total'] = "SUM ( report.byorder_number_of_visits ) / round(" . intval($total_views_session_numbers['total_views_number']) .',2)';
                    $time_fields = $this->getTimeFields($time_line, "  report.byorder_number_of_visits  / round(" . intval($total_views_session_numbers['total_views_number']).',2)');
                } else {
                    $fields['count_total'] = 0;
                    $time_fields = $this->getTimeFields($time_line, 0);
                }
            }
        } else if ($datas['time_target'] == 'goods_buyer_visit_rate') { //买家访问次数百分比 （需要计算）
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                if (!empty($totals_view_session_lists)){
                    $case = "CASE ";
                    foreach ($totals_view_session_lists as $total_user_sessions_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_user_sessions_list['channel_id']." THEN SUM ( report.byorder_user_sessions ) / round(" . $total_user_sessions_list['total_user_sessions'].",2)";
                    }
                    $case .= " ELSE 0 END";

                    $fields['count_total'] = $case;
                    $time_fields = $this->getTimeFields($time_line, $case);
                }else{
                    $fields['count_total'] = 0;
                    $time_fields = $this->getTimeFields($time_line, 0);
                }
            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_user_sessions']) > 0) {
                    $fields['count_total'] = " SUM ( report.byorder_user_sessions ) / round(" . intval($total_views_session_numbers['total_user_sessions']) .",2)";
                    $time_fields = $this->getTimeFields($time_line, " report.byorder_user_sessions  / round(" . intval($total_views_session_numbers['total_user_sessions']).",2)");
                } else {
                    $fields['count_total'] = 0;
                    $time_fields = $this->getTimeFields($time_line, 0);
                }
            }
        } else if ($datas['time_target'] == 'goods_buybox_rate') { //购买按钮赢得率
            $fields['count_total'] = " (SUM ( byorder_buy_button_winning_num )  * 1.0 /  nullif(SUM ( report.byorder_number_of_visits ) ,0) ) ";
            $time_fields = $this->getTimeFields($time_line, "byorder_buy_button_winning_num * 1.0", "report.byorder_number_of_visits");
        } else if ($datas['time_target'] == 'sale_sales_volume') { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = " SUM ( report.byorder_sales_volume  +  report.byorder_group_id) ";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_volume  +  report.byorder_group_id");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = " SUM ( report.report_sales_volume  +  report.byorder_group_id ) ";
                $time_fields = $this->getTimeFields($time_line, "report.report_sales_volume  +  report.byorder_group_id");
            }
        } else if ($datas['time_target'] == 'sale_many_channel_sales_volume') { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM ( report.byorder_group_id )";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_group_id");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = "SUM ( report.report_group_id )";
                $time_fields = $this->getTimeFields($time_line, "report.report_group_id");
            }
        } else if ($datas['time_target'] == 'sale_sales_quota') {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'sale_return_goods_number') {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['count_total'] = "SUM (report.byorder_refund_num )";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num");
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['count_total'] = "SUM (report.report_refund_num )";
                $time_fields = $this->getTimeFields($time_line, "report.report_refund_num");
            }
        } else if ($datas['time_target'] == 'sale_refund') {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( 0 - report.byorder_refund )";
                    $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund )");
                } else {
                    $fields['count_total'] = "SUM ( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_refund )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund ");
                } else {
                    $fields['count_total'] = "SUM ( report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            }
        } else if ($datas['time_target'] == 'sale_refund_rate') {  //退款率
            if ($datas['refund_datas_origin'] == '1') {
                $fields['count_total'] = "SUM (report.byorder_refund_num  ) * 1.0 / nullif(SUM(report.byorder_sales_volume),0)";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0 ", "report.byorder_sales_volume");
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['count_total'] = "SUM (report.report_refund_num ) * 1.0 / nullif(SUM(report.report_sales_volume),0)";
                $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0", "report.report_sales_volume");
            }
        } else if ($datas['time_target'] == 'promote_discount') {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'promote_refund_discount') {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'cost_profit_profit') {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = 'SUM(  report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit )';
                        $time_fields = $this->getTimeFields($time_line, ' report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = 'SUM(  report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                        $time_fields = $this->getTimeFields($time_line, ' report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }

                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course)';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = 'SUM( report.first_purchasing_cost + report.first_logistics_head_course +  report.report_goods_profit)';
                        $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = 'SUM(  ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }

                }
            }
        } else if ($datas['time_target'] == 'cost_profit_profit_rate') {  //毛利率
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)) / nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit))/ nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit', 'report.byorder_sales_quota');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course)) / nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( ( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit ))/ nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit', 'report.byorder_sales_quota');
                    }
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit ))/ nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit', 'report.report_sales_quota');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course)) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( ( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit )) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit', 'report.report_sales_quota');
                    }
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee') {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_goods_amazon_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_goods_amazon_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_sales_commission') {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission + report.byorder_reserved_field21 ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission + report.byorder_reserved_field21)');
                } else {
                    $fields['count_total'] = "SUM ( (report.byorder_platform_sales_commission+report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission+report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission +report.report_reserved_field21  ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission +report.report_reserved_field21)');
                } else {
                    $fields['count_total'] = "SUM ( (report.report_platform_sales_commission +report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission+report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_delivery_fee') {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_settlement_fee') {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_other_fee') {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_goods_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_goods_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_shipping_fee') {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_refund_deducted_commission') {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_estimated_monthly_storage_fee )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_estimated_monthly_storage_fee )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_estimated_monthly_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_long_term_storage_fee') { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee_rate') {  //亚马逊费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM ( report.byorder_goods_amazon_fee ) / nullif(SUM ( report.byorder_sales_quota ),0)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee ', 'report.byorder_sales_quota');
                } else {
                    $fields['count_total'] = 'SUM ( report.report_goods_amazon_fee ) / nullif(SUM ( report.byorder_sales_quota ),0)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee ', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM ( report.byorder_goods_amazon_fee ) / nullif(SUM ( report.report_sales_quota ),0)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee ', 'report.report_sales_quota');
                } else {
                    $fields['count_total'] = 'SUM ( report.report_goods_amazon_fee ) / nullif(SUM ( report.report_sales_quota ),0)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee ', 'report.report_sales_quota');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_purchase_cost') {  //采购成本
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (  (report.first_purchasing_cost) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                }
            }

        } else if ($datas['time_target'] == 'purchase_logistics_logistics_cost') {  // 物流/头程
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (( report.first_logistics_head_course) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_logistics_head_course)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.byorder_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost)) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.report_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost)) / nullif(SUM ( report.report_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)', 'report.report_sales_quota');
                }
            }
        } else if ($datas['time_target'] == 'operate_fee') {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM (0 -  report.byorder_reserved_field16 ) ";
                $time_fields = $this->getTimeFields($time_line, '0 - report.byorder_reserved_field16');
            } else {
                $fields['count_total'] = "SUM ((0 -  report.byorder_reserved_field16 )* ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($time_line, '  (0 - report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'operate_fee_rate') {  //运营费用占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM( 0 - report.byorder_reserved_field16 ) /nullif(SUM(report.byorder_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field16', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = "SUM( 0 - report.byorder_reserved_field16 ) /nullif(SUM(report.report_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, '(0 - report.byorder_reserved_field16)', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'evaluation_fee') {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }

        } else if ($datas['time_target'] == 'evaluation_fee_rate') {  //测评费用占比
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10', 'report.byorder_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10', 'report.byorder_sales_quota');
                }

            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10', 'report.report_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10', 'report.report_sales_quota');
                }

            }
        } else if ($datas['time_target'] == 'cpc_sp_cost') {  //CPC SP 花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost  ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))  ) ";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))  ');
            }
        }  else if ($datas['time_target'] == 'cpc_sd_cost') {  //CPC SD 花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_sd_cost  ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_cost');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))  ) ";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))  ');
            }
        }  else if ($datas['time_target'] == 'cpc_cost') {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost ');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_cost_rate') {  //CPC花费占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) /nullif(SUM(report.byorder_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = "SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) /nullif(SUM(report.report_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_exposure') {  //CPC曝光量
            $fields['count_total'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 )";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field1 + report.byorder_reserved_field2');
        } else if ($datas['time_target'] == 'cpc_click_number') {  //CPC点击次数
            $fields['count_total'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks )";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
        } else if ($datas['time_target'] == 'cpc_click_rate') {  //CPC点击率
            $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks)) / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2), 0 ) ";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks', 'report.byorder_reserved_field1 + report.byorder_reserved_field2');
        } else if ($datas['time_target'] == 'cpc_order_number') {  //CPC订单数
            $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) ';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"');
        } else if ($datas['time_target'] == 'cpc_order_rate') {  //cpc订单占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) / nullif(SUM ( report.byorder_sales_volume +report.byorder_group_id  ) ,0) ';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', '(report.byorder_sales_volume + report.byorder_group_id)');
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) / nullif(SUM ( report.report_sales_volume + report.report_group_id ) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', '(report.report_sales_volume + report.report_group_id)');
            }
        } else if ($datas['time_target'] == 'cpc_click_conversion_rate') {  //cpc点击转化率
            $fields['count_total'] = '(SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" )) /nullif (SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks) , 0 )';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
        } else if ($datas['time_target'] == 'cpc_turnover') {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_turnover_rate') {  //CPC成交额占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")/nullif( SUM(report.byorder_sales_quota),0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")/nullif( SUM(report.report_sales_quota),0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_avg_click_cost') {  //CPC平均点击花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ),0) ';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
            } else {
                $fields['count_total'] = 'SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ),0) ';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
            }

        } else if ($datas['time_target'] == 'cpc_acos') {  // ACOS
            $fields['count_total'] = 'SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) / nullif( SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  ) , 0 ) ';
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" ');
        } else if ($datas['time_target'] == 'cpc_direct_sales_volume') {  //CPC直接销量
            $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )';
            $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"');
        } else if ($datas['time_target'] == 'cpc_direct_sales_quota') {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" )';
                $time_fields = $this->getTimeFields($time_line, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU"');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" ) /nullif(SUM ( report.byorder_sales_volume ) ,0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"', 'report.byorder_sales_volume');
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" ) /nullif(SUM ( report.report_sales_volume ) ,0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"', 'report.report_sales_volume');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume') {  //CPC间接销量
            $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) ';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU"');
        } else if ($datas['time_target'] == 'cpc_indirect_sales_quota') {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU"  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" ');
            } else {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) /nullif(SUM ( report.byorder_sales_volume ) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ', 'report.byorder_sales_volume');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) /nullif(SUM ( report.report_sales_volume ) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ', 'report.report_sales_volume');
            }
        }
        else if ($datas['time_target'] =='other_vat_fee') { //VAT
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                    $time_fields = $this->getTimeFields($time_line, '0-report.byorder_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                    $time_fields = $this->getTimeFields($time_line, '0-report.report_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        }else if($datas['time_target'] =='cost_profit_total_income') { //总收入
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.byorder_sales_quota + report.byorder_refund_promote_discount)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_sales_quota + report.byorder_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.byorder_sales_quota + report.byorder_refund_promote_discount ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' (report.byorder_sales_quota + report.byorder_refund_promote_discount ) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.report_sales_quota + report.byorder_refund_promote_discount )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_sales_quota + report.byorder_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.report_sales_quota + report.byorder_refund_promote_discount) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, '(report.report_sales_quota + report.byorder_refund_promote_discount) * ({:RATE} / COALESCE(rates.rate ,1))  ');
                    }
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.byorder_sales_quota + report.report_refund_promote_discount)";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_sales_quota + report.report_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.byorder_sales_quota + report.report_refund_promote_discount ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, '  (report.byorder_sales_quota + report.report_refund_promote_discount ) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.report_sales_quota + report.report_refund_promote_discount )";
                        $time_fields = $this->getTimeFields($time_line, '  report.report_sales_quota + report.report_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.report_sales_quota + report.report_refund_promote_discount) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, '   (report.report_sales_quota + report.report_refund_promote_discount) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            }
        }else if($datas['time_target'] =='cost_profit_total_pay') { //总支出
            $cost_profit_total_income_str = '' ;
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['sale_datas_origin'] == '1') {
                    $cost_profit_total_income_str =  " - report.byorder_sales_quota - report.byorder_refund_promote_discount " ;
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $cost_profit_total_income_str =  " - report.report_sales_quota - report.byorder_refund_promote_discount " ;
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['sale_datas_origin'] == '1') {
                    $cost_profit_total_income_str =  " - report.report_sales_quota - report.byorder_refund_promote_discount " ;
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $cost_profit_total_income_str =  " - report.report_sales_quota - report.byorder_refund_promote_discount " ;
                }
            }

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course {$cost_profit_total_income_str} )";
                        $time_fields = $this->getTimeFields($time_line, " report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course {$cost_profit_total_income_str} ");
                    } else {
                        $fields['count_total'] = "SUM(  report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit {$cost_profit_total_income_str} )";
                        $time_fields = $this->getTimeFields($time_line, " report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit {$cost_profit_total_income_str}");
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = "SUM( (report.byorder_goods_profit  + report.byorder_purchasing_cost  + report.byorder_logistics_head_course {$cost_profit_total_income_str} )* ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "(report.byorder_goods_profit  + report.byorder_purchasing_cost  + report.byorder_logistics_head_course {$cost_profit_total_income_str} ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    } else {
                        $fields['count_total'] = "SUM(  (report.first_purchasing_cost  + report.first_logistics_head_course  + report.byorder_goods_profit {$cost_profit_total_income_str})* ({:RATE} / COALESCE(rates.rate ,1))  )";
                        $time_fields = $this->getTimeFields($time_line, " (report.first_purchasing_cost  + report.first_logistics_head_course  + report.byorder_goods_profit {$cost_profit_total_income_str})* ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }

                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = "SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course {$cost_profit_total_income_str})";
                        $time_fields = $this->getTimeFields($time_line, " report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course {$cost_profit_total_income_str}");
                    } else {
                        $fields['count_total'] = "SUM( report.first_purchasing_cost + report.first_logistics_head_course +  report.report_goods_profit {$cost_profit_total_income_str} )";
                        $time_fields = $this->getTimeFields($time_line, "report.first_purchasing_cost + report.first_logistics_head_course +  report.report_goods_profit {$cost_profit_total_income_str}");
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = "SUM( (report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course {$cost_profit_total_income_str} ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, " (report.report_goods_profit + report.report_purchasing_cost  + report.report_logistics_head_course {$cost_profit_total_income_str} ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    } else {
                        $fields['count_total'] = "SUM(  ( report.first_purchasing_cost + report.first_logistics_head_course  + report.report_goods_profit {$cost_profit_total_income_str})  * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, " ( report.first_purchasing_cost  + report.first_logistics_head_course  + report.report_goods_profit {$cost_profit_total_income_str} ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }

                }
            }
        }else{
            $fields_tmp = $this->getTimeField($datas,$time_line);
            $fields['count_total']  = $fields_tmp['count_total'];
            $time_fields            = $fields_tmp['time_fields'];
        }


        $fields[$datas['time_target']] = $fields['count_total'] ;
        if(!empty($time_fields) && is_array($time_fields)){
            foreach($time_fields as $kt=>$time_field){
                $fields[$kt] = $time_field ;
            }
        }
        //$fields = array_merge($fields, $time_fields);
        return $fields;
    }

    private function getTimeFields($timeList, $field1 = '', $field2 = '', $fun = '')
    {
        $fields = array();
        if (empty($fun)) {
            $fun = 'SUM';
        }

        foreach ($timeList as $time) {
            if (empty($field2)) {
                $fields[strval($time['key'])] = "{$fun}(CASE WHEN (report.create_time>={$time['start']} and report.create_time<={$time['end']}) THEN ({$field1}) ELSE 0 END) ";
            } else {
                $fields[strval($time['key'])] = "({$fun}(CASE WHEN (report.create_time>={$time['start']} and report.create_time<={$time['end']}) THEN ({$field1}) ELSE 0 END))/nullif({$fun}(CASE WHEN (report.create_time>={$time['start']} and report.create_time<={$time['end']}) THEN ({$field2}) ELSE 0 END),0) ";
            }
        }

        return $fields;
    }

    /**
     * 获取时间字段
     * @author json.qiu 2021/03/04
     *
     * @param $datas
     * @param $timeLine
     * @param int $type 1表示商品，2表示店铺类型，3表示运营人员类型
     * @return array
     */
    private function getTimeField($datas,$timeLine,$type = 1)
    {
        $fields = [];
        $fields['count_total']  = '1';
        $time_fields            = '1';
        switch ($datas['time_target']){
            case 'fba_sales_quota':
                $goods_time_filed   = $this->handleTimeFields($datas,$timeLine,6,'report.byorder_fba_sales_quota','report.report_fba_sales_quota');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'fbm_sales_quota':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,6,'report.byorder_fbm_sales_quota','report.report_fbm_sales_quota');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'fba_refund_num':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,4,'report.byorder_fba_refund_num','report.report_fba_refund_num');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'fbm_refund_num':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,4,'report.byorder_fbm_refund_num','report.report_fbm_refund_num');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'fba_logistics_head_course':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,5,'report.byorder_fba_logistics_head_course','report.report_fba_logistics_head_course','report.fba_first_logistics_head_course');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'fbm_logistics_head_course':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,5,'report.byorder_fbm_logistics_head_course','report.report_fbm_logistics_head_course','report.fbm_first_logistics_head_course');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'shipping_charge':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,1,'report.byorder_shipping_charge','report.report_shipping_charge');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'tax':
                $goods_time_filed = $this->handleTimeFields($datas,$timeLine,1,'report.byorder_tax','report.report_tax');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'ware_house_lost':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,1,'report.byorder_ware_house_lost','report.report_ware_house_lost');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'ware_house_damage':
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,1,'report.byorder_ware_house_damage','report.report_ware_house_damage');
                $fields['count_total']  = $goods_time_filed['count_total'];
                $time_fields            = $goods_time_filed['time_fields'];
                break;
            case 'channel_fbm_safe_t_claim_demage':
                if ($type == 2 or $type == 3){
                    $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,1,'report.channel_fbm_safe_t_claim_demage','report.channel_fbm_safe_t_claim_demage');
                    $fields['count_total']  = $goods_time_filed['count_total'];
                    $time_fields            = $goods_time_filed['time_fields'];
                }
                break;
            default:
                $fields['count_total']  = '1';
                $time_fields            = '1';
                break;
        }

        return array(
            "count_total" => $fields['count_total'],
            "time_fields" => $time_fields,
        );
    }

    /**
     * 处理字段的格式
     * @author json.qiu 2021/03/04
     *
     * @param $datas
     * @param $timeLine
     * @param $data_origin
     * @param $by_order_fields
     * @param $report_fields
     * @param $first_fields
     * @return array
     */
    private function handleTimeFields($datas,$timeLine,$data_origin,$by_order_fields,$report_fields,$first_fields = '')
    {
        switch ($data_origin){
            case 1://财务数据源,且包含货币
                if($datas['finance_datas_origin'] == 1){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($by_order_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                    } else {
                        $fields['count_total'] = "SUM(({$by_order_fields}) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, "({$by_order_fields}) * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($report_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $report_fields);
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, "($report_fields) * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
                break;
            case 2://财务数据源不包含货币
                if($datas['finance_datas_origin'] == 1){
                    $fields['count_total'] = "SUM($by_order_fields)";
                    $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                }else{
                    $fields['count_total'] = "SUM($report_fields)";
                    $time_fields = $this->getTimeFields($timeLine, $report_fields);
                }
                break;
            case 3://退款数据源，包含货币
                break;
            case 4://退款数据源，不包含货币
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM ({$by_order_fields} )";
                    $time_fields = $this->getTimeFields($timeLine, "{$by_order_fields}");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM ({$report_fields})";
                    $time_fields = $this->getTimeFields($timeLine, "{$report_fields}");
                }
                break;
            case 5://财务数据源包含货币包含先进先出
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " SUM ( {$by_order_fields} ) ";
                            $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                        } else {
                            $fields['count_total'] = " SUM ( {$by_order_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$by_order_fields} * ({:RATE} / COALESCE(rates.rate ,1))");
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " SUM ( {$report_fields} ) ";
                            $time_fields = $this->getTimeFields($timeLine, $report_fields);
                        } else {
                            $fields['count_total'] = " SUM ( {$report_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$report_fields} * ({:RATE} / COALESCE(rates.rate ,1))");
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM (( {$first_fields}) ) ";
                        $time_fields = $this->getTimeFields($timeLine, " ({$first_fields})");
                    } else {
                        $fields['count_total'] = " SUM ( ({$first_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                        $time_fields = $this->getTimeFields($timeLine, "({$first_fields} * ({:RATE} / COALESCE(rates.rate ,1)) )");
                    }
                }
                break;
            case 6://销售数据源,且包含货币
                if($datas['sale_datas_origin'] == 1){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($by_order_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                    } else {
                        $fields['count_total'] = "SUM(({$by_order_fields}) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, "({$by_order_fields}) * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($report_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $report_fields);
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, "($report_fields) * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
                break;
            default:
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM (report.byorder_refund_num )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM (report.report_refund_num )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num");
                }
                break;
        }

        return [
            "count_total" => $fields['count_total'],
            "time_fields" => $time_fields
        ];
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
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_sales_quota'] = "SUM(report.byorder_fba_sales_quota)";
                } else {
                    $fields['fba_sales_quota'] = "SUM(report.byorder_fba_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_sales_quota'] = "SUM(report.report_fba_sales_quota)";
                } else {
                    $fields['fba_sales_quota'] = "SUM((0-report.report_fba_sales_quota) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }

        if (in_array('fbm_sales_quota', $targets)) { //FBM商品销售额
            if($datas['sale_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_sales_quota'] = "SUM(report.byorder_fbm_sales_quota)";
                } else {
                    $fields['fbm_sales_quota'] = "SUM(report.byorder_fbm_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_sales_quota'] = "SUM(report.report_fbm_sales_quota)";
                } else {
                    $fields['fbm_sales_quota'] = "SUM((0-report.report_fbm_sales_quota) * ({:RATE} / COALESCE(rates.rate ,1)))";
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
                        $fields['fba_logistics_head_course'] = "SUM(report.byorder_fba_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))";
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fba_logistics_head_course'] = "SUM(report.report_fba_logistics_head_course)";
                    } else {
                        $fields['fba_logistics_head_course'] = "SUM((report.report_fba_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    }
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fba_logistics_head_course'] = "SUM(report.fba_first_logistics_head_course)";
                } else {
                    $fields['fba_logistics_head_course'] = "SUM((report.fba_first_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }

        }

        if (in_array('fbm_logistics_head_course', $targets)) { //fbm物流
            if($datas['cost_count_type'] == 1){
                if($datas['finance_datas_origin'] == 1){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = "SUM(report.byorder_fbm_logistics_head_course)";
                    } else {
                        $fields['fbm_logistics_head_course'] = "SUM(report.byorder_fbm_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))";
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = "SUM(report.report_fbm_logistics_head_course)";
                    } else {
                        $fields['fbm_logistics_head_course'] = "SUM((report.report_fbm_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    }
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['fbm_logistics_head_course'] = "SUM(report.fbm_first_logistics_head_course)";
                } else {
                    $fields['fbm_logistics_head_course'] = "SUM((report.fbm_first_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }

        }

        if (in_array('shipping_charge', $targets)) { //运费
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['shipping_charge'] = "SUM(report.byorder_shipping_charge)";
                } else {
                    $fields['shipping_charge'] = "SUM(report.byorder_shipping_charge * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['shipping_charge'] = "SUM(report.report_shipping_charge)";
                } else {
                    $fields['shipping_charge'] = "SUM((report.report_shipping_charge) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }


        if (in_array('tax', $targets)) { //TAX（销售）
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['tax'] = "SUM(report.byorder_tax)";
                } else {
                    $fields['tax'] = "SUM(report.byorder_tax * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['tax'] = "SUM(report.report_tax)";
                } else {
                    $fields['tax'] = "SUM(report.report_tax * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }

        if (in_array('ware_house_lost', $targets)) { //FBA仓丢失赔款
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_lost'] = "SUM(report.byorder_ware_house_lost)";
                } else {
                    $fields['ware_house_lost'] = "SUM(report.byorder_ware_house_lost * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_lost'] = "SUM(report.report_ware_house_lost)";
                } else {
                    $fields['ware_house_lost'] = "SUM(report.report_ware_house_lost * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }

        if (in_array('ware_house_damage', $targets)) { //FBA仓损坏赔款
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_damage'] = "SUM(report.byorder_ware_house_damage)";
                } else {
                    $fields['ware_house_damage'] = "SUM(report.byorder_ware_house_damage * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['ware_house_damage'] = "SUM(report.report_ware_house_damage)";
                } else {
                    $fields['ware_house_damage'] = "SUM(report.report_ware_house_damage * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }

        if ($type == 2 or $type == 3){//店铺和运营人员才有的
            if (in_array('channel_fbm_safe_t_claim_demage', $targets)) { //SAF-T
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage)";
                } else {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
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
        int $adminId = 0 ,
        array $rateInfo = []
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

        $mod_where = "report.user_id_mod = " . ($params['user_id'] % 20);


        $ym_where = $this->getYnWhere($params['max_ym'] , $params['min_ym'] ) ;

        if(($params['count_periods'] == 0 || $params['count_periods'] == 1) && $params['cost_count_type'] != 2){ //按天或无统计周期
            $table = "{$this->table_channel_day_report} AS report";
            $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;
        }else if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){  //按周
            $table = "{$this->table_channel_week_report} AS report" ;
        }else if($params['count_periods'] == 3 || $params['count_periods'] == 4 || $params['count_periods'] == 5 ){
            $table = "{$this->table_channel_month_report} AS report" ;
        }else if($params['cost_count_type'] == 2 ){
            $table = "{$this->table_channel_month_report} AS report" ;
        } else {
            return [];
        }

        //部门维度统计
        if ($params['count_dimension'] == 'department') {
            $table .= " LEFT JOIN {$this->table_department_channel} as dc ON dc.user_id = report.user_id AND dc.channel_id = report.channel_id  LEFT JOIN {$this->table_user_department} as ud ON ud.id = dc.user_department_id ";
            $where .= " AND ud.status < 3";
            $admin_info = UserAdminModel::query()->select('is_master', 'is_responsible', 'user_department_id')->where('user_id', 304)->where('id', 400)->first();
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
            $table .= " LEFT JOIN {$this->table_user_channel} as uc ON uc.user_id = report.user_id AND uc.channel_id = report.channel_id ";
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
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id ";
            }
        }

        $having = '';
        $where_detail = is_array($params['where_detail']) ? $params['where_detail'] : json_decode($params['where_detail'], true);
        if (empty($where_detail)) {
            $where_detail = [];
        }
        $group = '';
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
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByUnGoods Total Request', [$this->getLastSql()]);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByUnGoods Request', [$this->getLastSql()]);
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
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        //订单金额
        if (in_array('sale_sales_dollars', $targets) || in_array('cpc_cost_rate', $targets)) {
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['sale_sales_dollars'] = "SUM ( report.bychannel_sales_quota )";
            } else {
                $fields['sale_sales_dollars'] = "SUM ( report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                    $fields['sale_refund'] = "SUM ( ( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "SUM ( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
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
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('promote_store_fee', $targets)) { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
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
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
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
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            }

        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('cost_profit_total_pay', $targets) ) {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM(report.byorder_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                } else {
                    $fields['cost_profit_profit'] = "SUM(report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM(report.report_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
                } else {
                    $fields['cost_profit_profit'] = "SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}";
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
                    $fields['amazon_fee'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }
        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM ( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM ( report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM ( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM ( report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                $fields['operate_fee'] = "SUM ( report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
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
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
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
                    $fields['other_vat_fee'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }

        }

        if (in_array('other_other_fee', $targets)) { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  + report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_review_enrollment_fee  * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('other_review_enrollment_fee', $targets)) { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee  * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('cpc_ad_settlement', $targets)) { //广告结款
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_ad_settlement'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
            } else {
                $fields['cpc_ad_settlement'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('cpc_sp_cost', $targets)) {  //CPC_SP花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets)) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " SUM ( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }


        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
            } else {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
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
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
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
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_reserved_field5 * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report.bychannel_reserved_field6 * ({:RATE} / COALESCE(rates.rate ,1))   )';
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
                $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('promote_run_lightning_deal_fee', $targets)) {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
            } else {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
            }
        }
        if (in_array('amazon_order_fee', $targets)) {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }

            }
        }
        if (in_array('amazon_refund_fee', $targets)) { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            }
        }
        if (in_array('amazon_stock_fee', $targets)) { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }
        if (in_array('goods_adjust_fee', $targets)) { //商品调整费用

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }

        }

        if (in_array('cost_profit_total_income', $targets) || in_array('cost_profit_total_pay', $targets)  ) {  //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('cost_profit_total_pay', $targets)) {  //总支出
            $fields['cost_profit_total_pay'] = $fields['cost_profit_profit'] . '-' .  $fields['cost_profit_total_income'] ;
        }


        $this->getUnTimeFields($fields, $datas, $targets, 2);

        return $fields;
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
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'sale_sales_dollars') { //订单金额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM ( report.bychannel_sales_quota )";
                $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota");
            } else {
                $fields['count_total'] = "SUM ( report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
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
                    $fields['count_total'] = "SUM ( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, " (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_refund )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund ");
                } else {
                    $fields['count_total'] = "SUM ( report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ");
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
                    $fields['count_total'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'promote_refund_discount') {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'promote_store_fee') { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee +  report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1))');
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
                        $fields['count_total'] = 'SUM(report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = 'SUM(  (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                        $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))');
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
                        $fields['count_total'] = 'SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = 'SUM((report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))  )';
                        $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_sales_commission') {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_delivery_fee') {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_profit ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_profit ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_settlement_fee') {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }

        } else if ($datas['time_target'] == 'amazon_other_fee') {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_shipping_fee') {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_returnshipping )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_refund_deducted_commission') {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }

            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (( report.first_purchasing_cost )) ";
                    $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_purchasing_cost / COALESCE(rates.rate ,1)) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_logistics_cost') {  // 物流/头程
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == 1) {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }

            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    $time_fields = $this->getTimeFields($timeLine, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))');
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
                $fields['count_total'] = "SUM ( report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                    $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($timeLine, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                    $time_fields = $this->getTimeFields($timeLine, '0-report.report_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($timeLine, '(0-report.report_reserved_field17 )* ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }

        } else if ($datas['time_target'] == 'other_other_fee') { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment + report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
            }
        } else if ($datas['time_target'] == 'other_review_enrollment_fee') { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))  )";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
            }
        } else if ($datas['time_target'] == 'cpc_ad_settlement') { //广告结款
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund ');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1)))";
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_sp_cost') {  //CPC SP 花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost  ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost ');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_sp_cost') {  //CPC SD 花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_sd_cost  ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_sd_cost ');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_cost') {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_cost_rate') {  //CPC花费占比
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) )  /  nullif( SUM (report.bychannel_sales_quota ) , 0 )";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)', 'report.bychannel_sales_quota');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )  /  nullif( SUM (report.bychannel_sales_quota ) , 0 ) ";
                $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1))  ', 'report.bychannel_sales_quota');
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
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))');
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
                $fields['count_total'] = '(SUM ( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) - COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) - COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
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
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
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
                $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) )';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } elseif ($datas['time_target'] == 'promote_run_lightning_deal_fee') {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee');
            } else {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } elseif ($datas['time_target'] == 'amazon_order_fee') {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee ');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }

            }
        } elseif ($datas['time_target'] == 'amazon_refund_fee') { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_stock_fee') { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_storage_fee + report.bychannel_channel_amazon_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_long_term_storage_fee') { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } elseif ($datas['time_target'] == 'goods_adjust_fee') { //商品调整费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        }else if($datas['time_target'] == 'cost_profit_total_income'){ //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if($datas['time_target'] == 'cost_profit_total_pay'){ //总支出
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.byorder_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.byorder_sales_quota');
                        }else{
                            $fields['count_total'] = 'SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.byorder_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.report_sales_quota');
                        }
                    } else {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota');
                        }else{
                            $fields['count_total'] = 'SUM((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota');
                        }
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM(report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        $fields['count_total'] = 'SUM(  (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                        $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.byorder_sales_quota )';
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.byorder_sales_quota');
                        }else{
                            $fields['count_total'] = 'SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.report_sales_quota )';
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.report_sales_quota');
                        }
                    } else {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota');
                        }else{
                            $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota)';
                            $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota');
                        }
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                        }else{
                            $fields['count_total'] = 'SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        if ($datas['sale_datas_origin'] == '1') {
                            $fields['count_total'] = 'SUM((report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )';
                            $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                        }else{
                            $fields['count_total'] = 'SUM((report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )';
                            $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    }
                }
            }
        }else{
            $fields_tmp = $this->getTimeField($datas,$timeLine,2);
            $fields['count_total']  = $fields_tmp['count_total'];
            $time_fields            = $fields_tmp['time_fields'];

        }

        $fields[$datas['time_target']] = $fields['count_total'] ;
        if(!empty($time_fields) && is_array($time_fields)){
            foreach($time_fields as $kt=>$time_field){
                $fields[$kt] = $time_field ;
            }
        }
        return $fields;
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
            $table = "{$this->table_amazon_fba_inventory_by_channel} as c";
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
                $table .= " LEFT JOIN {$this->table_department_channel} as dc ON dc.user_id = c.user_id and dc.channel_id = c.channel_id " ;
            }else if($datas['count_dimension'] == 'admin_id'){ //子账号
                $fba_fields = $group = 'uc.admin_id , c.area_id' ;
                $table .= " LEFT JOIN {$this->table_user_channel} as uc ON uc.user_id = c.user_id and uc.channel_id = c.channel_id " ;
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
                    $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = c.site_id AND rates.user_id = 0 ";
                } else {
                    $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = c.site_id AND rates.user_id = c.user_id  ";
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
            }else{
                $where_str = '1=1' ;
            }
        }

        $amazon_fba_inventory_by_channel_md = new AmazonFbaInventoryByChannelPrestoModel($this->dbhost, $this->codeno);
        $amazon_fba_inventory_by_channel_md->dryRun(env('APP_TEST_RUNNING', false));
        $where.= ' AND ' . $where_str ;
        if ($datas['currency_code'] == 'ORIGIN') {
            $fba_fields .= " , SUM (DISTINCT(c.yjzhz))  as fba_goods_value";
        } else {
            $fba_fields .= " , SUM (DISTINCT(c.yjzhz * ({:RATE} / COALESCE(rates.rate ,1))))  as fba_goods_value";
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
     * 获取运营人员维度统计列表
     * @param string $where
     * @param array $datas
     * @param string $limit
     * @param string $sort
     * @param string $order
     * @param int $count_tip
     * @param array $channel_arr
     * @return array
     * @author: LWZ
     */
    public function getListByOperators(
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
        int $adminId = 0 ,
        array $rateInfo = []
    ) {
        //没有按周期统计 ， 按指标展示
        if ($datas['show_type'] == 2) {
            $fields = $this->getOperatorsFields($datas);
        } else {
            $fields = $this->getOperatorsTimeFields($datas, $timeLine);
        }
        if (empty($fields)) {
            return array();
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


        $mod_where = "report.user_id_mod = " . ($datas['user_id'] % 20);

        $ym_where = $this->getYnWhere($datas['max_ym'] , $datas['min_ym'] ) ;

        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));

        if(($datas['count_periods'] == 0 || $datas['count_periods'] == 1) && $datas['cost_count_type'] != 2){ //按天或无统计周期
            $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;
            $table = "{$this->table_operation_day_report} AS report" ;
        }else if($datas['count_periods'] == 2 && $datas['cost_count_type'] != 2){  //按周
            $table = "{$this->table_operation_week_report} AS report" ;
        }else if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
            $table = "{$this->table_operation_month_report} AS report";
        }else if($datas['cost_count_type'] == 2){//先进先出只能读取月报
            $table = "{$this->table_operation_month_report} AS report";
        } else {
            return [];
        }


        if (!empty($where_detail['operators_id'])) {
            if(is_array($where_detail['operators_id'])){
                $operators_str = implode(',', $where_detail['operators_id']);
            }else{
                $operators_str = $where_detail['operators_id'] ;
            }
            $where .= " AND report.goods_operation_user_admin_id  IN ( " . $operators_str . " ) ";
        }

        if ($datas['currency_code'] != 'ORIGIN') {
            if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id  ";
            }
        }
        if ($datas['count_periods'] > 0 && $datas['show_type'] == '2') {
            if($datas['count_periods'] == '4'){ //按季度
                $group = 'report.goods_operation_user_admin_id , report.myear , report.mquarter ';
                $orderby = 'report.goods_operation_user_admin_id ,report.myear , report.mquarter ';
            }else if($datas['count_periods'] == '5') { //年
                $group = 'report.goods_operation_user_admin_id , report.myear' ;
                $orderby = 'report.goods_operation_user_admin_id , report.myear ';
            }else {
                $group = 'report.goods_operation_user_admin_id_group  ';
                $orderby = "report.goods_operation_user_admin_id_group ";
            }

        }else{
            $group = 'report.goods_operation_user_admin_id  ';
            $orderby = empty($orderby) ? ('report.goods_operation_user_admin_id ') : ($orderby . ' , report.goods_operation_user_admin_id');
        }
        $having = '';
        $where .= " AND report.goods_operation_user_admin_id > 0";

        if (!empty($where_detail)) {
            $target_wheres = empty($where_detail['target']) ? array() : $where_detail['target'];
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
        $orderby = str_replace("{:RATE}", $exchangeCode, $orderby);
        $limit_num = 0 ;
        if($datas['show_type'] == 2 && $datas['limit_num'] > 0 ){
            $limit_num = $datas['limit_num'] ;
        }
        if ($count_tip == 2) { //仅统计总条数
            $count = $this->getTotalNum($where, $table, $group);
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        } else if ($count_tip == 1) {  //仅仅统计列表
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table, $limit);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);

            }
        } else {  //统计列表和总条数
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                $lists = $this->select($where, $field_data, $table, $limit);
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByOperators Total Request', [$this->getLastSql()]);
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByOperators Request', [$this->getLastSql()]);
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
        if(!empty($lists) && $datas['show_type'] = 2 && $datas['limit_num'] > 0 && !empty($order) && !empty($sort) && !empty($fields[$sort]) && !empty($fields[$datas['sort_target']]) && !empty($datas['sort_target']) && !empty($datas['sort_order'])){
            //根据字段对数组$lists进行排列
            $sort_names = array_column($lists,$sort);
            $order2  =  $order == 'desc' ? \SORT_DESC : \SORT_ASC;
            array_multisort($sort_names,$order2,$lists);
        }

        $rt['lists'] = empty($lists) ? array() : $lists;
        $rt['count'] = intval($count);
        return $rt;
    }

    //获取运营人员维度指标字段
    private function getOperatorsFields($datas = array())
    {
        $fields = array();
        $fields['user_id'] = 'max(report.user_id)';
        $fields['goods_operation_user_admin_id'] = 'max(report.goods_operation_user_admin_id)';

        if ($datas['count_periods'] == '1' && $datas['show_type'] == '2') { //按天
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-', cast(max(report.mmonth) as varchar), '-', cast(max(report.mday) as varchar))";
        } else if ($datas['count_periods'] == '2' && $datas['show_type'] == '2') { //按周
            $fields['time'] = "concat(cast(max(report.mweekyear) as varchar), '-', cast(max(report.mweek) as varchar))";
        } else if ($datas['count_periods'] == '3' && $datas['show_type'] == '2') { //按月
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-', cast(max(report.mmonth) as varchar))";
        } else if ($datas['count_periods'] == '4' && $datas['show_type'] == '2') {  //按季
            $fields['time'] = "concat(cast(max(report.myear) as varchar), '-',  cast(max(report.mquarter) as varchar))";
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

        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets)  || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
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
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
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
                    $fields['sale_refund'] = "SUM ( (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "SUM ( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "SUM ( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = $fields['sale_return_goods_number'] . " * 1.0 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END)';
                } else {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee +    report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee  ) END )';
                } else {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)))  ELSE (report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))   ) END)';
                }
            }

        }

        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = '(' . $fields['amazon_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('amazon_order_fee', $targets)) {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                } else {
                    $fields['amazon_order_fee'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }

            }
        }

        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }

        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }

        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_refund_fee', $targets)) { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                } else {
                    $fields['amazon_refund_fee'] = 'SUM(report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_stock_fee', $targets)) { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.byorder_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END )';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN ( report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.report_channel_amazon_storage_fee) ELSE report.report_estimated_monthly_storage_fee END )';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_estimated_monthly_storage_fee* ({:RATE} / COALESCE(rates.rate ,1))) END )';
                }
            }
        }

        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END  )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.report_estimated_monthly_storage_fee  END )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) END  )";
                }
            }
        }

        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }

        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ELSE report.byorder_goods_amazon_other_fee END ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END   ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END   ) ";
                } else {
                    $fields['amazon_other_fee'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ) ";
                }
            }
        }

        if (in_array('promote_discount', $targets)) {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount)";
                } else {
                    $fields['promote_discount'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('promote_refund_discount', $targets)) {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['promote_refund_discount'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('promote_store_fee', $targets)) { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                } else {
                    $fields['promote_store_fee'] = 'SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                }
            }
        }

        if (in_array('promote_coupon', $targets)) { //coupon优惠券
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
            } else {
                $fields['promote_coupon'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('promote_run_lightning_deal_fee', $targets)) {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
            } else {
                $fields['promote_run_lightning_deal_fee'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
            }
        }

        if (in_array('cpc_ad_fee', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //广告费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_ad_fee'] = " SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END ) ";
            } else {
                $fields['cpc_ad_fee'] = " SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) END) ";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = '(' . $fields['cpc_ad_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3 )";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = '('.$fields['cpc_click_number'].')' . " / nullif( " . $fields['cpc_exposure'] . " , 0 ) ";
        }
        if (in_array('cpc_order_number', $targets) ||  in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
        }

        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = '('.$fields['cpc_order_number'] . ") / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }

        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5" )';
            } else {
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }

        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = '(' . $fields['cpc_turnover'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = '('.$fields['cpc_ad_fee'] . ") / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = '('.$fields['cpc_ad_fee'] . ") / nullif( " . $fields['cpc_turnover'] . " , 0 ) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = '(' . $fields['cpc_direct_sales_volume'] . ") / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8) ';
        }

        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report.bychannel_reserved_field5 - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report.bychannel_reserved_field6 )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_reserved_field5 * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report.bychannel_reserved_field6 * ({:RATE} / COALESCE(rates.rate ,1))   )';
            }
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = '(' . $fields['cpc_indirect_sales_volume'] . ") / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('goods_adjust_fee', $targets)) { //商品调整费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }

        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = '(' . $fields['evaluation_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    }
                }
            }

        }

        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            }
        }

        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = '(' . $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "SUM ( 0- report.byorder_reserved_field16 + report.bychannel_operating_fee ) ";
            } else {
                $fields['operate_fee'] = "SUM ( (0 -  report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = '(' . $fields['operate_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('other_vat_fee', $targets)) {//VAT
            if($datas['finance_datas_origin'] == 1){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.byorder_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['other_vat_fee'] = "SUM(0-report.report_reserved_field17)";
                } else {
                    $fields['other_vat_fee'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }

        }

        if (in_array('other_other_fee', $targets)) { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  + report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_other_fee'] = "SUM(report.bychannel_loan_payment  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_review_enrollment_fee  * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('other_review_enrollment_fee', $targets)) { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee)";
            } else {
                $fields['other_review_enrollment_fee'] = "SUM(report.bychannel_review_enrollment_fee  * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //毛利润
            $purchase_logistics = $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'];
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.byorder_goods_profit ) END )+ $purchase_logistics";
                } else {
                    $fields['cost_profit_profit'] = "SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) END  ) + $purchase_logistics ";
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = "SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.report_goods_profit ) END  ) + $purchase_logistics";
                } else {
                    $fields['cost_profit_profit'] = "SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) END )+$purchase_logistics ";
                }
            }

        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = "(" . $fields['cost_profit_profit'] . ")" . " /  nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        $this->getUnTimeFields($fields,$datas,$targets,3);

        return $fields;

    }

    //按运营人员维度,时间展示字段（新增统计维度完成）
    private function getOperatorsTimeFields($datas = array(), $time_line)
    {
        $fields = array();
        $fields['user_id'] = 'max(report.user_id)';
        $fields['goods_operation_user_admin_id'] = 'max(report.goods_operation_user_admin_id)';
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
        $time_fields = array();
        if ($datas['time_target'] == 'goods_visitors') {  // 买家访问次数
            $fields['count_total'] = "SUM(report.byorder_user_sessions)";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_user_sessions');
        } else if ($datas['time_target'] == 'goods_conversion_rate') { //订单商品数量转化率
            $fields['count_total'] = 'SUM ( report.byorder_quantity_of_goods_ordered ) / nullif(SUM ( report.byorder_user_sessions ) ,0)';
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
        }    else if ($datas['time_target'] == 'sale_sales_volume') { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = " SUM ( report.byorder_sales_volume  +  report.byorder_group_id) ";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_volume  +  report.byorder_group_id");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = " SUM ( report.report_sales_volume  +  report.byorder_group_id ) ";
                $time_fields = $this->getTimeFields($time_line, "report.report_sales_volume  +  report.byorder_group_id");
            }
        } else if ($datas['time_target'] == 'sale_many_channel_sales_volume') { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM ( report.byorder_group_id )";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_group_id");
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = "SUM ( report.report_group_id )";
                $time_fields = $this->getTimeFields($time_line, "report.report_group_id");
            }
        } else if ($datas['time_target'] == 'sale_sales_quota') {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'sale_return_goods_number') {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['count_total'] = "SUM (report.byorder_refund_num )";
                $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num");
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['count_total'] = "SUM (report.report_refund_num )";
                $time_fields = $this->getTimeFields($time_line, "report.report_refund_num");
            }
        } else if ($datas['time_target'] == 'sale_refund') {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( 0 - report.byorder_refund )";
                    $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund )");
                } else {
                    $fields['count_total'] = "SUM ( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( 0 - report.report_refund )";
                    $time_fields = $this->getTimeFields($time_line, " ( 0 - report.report_refund ) ");
                } else {
                    $fields['count_total'] = "SUM ( ( 0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, " (0 - report.report_refund )* ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            }
        } else if ($datas['time_target'] == 'sale_refund_rate') {  //退款率
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM (report.byorder_refund_num ) * 1.0 / nullif(SUM(report.byorder_sales_volume + report.byorder_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0", "report.byorder_sales_volume+ report.byorder_group_id");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM (report.report_refund_num  ) * 1.0 / nullif(SUM(report.byorder_sales_volume+ report.byorder_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0 ", "report.byorder_sales_volume+ report.byorder_group_id");
                }
            }else{
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM (report.byorder_refund_num ) * 1.0  / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "SUM (report.report_refund_num ) * 1.0 / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee') {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )');
                } else {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE  (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ) ';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE  (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )');
                } else {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (  report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (  report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fee_rate') {  //亚马逊费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END ) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )', 'report.byorder_sales_quota');

                } elseif ($datas['finance_datas_origin'] == '2') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END ) /  nullif( SUM (report.byorder_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END ) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )', 'report.report_sales_quota');

                } elseif ($datas['finance_datas_origin'] == '2') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END ) /  nullif( SUM (report.report_sales_quota) , 0 ) ';
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.report_goods_amazon_fee ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )', 'report.report_sales_quota');
                }
            }
        }else if ($datas['time_target'] == 'amazon_order_fee') {  //亚马逊-订单费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee ');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_amazon_order_fee + report.bychannel_channel_amazon_order_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }

            }
        }else if ($datas['time_target'] == 'amazon_sales_commission') {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_platform_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_delivery_fee') {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }

        } else if ($datas['time_target'] == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_settlement_fee') {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        }else if ($datas['time_target'] == 'amazon_refund_fee') { //亚马逊-退货退款费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_amazon_refund_fee + report.bychannel_channel_amazon_refund_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        }else if ($datas['time_target'] == 'amazon_return_shipping_fee') {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_refund_deducted_commission') {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_stock_fee') { //亚马逊-库存费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.byorder_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END )';
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.byorder_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END');
                } else {
                    $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee  COALESCE(rates.rate ,1) * {:RATE}) END )';
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee  COALESCE(rates.rate ,1) * {:RATE}) END ');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee+report.report_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END )';
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.report_channel_amazon_storage_fee) ELSE report.report_estimated_monthly_storage_fee END');
                } else {
                    $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee  COALESCE(rates.rate ,1) * {:RATE}) END )';
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_estimated_monthly_storage_fee  COALESCE(rates.rate ,1) * {:RATE}) END');
                }
            }
        }else if ($datas['time_target'] == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END  )";
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END');
                } else {
                    $fields['count_total'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.report_estimated_monthly_storage_fee END  )";
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.report_estimated_monthly_storage_fee END');
                } else {
                    $fields['count_total'] = "SUM ( CASE  WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                    $time_fields = $this->getTimeFields($time_line, 'CASE  WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_long_term_storage_fee') { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee + report.bychannel_fba_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee  * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_fba_long_term_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_other_fee') {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.byorder_goods_amazon_other_fee END ) ";
                    $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.byorder_goods_amazon_other_fee END )');
                } else {
                    $fields['count_total'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  ) ";
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END ) ";
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END )');
                } else {
                    $fields['count_total'] = "SUM (CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  ) ";
                    $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )');
                }
            }
        }  else if ($datas['time_target'] == 'promote_discount') {  //promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'promote_refund_discount') {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) ");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
        } else if ($datas['time_target'] == 'promote_store_fee') { //店铺促销费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee +  report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee + report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)))';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_refund_promote_discount * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_promote_discount * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }
        }else if ($datas['time_target'] == 'promote_coupon') { //coupon优惠券
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax"');
            } else {
                $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) )';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } elseif ($datas['time_target'] == 'promote_run_lightning_deal_fee') {  //RunLightningDealFee';
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_run_lightning_deal_fee');
            } else {
                $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_ad_fee') {  //广告费用
            if ($datas['currency_code'] == 'ORIGIN') {//由于byorder_cpc_cost 和report_cpc_cost 实际一样，所以不用区分
                $fields['count_total'] = " SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END ) ";
                $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END )');
            } else {
                $fields['count_total'] = " SUM ( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) END) ";
                $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) END) ');
            }
        } else if ($datas['time_target'] == 'cpc_cost_rate') {  //CPC花费占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM(  report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) /nullif(SUM(report.byorder_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, '  report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = "SUM(  report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) /nullif(SUM(report.report_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, '  report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_exposure') {  //CPC曝光量
            $fields['count_total'] = "SUM ( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3 )";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');
        } else if ($datas['time_target'] == 'cpc_click_number') {  //CPC点击次数
            $fields['count_total'] = "SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
        } else if ($datas['time_target'] == 'cpc_click_rate') {  //CPC点击率
            $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )) / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3), 0 ) ";
            $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4', 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');

        } else if ($datas['time_target'] == 'cpc_order_number') {  //CPC订单数
            $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7');
        } else if ($datas['time_target'] == 'cpc_click_conversion_rate') {  //cpc点击转化率
            $fields['count_total'] = '(SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 )) /nullif (SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4) , 0 )';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
        } else if ($datas['time_target'] == 'cpc_turnover') {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_turnover_rate') {  //CPC成交额占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM (  report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5")/nullif( SUM(report.byorder_sales_quota),0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = 'SUM (  report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5")/nullif( SUM(report.report_sales_quota),0)';
                $time_fields = $this->getTimeFields($time_line, '  report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'cpc_avg_click_cost') {  //CPC平均点击花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            } else {
                $fields['count_total'] = 'SUM (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))  +  report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))  +  report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            }

        } else if ($datas['time_target'] == 'cpc_acos') {  // ACOS
            $fields['count_total'] = 'SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost  ) / nullif( SUM ( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"  ) , 0 ) ';
            $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5" ');

        } else if ($datas['time_target'] == 'cpc_direct_sales_volume') {  //CPC直接销量
            $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
            $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ');
        } else if ($datas['time_target'] == 'cpc_direct_sales_quota') {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6"  )';
                $time_fields = $this->getTimeFields($time_line, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" ');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  ) /nullif(SUM ( report.byorder_sales_volume+ report.byorder_group_id ) ,0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.byorder_sales_volume+ report.byorder_group_id');
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ) /nullif(SUM ( report.report_sales_volume + report.report_group_id  ) ,0)';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.report_sales_volume+ report.report_group_id');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume') {  //CPC间接销量
            $fields['count_total'] = ' SUM(report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8 )';
            $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8');
        } else if ($datas['time_target'] == 'cpc_indirect_sales_quota') {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5" - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6" )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6"');
            } else {
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" ) /nullif(SUM ( report.byorder_sales_volume + report.byorder_group_id) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.byorder_sales_volume + report.byorder_group_id');
            } else {
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" ) /nullif(SUM ( report.report_sales_volume + report.report_group_id) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.report_sales_volume+ report.report_group_id');
            }
        } else if ($datas['time_target'] == 'goods_adjust_fee') { //商品调整费用

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }

        } else if ($datas['time_target'] == 'evaluation_fee') {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            }

        } else if ($datas['time_target'] == 'evaluation_fee_rate') {  //测评费用占比
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10', 'report.byorder_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.byorder_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10', 'report.byorder_sales_quota');
                }

            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.byorder_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10', 'report.report_sales_quota');
                }else{
                    $fields['count_total'] = "SUM(report.report_reserved_field10 ) /nullif(SUM(report.report_sales_quota),0)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10', 'report.report_sales_quota');
                }

            }
        } else if ($datas['time_target'] == 'purchase_logistics_purchase_cost') {  //采购成本
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (  (report.first_purchasing_cost) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                }
            }

        } else if ($datas['time_target'] == 'purchase_logistics_logistics_cost') {  // 物流/头程
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (( report.first_logistics_head_course) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_logistics_head_course)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )');
                }
            }
        } else if ($datas['time_target'] == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.byorder_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.byorder_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost)) / nullif(SUM ( report.byorder_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)', 'report.byorder_sales_quota');
                }
            } else {
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course + report.byorder_purchasing_cost ) / nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course  + report.byorder_purchasing_cost ', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course + report.report_purchasing_cost  )/ nullif(SUM ( report.report_sales_quota ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course + report.report_purchasing_cost', 'report.report_sales_quota');
                    }
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course + report.first_purchasing_cost)) / nullif(SUM ( report.report_sales_quota ),0)  ";
                    $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)', 'report.report_sales_quota');
                }
            }
        }  else if ($datas['time_target'] == 'operate_fee') {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM (0- report.byorder_reserved_field16 + report.bychannel_operating_fee ) ";
                $time_fields = $this->getTimeFields($time_line, '0 - report.byorder_reserved_field16 + report.bychannel_operating_fee');
            } else {
                $fields['count_total'] = "SUM ((0 -  report.byorder_reserved_field16 )* ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                $time_fields = $this->getTimeFields($time_line, '  (0 - report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
            }
        } else if ($datas['time_target'] == 'operate_fee_rate') {  //运营费用占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['count_total'] = "SUM( 0 - report.byorder_reserved_field16 + report.bychannel_operating_fee ) /nullif(SUM(report.byorder_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field16 + report.bychannel_operating_fee', 'report.byorder_sales_quota');
            } else {
                $fields['count_total'] = "SUM( 0 - report.byorder_reserved_field16 + report.bychannel_operating_fee ) /nullif(SUM(report.report_sales_quota),0)";
                $time_fields = $this->getTimeFields($time_line, '(0 - report.byorder_reserved_field16 + report.bychannel_operating_fee)', 'report.report_sales_quota');
            }
        } else if ($datas['time_target'] == 'other_vat_fee') {//VAT
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                    $time_fields = $this->getTimeFields($time_line, '0-report.byorder_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                    $time_fields = $this->getTimeFields($time_line, '0-report.report_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.report_reserved_field17 )* ({:RATE} / COALESCE(rates.rate ,1))');
                }
            }

        } else if ($datas['time_target'] == 'other_other_fee') { //其他
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_loan_payment + report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
            }
        } else if ($datas['time_target'] == 'other_review_enrollment_fee') { //早期评论者计划
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee)";
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_review_enrollment_fee');
            } else {
                $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))  )";
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
            }
        } else if ($datas['time_target'] == 'cost_profit_profit') {  //毛利润
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) + COALESCE(report.byorder_purchasing_cost,0) + COALESCE(report.byorder_logistics_head_course,0)) ELSE (report.byorder_goods_profit+ report.byorder_purchasing_cost + report.byorder_logistics_head_course  END )';
                        $time_fields           = $this->getTimeFields($time_line, ' (CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course) ELSE (report.byorder_goods_profit+ report.byorder_purchasing_cost + report.byorder_logistics_head_course  END )');
                    } else {
                        $fields['count_total'] = 'SUM(  CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.first_purchasing_cost,0) + COALESCE(report.first_logistics_head_course,0)  +COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0)) ELSE (report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit) END  )';
                        $time_fields           = $this->getTimeFields($time_line, ' (  CASE WHEN report.goods_operation_pattern = 2 THEN (report.first_purchasing_cost + report.first_logistics_head_course  +report.byorder_channel_profit + report.bychannel_channel_profit) ELSE (report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit) END  )');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $purchasing_logistics = "COALESCE(report.byorder_purchasing_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                    } else {
                        $purchasing_logistics = "COALESCE(report.first_purchasing_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                    }
                    $fields_tmp            = "CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics ) ELSE (report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics ) END";
                    $fields['count_total'] = "SUM( $fields_tmp )";
                    $time_fields           = $this->getTimeFields($time_line, "( $fields_tmp )");

                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $purchasing_logistics = "report.report_purchasing_cost  + report.report_logistics_head_course";
                    } else {
                        $purchasing_logistics = "report.first_purchasing_cost  + report.first_logistics_head_course";
                    }
                    $fields_tmp = "CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_profit + report.bychannel_channel_profit + $purchasing_logistics) ELSE (report.report_goods_profit + $purchasing_logistics ) END";

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $purchasing_logistics = "report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                    } else {
                        $purchasing_logistics = "report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";

                    }
                    $fields_tmp = "CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))  + $purchasing_logistics) ELSE ( report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics) END";

                }
                $fields['count_total'] = "SUM( $fields_tmp )";
                $time_fields = $this->getTimeFields($time_line, "( $fields_tmp )");
            }
        } else if ($datas['time_target'] == 'cost_profit_profit_rate') {  //毛利率
            if ($datas['sale_datas_origin'] == 1) {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit))/ nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit', 'report.byorder_sales_quota');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course)) / nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.byorder_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( ( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit  ))/ nullif(SUM (report.byorder_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit', 'report.byorder_sales_quota');
                    }
                }
            } else {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit ))/ nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit + report.byorder_channel_profit + report.bychannel_channel_profit', 'report.report_sales_quota');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['count_total'] = '(SUM(report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit +  report.report_purchasing_cost + report.report_logistics_head_course)) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course', 'report.report_sales_quota');
                    } else {
                        $fields['count_total'] = '(SUM( ( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit )) / nullif(SUM (report.report_sales_quota),0)';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit + report.report_channel_profit + report.bychannel_channel_profit', 'report.report_sales_quota');
                    }
                }
            }
        }else{

            $fields_tmp = $this->getTimeField($datas,$time_line,3);
            $fields['count_total']  = $fields_tmp['count_total'];
            $time_fields            = $fields_tmp['time_fields'];

        }

        $fields[$datas['time_target']] = $fields['count_total'] ;
        if(!empty($time_fields) && is_array($time_fields)){
            foreach($time_fields as $kt=>$time_field){
                $fields[$kt] = $time_field ;
            }
        }
        //$fields = array_merge($fields, $time_fields);
        return $fields;
    }

    public function getYnWhere($max_ym = '' , $min_ym = ''){
        $ym_array = array() ;
        if(!empty($max_ym) && !empty($min_ym)){
            while($max_ym != $min_ym){
                $year = intval(substr($min_ym ,0 ,4)) ;
                $month = intval(substr($min_ym ,4 ,2));
                $ym_array[] = $year.$month ;
                if($month == 12){
                    $year = $year + 1 ;
                    $month = '01' ;
                }else{
                    $month = $month + 1 ;
                    if($month <10){
                        $month = '0'.$month ;
                    }else{
                        $month = "{$month}" ;
                    }
                }
                $min_ym = $year.$month ;
            }
            $year = intval(substr($max_ym ,0 ,4)) ;
            $month = intval(substr($max_ym ,4 ,2));
            $ym_array[] = $year.$month ;

        }
        if(empty($ym_array)){
            return ' 1=1 ' ;
        }else{
            $ym_str = "'".implode("','" , $ym_array)."'" ;
            if(count($ym_array) == 1){
                $ym_where = "report.ym = " . $ym_str ;
            }else{
                $ym_where =  "report.ym IN (" . $ym_str . " )" ;
            }
            return $ym_where ;
        }

    }
}
