<?php

namespace App\Model;

class AmazonGoodsFinanceReportByOrderPrestoModel extends AbstractPrestoModel
{
    const SEARCH_TYPE_PRESTO = 0;

    const SEARCH_TYPE_ES = 1;

    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "ods.ods_dataark_f_amazon_goods_finance_report_by_order_{$this->dbhost}";
    }

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
    protected function getListByGoods(
        $where = '',
        $datas = [],
        $limit = '',
        $sort = '',
        $order = '',
        $count_tip = 0,
        $channel_arr = [],
        $currencyInfo = [],
        $exchangeCode = '1',
        array $timeLine = [],
        array $deparmentData = [],
        int $searchType = self::SEARCH_TYPE_PRESTO
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

        if(($datas['count_periods'] == 0 || $datas['count_periods'] == 1) && $datas['cost_count_type'] != 2){ //按天或无统计周期
            if ($searchType === self::SEARCH_TYPE_ES) {
                $table = "f_dw_goods_day_report_{$this->dbhost} AS report" ;
            } else {
                $table = "dwd.dwd_dataark_f_dw_goods_day_report_{$this->dbhost} AS report" ;
            }
        }else if($datas['count_periods'] == 2 && $datas['cost_count_type'] != 2){  //按周
            $table = "dwd.dwd_dataark_f_dw_goods_week_report_{$this->dbhost} AS report" ;
        }else if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
            $table = "dwd.dwd_dataark_f_dw_goods_month_report_{$this->dbhost} AS report" ;
        }else if($datas['cost_count_type'] == 2 ){
            $table = "dwd.dwd_dataark_f_dw_goods_month_report_{$this->dbhost} AS report" ;
        }else{
            return [];
        }

        $mod_where = "report.user_id_mod = '" . ($datas['user_id'] % 20) . "'";
        if (!empty($mod_where)) {
            $where .= ' AND ' . $mod_where;
        }

        if ($datas['currency_code'] != 'ORIGIN') {
            if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN ods.ods_dataark_b_site_rate as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id  ";
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
            $table.= " LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_rel_{$this->dbhost} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id and  tags_rel.status = 1 LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_{$this->dbhost} AS gtags ON gtags.id = tags_rel.tags_id AND gtags.status = 1" ;
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
        }

        if (!empty($where_detail)) {
            if (!empty($where_detail['transport_mode'])) {
                if(!is_array($where_detail['transport_mode'])){
                    $transport_modes = explode(',' , $where_detail['transport_mode']) ;
                }else{
                    $transport_modes = $where_detail['transport_mode'] ;
                }
                if(count($transport_modes) == 1){
                    $where .= ' AND report."goods_Transport_mode" = ' . ($transport_modes[0] == 'FBM' ? 1 : 2);
                }
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
                    $table .= " LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_rel_{$this->dbhost} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_{$this->dbhost} AS gtags ON gtags.id = tags_rel.tags_id";
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

            $target_wheres = $where_detail['target'];
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
            $where = 'g.user_id = ' . $lists[0]['user_id']   ;
            if (!empty($channel_arr)){
                if (count($channel_arr)==1){
                    $where .= " AND g.channel_id = ".intval(implode(",",$channel_arr));
                }else{
                    $where .= " AND g.channel_id IN (".implode(",",$channel_arr).")";
                }
            }
            $table = "ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost} as g " ;
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
                $table .= "  LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_rel_{$this->dbhost} AS rel ON g.g_amazon_goods_id = rel.goods_id ";
            }else if($datas['count_dimension'] == 'head_id') { //负责人
                $fba_fields = $group = 'i.head_id ,g.fba_inventory_v3_id' ;
                $table .= "  LEFT JOIN ods.ods_dataark_f_amazon_goods_isku_{$this->dbhost} AS i ON g.isku_id = i.id  ";
            }else if($datas['count_dimension'] == 'developer_id') { //开发人员
                $fba_fields = $group = 'i.developer_id ,g.fba_inventory_v3_id' ;
                $table .= "  LEFT JOIN ods.ods_dataark_f_amazon_goods_isku_{$this->dbhost} AS i ON g.isku_id = i.id  ";
            }

            $where_arr = array() ;
            foreach($lists as $list1){
                if($datas['count_dimension'] == 'sku'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('sku' => addslashes($list1['sku']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('sku' => addslashes($list1['sku']));
                    }
                }else if($datas['count_dimension'] == 'asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('asin' => addslashes($list1['asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('asin' => addslashes($list1['asin']));
                    }
                }else if($datas['count_dimension'] == 'parent_asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('parent_asin' => addslashes($list1['parent_asin']), 'channel_id' => $list1['channel_id'], 'site_id' => $list1['site_id']);
                    }else{
                        $where_arr[] = array('parent_asin' => addslashes($list1['parent_asin']));
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
                $where .= ' AND g."Transport_mode" = ' . ($datas['where_detail']['transport_mode'] == 'FBM' ? 1 : 2);
            }
            if ($datas['where_detail']['is_care'] && !empty(trim($datas['where_detail']['is_care']))){
                $where .= ' AND g.is_care = ' . (intval($datas['where_detail']['is_care'])==1?1:0);
            }
            if ($datas['where_detail']['tag_id'] && !empty(trim($datas['where_detail']['tag_id']))){
                if ($datas['count_dimension'] != 'tags'){
                    $table .= "  LEFT JOIN ods.ods_dataark_g_amazon_goods_tags_rel_{$this->dbhost} AS rel ON g.g_amazon_goods_id = rel.goods_id ";
                }
                $where .=' AND rel.tags_id IN (' .  trim($datas['where_detail']['tag_id']) . ' ) ';
            }
            if ($datas['where_detail']['operators_id'] && !empty(trim($datas['where_detail']['operators_id']))){

                $table .= "  LEFT JOIN ods.ods_dataark_b_channel AS c ON g.channel_id = c.id  ";

                $where .=' AND (g.operation_user_admin_id IN (' .  trim($datas['where_detail']['operators_id']) . ' ) OR c.operation_user_admin_id IN (' .  trim($datas['where_detail']['operators_id']) . ' ) )';
            }

        }

        $fba_fields .= ' , SUM(DISTINCT(CASE WHEN g.fulfillable_quantity < 0 THEN 0 ELSE g.fulfillable_quantity END )) as fba_sales_stock ,MAX(DISTINCT( CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as  fba_sales_day , MAX(DISTINCT(g.available_days) ) as max_fba_sales_day , MIN( DISTINCT(g.available_days) ) as min_fba_sales_day , MIN(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END ))  as min_egt0_fba_sales_day , MAX(DISTINCT(CASE WHEN g.available_days < 0 THEN 0 ELSE g.available_days END )) as max_egt0_fba_sales_day , SUM(DISTINCT(CASE WHEN g.reserved_quantity < 0 THEN 0 ELSE g.reserved_quantity END )) as fba_reserve_stock  , SUM(DISTINCT( CASE WHEN g.replenishment_quantity < 0 THEN 0 ELSE g.replenishment_quantity END ))  as fba_recommended_replenishment , MAX( DISTINCT(g.replenishment_quantity) ) as max_fba_recommended_replenishment ,MIN( DISTINCT(g.replenishment_quantity) ) as min_fba_recommended_replenishment , SUM(DISTINCT( CASE WHEN g.available_stock < 0 THEN 0 ELSE g.available_stock END )) as fba_special_purpose , MAX( DISTINCT(g.available_stock)) as  max_fba_special_purpose , MIN(DISTINCT( g.available_stock) )  as min_fba_special_purpose ';

        $goods_finance_md = new AmazonGoodsFinancePrestoModel($this->dbhost, $this->codeno);
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

        if (in_array('goods_views_rate', $targets)) { //页面浏览次数百分比 (需要计算)
            //总流量次数
            $table = "dwd.dwd_dataark_f_dw_goods_day_report_{$this->dbhost} AS report  LEFT JOIN ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost} AS goods ON report.amazon_goods_id = goods.id ";
            $where =$datas['origin_where'] .  " AND report.user_id_mod = '" . ($datas['user_id'] % 20) . "'";
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){

                $total_views_numbers = $this->select($where." AND byorder_number_of_visits>0", 'report.channel_id,SUM(report.byorder_number_of_visits) as total_views_number', $table,'','',"report.channel_id");
                if (!empty($total_views_numbers)){
                    $case = " CASE ";
                    foreach ($total_views_numbers as $total_views_numbers_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_views_numbers_list['channel_id']." THEN SUM ( report.byorder_number_of_visits ) / round( " . $total_views_numbers_list['total_views_number'].",2) ";
                    }
                    $case .= "ELSE 0 END";
                    $fields['goods_views_rate'] = $case ;
                }else{
                    $fields['goods_views_rate'] = 0 ;
                }
            }else{
                $total_views_numbers = $this->get_one($where, 'SUM(report.byorder_number_of_visits) as total_views_number', $table);
                if (intval($total_views_numbers['total_views_number']) > 0) {
                    $fields['goods_views_rate'] = " SUM ( report.byorder_number_of_visits ) / round(" . intval($total_views_numbers['total_views_number']) .' , 2)';
                }else{
                    $fields['goods_views_rate'] = 0 ;
                }
            }
        }
        if (in_array('goods_buyer_visit_rate', $targets)) { //买家访问次数百分比 （需要计算）
            $table = "dwd.dwd_dataark_f_dw_goods_day_report_{$this->dbhost} AS report LEFT JOIN ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost} AS goods ON report.amazon_goods_id = goods.id ";
            $where =$datas['origin_where'] .  " AND report.user_id_mod = '" . ($datas['user_id'] % 20) . "'";

            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                $total_user_sessions = $this->select($where." AND byorder_user_sessions>0", 'report.channel_id,SUM(report.byorder_user_sessions) as total_user_sessions', $table,'','',"report.channel_id");
                if (!empty($total_user_sessions)){
                    $case = " CASE ";
                    foreach ($total_user_sessions as $total_user_sessions_list){
                        $case .=  " WHEN max(report.channel_id) = " . $total_user_sessions_list['channel_id']." THEN SUM ( report.byorder_user_sessions ) / round(" . $total_user_sessions_list['total_user_sessions'].",2)";
                    }
                    $case .= " ELSE 0 END";
                    $fields['goods_buyer_visit_rate'] =  $case  ;
                }else{
                    $fields['goods_buyer_visit_rate'] = 0 ;
                }
            }else{
                $total_user_sessions = $this->get_one($where, 'SUM(report.byorder_user_sessions) as total_user_sessions', $table);
                if (intval($total_user_sessions['total_user_sessions']) > 0) {
                    $fields['goods_buyer_visit_rate'] = " SUM ( report.byorder_user_sessions ) / round(" . intval($total_user_sessions['total_user_sessions']).',2)';
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
                    $fields['sale_refund'] = "SUM ( (0 - report.byorder_refund) / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
            $fields['sale_refund_rate'] = $fields['sale_return_goods_number'] . " * 1.0 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " SUM ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
                        $fields['purchase_logistics_logistics_cost'] = " SUM (  (report.first_logistics_head_course) ) ";
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
                    $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit / COALESCE(rates.rate ,1) * {:RATE})' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit)' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit / COALESCE(rates.rate ,1) * {:RATE})' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
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
                    $fields['amazon_fee'] = 'SUM (report.byorder_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM (report.report_goods_amazon_fee)';
                } else {
                    $fields['amazon_fee'] = 'SUM (report.report_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            }
        }

        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.byorder_platform_sales_commission + report.byorder_reserved_field21) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( (report.byorder_platform_sales_commission + report.byorder_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "SUM ( report.report_platform_sales_commission + report.report_reserved_field21 ) ";
                } else {
                    $fields['amazon_sales_commission'] = "SUM ( (report.report_platform_sales_commission + report.report_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
                    $fields['amazon_other_fee'] = "SUM(report.byorder_goods_amazon_other_fee)";
                } else {
                    $fields['amazon_other_fee'] = "SUM(report.byorder_goods_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "SUM(report.report_goods_amazon_other_fee)";
                } else {
                    $fields['amazon_other_fee'] = "SUM(report.report_goods_amazon_other_fee / COALESCE(rates.rate ,1) * {:RATE} )";
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
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.byorder_estimated_monthly_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.byorder_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.report_estimated_monthly_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "SUM ( report.report_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }
        }
        if (in_array('amazon_long_term_storage_fee', $targets)) { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.byorder_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee)';
                } else {
                    $fields['amazon_long_term_storage_fee'] = 'SUM(report.report_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
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
                $fields['operate_fee'] = "SUM ( (0 -  report.byorder_reserved_field16) / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
            $fields['evaluation_fee_rate'] = '(' . $fields['evaluation_fee'] . ") / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
            } else {
                $fields['cpc_cost'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
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
                $fields['cpc_turnover'] = 'SUM ( report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  )';
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
                $fields['cpc_direct_sales_quota'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  )';
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
                $fields['cpc_indirect_sales_quota'] = 'SUM (report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  )';
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
                if ($datas['currency_code'] == 'ORIGIN') {
                    if($datas['cost_count_type'] == '1'){
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.byorder_purchasing_cost +  report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                    }else{
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                    }
                    if($datas['cost_count_type'] == '1'){
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( (0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.byorder_purchasing_cost + report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                    }else{
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( (0 - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if($datas['cost_count_type'] == '1'){
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.report_purchasing_cost +  report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                    }else{
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( 0 - report.byorder_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                    }
                    if($datas['cost_count_type'] == '1'){
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( (0 - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.report_purchasing_cost + report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                    }else{
                        $fields['cost_profit_total_pay'] = $fields['amazon_fee'] . "+" . "SUM ( (0 - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                    }
                }
            }

        }

        if (in_array('cost_profit_total_income', $targets) ) {   //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "SUM ( report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                }
            }

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = $fields['cost_profit_total_income'] . " + SUM(report.byorder_refund_promote_discount)";
                } else {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] . " + SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] ." + SUM(report.report_refund_promote_discount)";
                } else {
                    $fields['cost_profit_total_income'] =  $fields['cost_profit_total_income'] ." + SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                }
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
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['goods_price_min'] = 'min(report.goods_price)';
                $fields['goods_price_max'] = 'max(report.goods_price)';
            } else {
                $fields['goods_price_min'] = 'min(report.goods_price/ COALESCE(rates.rate ,1) * {:RATE})';
                $fields['goods_price_max'] = 'max(report.goods_price/ COALESCE(rates.rate ,1) * {:RATE})';

            }

            $fields['min_transport_mode'] = ' min(report."goods_Transport_mode") ' ;
            $fields['max_transport_mode'] = ' max(report."goods_Transport_mode") ' ;
        }

        if ($datas['count_dimension'] == 'parent_asin') {
            $fields['parent_asin'] = "max(report.goods_parent_asin)";
            $fields['image'] = 'max(report.goods_image)';
            $fields['title'] = 'max(report.goods_title)';
            if($datas['is_distinct_channel'] == '1'){
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
            }
            if($_REQUEST['is_bi_request']  == 1){
                $fields['goods_is_care']                 = 'max(report.goods_is_care)';
                $fields['goods_is_new']                  = 'max(report.goods_is_new)';
                $fields['up_status']                  = 'max(report.goods_up_status)';
            }
        }else if ($datas['count_dimension'] == 'asin') {
            $fields['asin'] = "max(report.goods_asin)";
            $fields['image'] = 'max(report.goods_image)';
            $fields['title'] = 'max(report.goods_title)';
            if($datas['is_distinct_channel'] == '1'){
                $fields['parent_asin'] = "max(report.goods_parent_asin)";
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';

            }
            if($_REQUEST['is_bi_request'] == 1){
                $fields['goods_is_care']                 = 'max(report.goods_is_care)';
                $fields['goods_is_new']                  = 'max(report.goods_is_new)';
                $fields['up_status']                  = 'max(report.goods_up_status)';
            }
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
                $fields['goods_is_new']                  = 'max(report.goods_is_new)';
                $fields['up_status']                  = 'max(report.goods_up_status)';
                $fields['goods_g_amazon_goods_id']       = 'max(report.goods_g_amazon_goods_id)';
                $fields['isku_id']                       = 'max(report.goods_isku_id)';

                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';

                $fields['class1'] = 'max(report.goods_product_category_name_1)';
                $fields['group'] = 'max(report.goods_group_name)';
                $fields['operators'] = 'max(report.goods_operation_user_admin_name)';
                $fields['goods_operation_user_admin_id'] = 'max(report.goods_operation_user_admin_id)';
            }
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
        }

        return $fields;
    }

    //按商品维度,时间展示字段（新增统计维度完成）
    private function getGoodsTimeFields($datas = [], $time_line)
    {
        $fields = [];
        $fields = $this->getGoodsTheSameFields($datas,$fields);

        $time_fields = [];
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
            //总流量次数
            $table = "dwd.dwd_dataark_f_dw_goods_day_report_{$this->dbhost} AS report LEFT JOIN ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost} AS goods ON report.amazon_goods_id = goods.id ";
            $where =$datas['origin_where'] .  " AND report.user_id_mod = '" . ($datas['user_id'] % 20) . "'";


            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){

                $total_views_numbers = $this->select($where." AND byorder_number_of_visits>0", 'report.channel_id,SUM(report.byorder_number_of_visits) as total_views_number', $table,'','',"report.channel_id");
                if (!empty($total_views_numbers)){
                    $case = "CASE ";
                    foreach ($total_views_numbers as $total_views_numbers_list){
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
                $total_views_numbers = $this->get_one($where, 'SUM(report.byorder_number_of_visits) as total_views_number', $table);
                if (intval($total_views_numbers['total_views_number']) > 0) {
                    $fields['count_total'] = "SUM ( report.byorder_number_of_visits ) / round(" . intval($total_views_numbers['total_views_number']) .',2)';
                    $time_fields = $this->getTimeFields($time_line, "  report.byorder_number_of_visits  / round(" . intval($total_views_numbers['total_views_number']).',2)');
                } else {
                    $fields['count_total'] = 0;
                    $time_fields = $this->getTimeFields($time_line, 0);
                }
            }
        } else if ($datas['time_target'] == 'goods_buyer_visit_rate') { //买家访问次数百分比 （需要计算）
            $table = "dwd.dwd_dataark_f_dw_goods_day_report_{$this->dbhost} AS report LEFT JOIN ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost} AS goods ON report.amazon_goods_id = goods.id ";
            $where =$datas['origin_where'] .  " AND report.user_id_mod = '" . ($datas['user_id'] % 20) . "'";


            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){

                $total_user_sessions = $this->select($where." AND byorder_user_sessions>0", 'report.channel_id,SUM(report.byorder_user_sessions) as total_user_sessions', $table,'','',"report.channel_id");
                if (!empty($total_user_sessions)){
                    $case = "CASE ";
                    foreach ($total_user_sessions as $total_user_sessions_list){
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
                $total_user_sessions = $this->get_one($where, 'SUM(report.byorder_user_sessions) as total_user_sessions', $table);
                if (intval($total_user_sessions['total_user_sessions']) > 0) {
                    $fields['count_total'] = " SUM ( report.byorder_user_sessions ) / round(" . intval($total_user_sessions['total_user_sessions']) .",2)";
                    $time_fields = $this->getTimeFields($time_line, " report.byorder_user_sessions  / round(" . intval($total_user_sessions['total_user_sessions']).",2)");
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
                    $fields['count_total'] = "SUM ( report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota / COALESCE(rates.rate ,1) * {:RATE}");
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_sales_quota )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota");
                } else {
                    $fields['count_total'] = "SUM ( report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota / COALESCE(rates.rate ,1) * {:RATE}");
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
                    $fields['count_total'] = "SUM ( ( 0 - report.byorder_refund) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund ) / COALESCE(rates.rate ,1) * {:RATE} ");
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_refund )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund ");
                } else {
                    $fields['count_total'] = "SUM ( report.report_refund / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund / COALESCE(rates.rate ,1) * {:RATE} ");
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
                    $fields['count_total'] = "SUM(report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
                }
            }
        } else if ($datas['time_target'] == 'promote_refund_discount') {  //退款返还promote折扣
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} ");
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount");
                } else {
                    $fields['count_total'] = "SUM(report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_promote_discount /  COALESCE(rates.rate ,1) * {:RATE}");
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
                        $fields['count_total'] = 'SUM(report.byorder_goods_profit / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})';
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    } else {
                        $fields['count_total'] = 'SUM(  report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_goods_profit / COALESCE(rates.rate ,1) * {:RATE} )';
                        $time_fields = $this->getTimeFields($time_line, ' report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_goods_profit / COALESCE(rates.rate ,1) * {:RATE}');
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
                        $fields['count_total'] = 'SUM(report.report_goods_profit / COALESCE(rates.rate ,1) * {:RATE} + report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE})';
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit / COALESCE(rates.rate ,1) * {:RATE} + report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    } else {
                        $fields['count_total'] = 'SUM(  ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) + report.report_goods_profit / COALESCE(rates.rate ,1) * {:RATE} )';
                        $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} + report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) + report.report_goods_profit / COALESCE(rates.rate ,1) * {:RATE} ');
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
                    $fields['count_total'] = 'SUM(report.byorder_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_goods_amazon_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_sales_commission') {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_platform_sales_commission + report.byorder_reserved_field21 ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission + report.byorder_reserved_field21)');
                } else {
                    $fields['count_total'] = "SUM ( (report.byorder_platform_sales_commission+report.byorder_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission+report.byorder_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_platform_sales_commission +report.report_reserved_field21  ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission +report.report_reserved_field21)');
                } else {
                    $fields['count_total'] = "SUM ( (report.report_platform_sales_commission +report.report_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission+report.report_reserved_field21) / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_delivery_fee') {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} -report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.byorder_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} -report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.report_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} - report.report_profit / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_generation_delivery_cost / COALESCE(rates.rate ,1) * {:RATE}+ report.report_fbaperorderfulfillmentfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbaweightbasedfee / COALESCE(rates.rate ,1) * {:RATE} - report.report_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_profit ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_profit');
                } else {
                    $fields['count_total'] = "SUM ( report.report_profit / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_profit / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_settlement_fee') {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fixedclosingfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_refund_variableclosingfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_other_fee') {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_goods_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_goods_amazon_other_fee  / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee  / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_goods_amazon_other_fee) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_goods_amazon_other_fee  / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee  / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_shipping_fee') {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_returnshipping / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_returnshipping )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_returnshipping ');
                } else {
                    $fields['count_total'] = "SUM ( report.report_returnshipping / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_returnshipping / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } else if ($datas['time_target'] == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_refund_deducted_commission') {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission');
                } else {
                    $fields['count_total'] = "SUM ( report.report_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnperorderfee / COALESCE(rates.rate ,1) * {:RATE} + report.report_fbacustomerreturnweightbasedfee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if ($datas['time_target'] == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.byorder_estimated_monthly_storage_fee )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.byorder_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_estimated_monthly_storage_fee )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_estimated_monthly_storage_fee');
                } else {
                    $fields['count_total'] = "SUM ( report.report_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE} )";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }
        } elseif ($datas['time_target'] == 'amazon_long_term_storage_fee') { //FBA长期仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.byorder_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE}');
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.report_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} )';
                    $time_fields = $this->getTimeFields($time_line, 'report.report_long_term_storage_fee  / COALESCE(rates.rate ,1) * {:RATE} ');
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
                        $fields['count_total'] = " SUM ( report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_purchasing_cost');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (  (report.first_purchasing_cost) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE} ) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost / COALESCE(rates.rate ,1) * {:RATE}) ');
                }
            }

        } else if ($datas['time_target'] == 'purchase_logistics_logistics_cost') {  // 物流/头程
            if ($datas['cost_count_type'] == '1') {
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course');
                    } else {
                        $fields['count_total'] = " SUM ( report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " SUM (( report.first_logistics_head_course) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' (report.first_logistics_head_course)');
                } else {
                    $fields['count_total'] = " SUM ( (report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} ) ) ";
                    $time_fields = $this->getTimeFields($time_line, '(report.first_logistics_head_course / COALESCE(rates.rate ,1) * {:RATE} )');
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
                $fields['count_total'] = "SUM ((0 -  report.byorder_reserved_field16 )/ COALESCE(rates.rate ,1) * {:RATE} ) ";
                $time_fields = $this->getTimeFields($time_line, '  (0 - report.byorder_reserved_field16) / COALESCE(rates.rate ,1) * {:RATE} ');
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
                    $fields['count_total'] = "SUM ( report.byorder_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10');
                } else {
                    $fields['count_total'] = "SUM ( report.report_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.report_reserved_field10 / COALESCE(rates.rate ,1) * {:RATE} ');
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
        } else if ($datas['time_target'] == 'cpc_cost') {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost ');
            } else {
                $fields['count_total'] = " SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} ) ";
                $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} ');
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
                $fields['count_total'] = 'SUM ( report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} + report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} ');
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
                $fields['count_total'] = 'SUM ( report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE} ) / nullif(SUM ( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ),0) ';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost / COALESCE(rates.rate ,1) * {:RATE} + report.byorder_cpc_sd_cost / COALESCE(rates.rate ,1) * {:RATE}', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
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
                $fields['count_total'] = 'SUM ( report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} ');
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
                $fields['count_total'] = 'SUM (report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE} - report."byorder_sd_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE}  - report."byorder_sp_attributedSales7dSameSKU" / COALESCE(rates.rate ,1) * {:RATE} ');
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
                    $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                    $time_fields = $this->getTimeFields($time_line, '0-report.report_reserved_field17');
                } else {
                    $fields['count_total'] = "SUM((0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, '(0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE}');
                }
            }
        } else if($datas['time_target'] =='cost_profit_total_pay') { //总支出
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if($datas['cost_count_type'] == '1'){
                        $fields['count_total'] = "SUM (report.byorder_goods_amazon_fee  - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.byorder_purchasing_cost +  report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee  - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.byorder_purchasing_cost +  report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17');
                    }else{
                        $fields['count_total'] =  "SUM ( report.byorder_goods_amazon_fee - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17)" ;
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17');
                    }
                }else{
                    if($datas['cost_count_type'] == '1'){
                        $fields['count_total'] =  "SUM ( (report.byorder_goods_amazon_fee  - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.byorder_purchasing_cost + report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                        $time_fields = $this->getTimeFields($time_line, '(report.byorder_goods_amazon_fee  - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.byorder_purchasing_cost + report.byorder_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} ');
                    }else{
                        $fields['count_total'] =  "SUM ( (report.byorder_goods_amazon_fee - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                        $time_fields = $this->getTimeFields($time_line, '(report.byorder_goods_amazon_fee - report.byorder_refund + report.byorder_promote_discount + report.byorder_cpc_cost + report.byorder_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.byorder_reserved_field10 - report.byorder_reserved_field16 -report.byorder_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE}');
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if($datas['cost_count_type'] == '1'){
                        $fields['count_total'] =  "SUM ( report.report_goods_amazon_fee  - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.report_purchasing_cost +  report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                        $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee  - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.report_purchasing_cost +  report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17');
                    }else{
                        $fields['count_total'] = "SUM ( report.report_goods_amazon_fee  - report.byorder_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17)" ;
                        $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee  - report.byorder_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost +  report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17');
                    }
                }else{
                    if($datas['cost_count_type'] == '1'){
                        $fields['count_total'] =  "SUM ( (report.report_goods_amazon_fee   - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.report_purchasing_cost + report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                        $time_fields = $this->getTimeFields($time_line, '(report.report_goods_amazon_fee   - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.report_purchasing_cost + report.report_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE}');
                    }else{
                        $fields['count_total'] =  "SUM ( (report.report_goods_amazon_fee - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} )" ;
                        $time_fields = $this->getTimeFields($time_line, '(report.report_goods_amazon_fee - report.report_refund + report.report_promote_discount + report.report_cpc_cost + report.report_cpc_sd_cost + report.first_purchasing_cost + report.first_logistics_head_course + report.report_reserved_field10 - report.report_reserved_field16 -report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                }
            }
        }else if($datas['time_target'] =='cost_profit_total_income') { //总收入

            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.byorder_sales_quota + report.byorder_refund_promote_discount)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_sales_quota + report.byorder_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.byorder_sales_quota + report.byorder_refund_promote_discount ) / COALESCE(rates.rate ,1) * {:RATE} )";
                        $time_fields = $this->getTimeFields($time_line, ' (report.byorder_sales_quota + report.byorder_refund_promote_discount ) / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.report_sales_quota + report.byorder_refund_promote_discount )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_sales_quota + report.byorder_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.report_sales_quota + report.byorder_refund_promote_discount) / COALESCE(rates.rate ,1) * {:RATE} )";
                        $time_fields = $this->getTimeFields($time_line, '(report.report_sales_quota + report.byorder_refund_promote_discount) / COALESCE(rates.rate ,1) * {:RATE}  ');
                    }
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.byorder_sales_quota + report.report_refund_promote_discount)";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_sales_quota + report.report_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.byorder_sales_quota + report.report_refund_promote_discount ) / COALESCE(rates.rate ,1) * {:RATE} )";
                        $time_fields = $this->getTimeFields($time_line, '  (report.byorder_sales_quota + report.report_refund_promote_discount ) / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM ( report.report_sales_quota + report.report_refund_promote_discount )";
                        $time_fields = $this->getTimeFields($time_line, '  report.report_sales_quota + report.report_refund_promote_discount ');
                    } else {
                        $fields['count_total'] = "SUM ( (report.report_sales_quota + report.report_refund_promote_discount) / COALESCE(rates.rate ,1) * {:RATE} )";
                        $time_fields = $this->getTimeFields($time_line, '   (report.report_sales_quota + report.report_refund_promote_discount) / COALESCE(rates.rate ,1) * {:RATE} ');
                    }
                }
            }
        }else{
            $fields_tmp = $this->getTimeField($datas,$time_line);
            $fields['count_total']  = $fields_tmp['count_total'];
            $time_fields            = $fields_tmp['time_fields'];
        }


        $fields[$datas['time_target']] = $fields['count_total'] ;
        if(!empty($time_fields)){
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
                        $fields['count_total'] = "SUM(({$by_order_fields}) / COALESCE(rates.rate ,1) * {:RATE})";
                        $time_fields = $this->getTimeFields($timeLine, "({$by_order_fields}) / COALESCE(rates.rate ,1) * {:RATE}");
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($report_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $report_fields);
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                        $time_fields = $this->getTimeFields($timeLine, "($report_fields) / COALESCE(rates.rate ,1) * {:RATE}");
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
                            $fields['count_total'] = " SUM ( {$by_order_fields} / COALESCE(rates.rate ,1) * {:RATE} ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$by_order_fields} / COALESCE(rates.rate ,1) * {:RATE}");
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " SUM ( {$report_fields} ) ";
                            $time_fields = $this->getTimeFields($timeLine, $report_fields);
                        } else {
                            $fields['count_total'] = " SUM ( {$report_fields} / COALESCE(rates.rate ,1) * {:RATE} ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$report_fields} / COALESCE(rates.rate ,1) * {:RATE}");
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " SUM (( {$first_fields}) ) ";
                        $time_fields = $this->getTimeFields($timeLine, " ({$first_fields})");
                    } else {
                        $fields['count_total'] = " SUM ( ({$first_fields} / COALESCE(rates.rate ,1) * {:RATE} ) ) ";
                        $time_fields = $this->getTimeFields($timeLine, "({$first_fields} / COALESCE(rates.rate ,1) * {:RATE} )");
                    }
                }
                break;
            case 6://销售数据源,且包含货币
                if($datas['sale_datas_origin'] == 1){
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($by_order_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                    } else {
                        $fields['count_total'] = "SUM(({$by_order_fields}) / COALESCE(rates.rate ,1) * {:RATE})";
                        $time_fields = $this->getTimeFields($timeLine, "({$by_order_fields}) / COALESCE(rates.rate ,1) * {:RATE}");
                    }
                }else{
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM($report_fields)";
                        $time_fields = $this->getTimeFields($timeLine, $report_fields);
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) / COALESCE(rates.rate ,1) * {:RATE})";
                        $time_fields = $this->getTimeFields($timeLine, "($report_fields) / COALESCE(rates.rate ,1) * {:RATE}");
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
     * @desc 获取不是时间类型的字段
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

        if ($type == 2 or $type == 3){//店铺和运营人员才有的
            if (in_array('channel_fbm_safe_t_claim_demage', $targets)) { //SAF-T
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage)";
                } else {
                    $fields['channel_fbm_safe_t_claim_demage'] = "SUM(report.channel_fbm_safe_t_claim_demage / COALESCE(rates.rate ,1) * {:RATE})";
                }
            }
        }
    }
}
