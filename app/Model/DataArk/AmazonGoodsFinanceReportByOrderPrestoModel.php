<?php

namespace App\Model\DataArk;

use App\Model\ChannelTargetsMySQLModel;
use App\Model\SiteRateMySQLModel;
use App\Model\UserAdminModel;
use App\Model\AbstractPrestoModel;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use App\Service\CommonService;
use Hyperf\Di\Annotation\Inject;
use function App\getUserInfo;
use Hyperf\Utils\Exception\ParallelExecutionException;
use Hyperf\Utils\Parallel;

class AmazonGoodsFinanceReportByOrderPrestoModel extends AbstractPrestoModel
{
    /**
     * @Inject()
     * @var CommonService
     */
    protected $commonService;

    protected $table = 'table_amazon_goods_finance_report_by_order';

    //按指标展现时 自定义算法和指标
    protected $customTargetsList;
    //按时间展现时 筛选指标
    protected $timeCustomTarget;
    //自定义指标是否有店铺维度
    protected $countDimensionChannel = false;

    //商品维度 分摊 f_monthly_profit_report_by_sku_001字段
    protected $default_goods_split_fields = [
        'goods_promote_coupon' => 'reserved_field39', //coupon费用
        'goods_other_review_enrollment_fee' => 'reserved_field38', //早期评论者计划费用
        'goods_promote_run_lightning_deal_fee' => 'reserved_field37', //RunLightningDealFee费用
    ];

    //变量参数组
    protected $ark_custom_params = [
        ":DAY" => '天数'
    ];

    //fba字段
    protected $fba_fields_arr = [
        //商品
        'fba_sales_stock',
        'fba_sales_day',
        'fba_reserve_stock',
        'fba_recommended_replenishment',
        'fba_special_purpose',
        //店铺
        'fba_goods_value',
        'fba_stock',
        'fba_sales_volume',
        'fba_need_replenish',
        'fba_predundancy_number',
        //erp字段
        'ark_erp_purchasing_num',
        'ark_erp_send_num',
        'ark_erp_good_num',
        'ark_erp_bad_num',
        'ark_erp_lock_num',
        'ark_erp_goods_cost_total'
    ];

    protected  $amzon_site = array(
        1 => array("currency_code" => "USD", "currency_symbol" => "$", "code" => "US"),
        2 => array("currency_code" => "CAD", "currency_symbol" => "C$", "code" => "CA"),
        3 => array("currency_code" => "MXN", "currency_symbol" => "Mex$", "code" => "MX"),
        4 => array("currency_code" => "EUR", "currency_symbol" => "€", "code" => "DE"),
        5 => array("currency_code" => "EUR", "currency_symbol" => "€", "code" => "ES"),
        6 => array("currency_code" => "EUR", "currency_symbol" => "€", "code" => "FR"),
        7 => array("currency_code" => "INR", "currency_symbol" => "₹", "code" => "IN"),
        8 => array("currency_code" => "EUR", "currency_symbol" => "€", "code" => "IT"),
        9 => array("currency_code" => "GBP", "currency_symbol" => "£", "code" => "UK"),
        10 => array("currency_code" => "CNY", "currency_symbol" => "￥", "code" => "CN"),
        11 => array("currency_code" => "JPY", "currency_symbol" => "¥", "code" => "JP"),
        12 => array("currency_code" => "AUD", "currency_symbol" => "A$", "code" => "AU"),
        13 => array("currency_code" => "BRL", "currency_symbol" => "R$", "code" => "BR"),
        14 => array("currency_code" => "TRY", "currency_symbol" => "₺", "code" => "TR"),
        15 => array("currency_code" => "AED", "currency_symbol" => "AED", "code" => "AE"),
        16 => array("currency_code" => "EUR", "currency_symbol" => "€", "code" => "NL") ,
        17 => array("currency_code" => "SAR", "currency_symbol" => "SAR", "code" => "SA"),
        18 => array("currency_code" => "SGD", "currency_symbol" => "S$", "code" => "SG")
    );

    /**
     * 获取商品维度统计列表(新增统计维度完成)
     * @param string $where
     * @param array $datas
     * @param string $limitgoods
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
        array $rateInfo = [],
        int $day_param = 1
    ) {
        $isMysql = $this->getIsMysql($datas);
        $datas['is_month_table'] = 0;
        if(($datas['count_periods'] == 0 || $datas['count_periods'] == 1) && $datas['cost_count_type'] != 2){ //按天或无统计周期
            $table = "{$this->table_goods_day_report}" ;
        }else if($datas['count_periods'] == 2 && $datas['cost_count_type'] != 2){  //按周
            $table = "{$this->table_goods_week_report}" ;
        }else if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
            $table = "{$this->table_goods_month_report}" ;
            $datas['is_month_table'] = 1;
        }else if($datas['cost_count_type'] == 2 ){
            $table = "{$this->table_goods_month_report}" ;
            $datas['is_month_table'] = 1;
        }else{
            return [];
        }

        //没有按周期统计 ， 按指标展示
        if ($datas['show_type'] == 2) {
            $fields_arr = $this->getGoodsFields($datas);
            $fields = $fields_arr['fields'];
            $fba_target_key = $fields_arr['fba_target_key'];
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
            if ($isMysql){
                $fields_arr[] = $field . " AS '" . $field_name . "'";

            }else{
                $fields_arr[] = $field . ' AS "' . $field_name . '"';

            }
        }

//        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));
        $field_data = str_replace("{:RATE}", $exchangeCode, str_replace("COALESCE(rates.rate ,1)","(COALESCE(rates.rate ,1)*1.000000)", implode(',', $fields_arr)));//去除presto除法把数据只保留4位导致精度异常，如1/0.1288 = 7.7639751... presto=7.7640

        $field_data = str_replace("{:DAY}", $day_param, $field_data);

        $mod_where = "report.user_id_mod = " . ($datas['user_id'] % 20) . " and amazon_goods.goods_user_id_mod=" . ($datas['user_id'] % 20);

        $ym_where = $this->getYnWhere($datas['max_ym'] , $datas['min_ym'] ) ;
        $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;





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
            $table.= " LEFT JOIN {$this->table_amazon_goods_tags_rel} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id AND tags_rel.db_num = '{$this->dbhost}' and  tags_rel.status = 1 LEFT JOIN {$this->table_amazon_goods_tags} AS gtags ON gtags.id = tags_rel.tags_id AND gtags.status = 1 AND gtags.db_num = '{$this->dbhost}' " ;
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
                    $where .= ' AND report.goods_Transport_mode = ' . ($transport_modes[0] == 'FBM' ? 1 : 2);
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
                }elseif ($group_str == 0){
                    $where .= " AND report.goods_group_id = 0 ";
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
                    $table .= " LEFT JOIN {$this->table_amazon_goods_tags_rel} AS tags_rel ON tags_rel.goods_id = report.goods_g_amazon_goods_id AND tags_rel.db_num = '{$this->dbhost}' AND tags_rel.status = 1 LEFT JOIN {$this->table_amazon_goods_tags} AS gtags ON gtags.id = tags_rel.tags_id AND gtags.db_num = '{$this->dbhost}'";

                }
                if(is_array($where_detail['tag_id'])){
                    $tag_str = implode(',', $where_detail['tag_id']);

                }else{
                    $tag_str = $where_detail['tag_id'] ;
                }
                if (!empty($tag_str)) {
                    if (in_array(0,explode(",",$tag_str))){
                        $where .= " AND (tags_rel.tags_id  IN ( " . $tag_str . " )  OR  tags_rel.tags_id IS NULL )  ";

                    }else{
                        $where .= " AND tags_rel.tags_id  IN ( " . $tag_str . " ) ";

                    }
                }elseif ($tag_str == 0){
                    $where .= " AND (tags_rel.tags_id = 0 OR tags_rel.tags_id IS NULL) ";
                }
            }

            if(!empty($where_detail['sku'])){
                if(is_array($where_detail['sku'])){
                    $sku_str="'".join("','",$where_detail['sku'])."'";
                }else{
                    $sku_str = "'".$where_detail['sku']."'" ;
                }

                if (!empty($sku_str)) {
                    $where .= " AND report.goods_sku  IN ( " . $sku_str . ")";
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

        $group = str_replace("{:DAY}", $day_param, $group);
        $where = str_replace("{:DAY}", $day_param, $where);
        $orderby = str_replace("{:DAY}", $day_param, $orderby);
        $limit_num = 0 ;
        if($datas['show_type'] == 2 && $datas['limit_num'] > 0 ){
            $limit_num = $datas['limit_num'] ;
        }
        $count = 0;
        if ($count_tip == 2) { //仅统计总条数
            $count = $this->getTotalNum($where, $table, $group, true,$isMysql);
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        } else if ($count_tip == 1) {  //仅仅统计列表
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields,$exchangeCode,$day_param,$field_data,$table,$where,$group,true,$isMysql);
                }else {
                    $lists = $this->select($where, $field_data, $table, "", "", "", true,null,300,$isMysql);
                }
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group,true);
                if($datas['show_type'] == 2 && ( !empty($fields['fba_sales_stock']) || !empty($fields['fba_sales_day']) || !empty($fields['fba_reserve_stock']) || !empty($fields['fba_recommended_replenishment']) || !empty($fields['fba_special_purpose']) )){
                    $lists = $this->getGoodsFbaDataTmp($lists , $fields , $datas,$channel_arr) ;
                }
                if($datas['show_type'] == 2 && ( !empty($fields['ark_erp_purchasing_num']) || !empty($fields['ark_erp_send_num']) || !empty($fields['ark_erp_good_num']) || !empty($fields['ark_erp_bad_num']) || !empty($fields['ark_erp_lock_num']) || !empty($fields['ark_erp_goods_cost_total']) )){
                    $lists = $this->getGoodsErpData($lists , $fields , $datas , $rateInfo) ;
                }
                //自定义公式涉及到fba
                if ($datas['show_type'] == 2 && !empty($lists)) {
                    foreach ($lists as $k => $item) {
                        foreach ($item as $key => $value) {
                            if (in_array($key, $fba_target_key)) {
                                $item[$key] = $this->count_custom_formula($value, $item);
                            }
                        }
                        $lists[$k] = $item;
                    }
                }
            }
        } else {  //统计列表和总条数
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields,$exchangeCode,$day_param,$field_data,$table,$where,$group,true,$isMysql);
                }else{
                    $lists = $this->select($where, $field_data, $table,"","","",true,null,300,$isMysql);
                }
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByGoods Total Request', [$this->getLastSql()]);
            }else{
                $parallel = new Parallel();
                $parallel->add(function () use($where, $field_data, $table, $limit, $orderby, $group,$isMysql){
                    $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group,true,null,300,$isMysql);
                    return $lists;
                });
                $parallel->add(function () use($where, $table, $group,$isMysql){
                    $count = $this->getTotalNum($where, $table, $group,true,$isMysql);
                    return $count;
                });

                try{
                    // $results 结果为 [1, 2]
                    $results = $parallel->wait();
                    $lists = $results[0];
                    $count = $results[1];
                } catch(ParallelExecutionException $e){
                    // $e->getResults() 获取协程中的返回值。
                    // $e->getThrowables() 获取协程中出现的异常。
                }
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByGoods Request', [$this->getLastSql()]);
                if($datas['show_type'] == 2 && ( !empty($fields['fba_sales_stock']) || !empty($fields['fba_sales_day']) || !empty($fields['fba_reserve_stock']) || !empty($fields['fba_recommended_replenishment']) || !empty($fields['fba_special_purpose']) )){
                    $lists = $this->getGoodsFbaDataTmp($lists , $fields , $datas,$channel_arr) ;
                }
                if($datas['show_type'] == 2 && ( !empty($fields['ark_erp_purchasing_num']) || !empty($fields['ark_erp_send_num']) || !empty($fields['ark_erp_good_num']) || !empty($fields['ark_erp_bad_num']) || !empty($fields['ark_erp_lock_num']) || !empty($fields['ark_erp_goods_cost_total']) )){
                    $lists = $this->getGoodsErpData($lists , $fields , $datas , $rateInfo) ;
                }
                //自定义公式涉及到fba
                if ($datas['show_type'] == 2 && !empty($lists)) {
                    foreach ($lists as $k => $item) {
                        foreach ($item as $key => $value) {
                            if (in_array($key, $fba_target_key)) {
                                $item[$key] = $this->count_custom_formula($value, $item);
                            }
                        }
                        $lists[$k] = $item;
                    }
                }
            }

            if($limit_num > 0 && $count > $limit_num) {
                $count = $limit_num;
            }

        }
        if(!empty($lists) && $datas['show_type'] == 2 && $datas['limit_num'] > 0 && !empty($order) && !empty($sort) && !empty($fields[$sort]) && !empty($fields[$datas['sort_target']]) && !empty($datas['sort_target']) && !empty($datas['sort_order'])){
            //根据字段对数组$lists进行排列
            $sort_names = array_column($lists,$sort);
            $order2  =  $order == 'desc' ? \SORT_DESC : \SORT_ASC;
            array_multisort($sort_names,$order2,$lists);
        }

        $rt['lists'] = empty($lists) ? array() : $lists;
        $rt['count'] = intval($count);
        return $rt;
    }

    protected function getTotalNum($where = '', $table = '', $group = '', $isJoin = false, $isMysql = false)
    {
        return $this->count($where, $table, $group, '', '', $isJoin, null, 300, $isMysql);
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
        $is_join = true;
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
                $is_join = false;
                $field_data = "max(report.site_id) as site_id";
                break;
            case "channel_id":
                $is_join = false;
                $field_data = "max(report.channel_id) as channel_id";
                break;
            case "department":
                $is_join = false;
                $field_data = "max(dc.user_department_id) as user_department_id";
                break;
            case "admin_id":
                $is_join = false;
                $field_data = "max(uc.admin_id) as admin_id";
                break;
                //运营人员
            case "operators":
                $is_join = false;
                $field_data = "max(report.goods_operation_user_admin_id) as goods_operation_user_admin_id";
                break;

            default:
                return $where;
        }
        $lists = $this->select($where,$field_data , $table, $limit, $orderby, $group,$is_join );
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
            $amazon_fba_inventory_v3_md = new AmazonFbaInventoryV3MySQLModel([], $this->dbhost, $this->codeno);
            $where = "g.user_id = " . intval($lists[0]['user_id']) ." AND g.is_parent=0";
            if (!empty($channel_arr)){
                if (count($channel_arr)==1){
                    $where .= " AND rel.channel_id = ".intval(implode(",",$channel_arr));
                }else{
                    $where .= " AND rel.channel_id IN (".implode(",",$channel_arr).")";
                }
            }
            $table = "g_amazon_fba_inventory_v3_{$this->codeno} as g LEFT JOIN g_amazon_fba_inventory_v3_rel_{$this->codeno} as rel ON g.id = rel.inventory_id " ;
            if($datas['count_dimension'] == 'sku'){
                if($datas['is_distinct_channel'] == 1){
                    $table_fields = 'max(g.seller_sku) as sku , rel.channel_id' ;
                    $table_group = 'g.id  , rel.channel_id' ;
                    $fba_fields = $group = 'sku , channel_id' ;
                }else{
                    $table_fields = 'max(g.seller_sku) as sku , g.id' ;
                    $table_group = ' g.id' ;
                    $fba_fields = $group = 'sku, id' ;
                }
            }else if($datas['count_dimension'] == 'asin'){
                if($datas['is_distinct_channel'] == 1){
                    $table_fields = 'max(g.asin) as asin  , rel.channel_id' ;
                    $table_group = 'g.id , rel.channel_id' ;
                    $fba_fields = $group = 'asin , channel_id' ;
                }else{
                    $table_fields =  'max(g.asin) as asin  , g.id' ;
                    $table_group = ' g.id' ;
                    $fba_fields = $group = 'asin ,id ' ;
                }
            }else if($datas['count_dimension'] == 'parent_asin'){
                if($datas['is_distinct_channel'] == 1){
                    $table_fields =  'max(g.parent_asin) as parent_asin , rel.channel_id' ;
                    $table_group = 'g.id , rel.channel_id' ;
                    $fba_fields = $group = 'parent_asin , channel_id' ;
                }else{
                    $table_fields =  'max(g.parent_asin) as parent_asin ,  g.id' ;
                    $table_group = ' g.id' ;
                    $fba_fields = $group = 'parent_asin ,id ' ;
                }
            }else if($datas['count_dimension'] == 'isku'){
                $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id " ;

                $table_fields =  'max(ext.isku_id) as isku_id , g.id' ;
                $table_group = ' g.id' ;
                $fba_fields = $group = 'isku_id ,id' ;
            }else if($datas['count_dimension'] == 'class1'){
                //分类暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'group'){ //分组
                $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id " ;

                $table_fields = 'max(ext.group_id) as group_id , g.id' ;
                $table_group = ' g.id' ;
                $fba_fields = $group = 'group_id , id' ;

            }else if($datas['count_dimension'] == 'tags'){ //标签（需要刷数据）
                $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id LEFT JOIN g_amazon_goods_tags_rel_{$this->codeno} as tags_rel ON tags_rel.goods_id = ext.amazon_goods_id " ;

                $table_fields =  'tags_rel.tags_id ,g.id' ;
                $table_group = 'tags_rel.tags_id , g.id' ;
                $fba_fields = $group = 'tags_id , id' ;


            }else if($datas['count_dimension'] == 'head_id') { //负责人
                //负责人暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'developer_id') { //开发人员
                //开发人员暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'all_goods'){
                if($datas['is_distinct_channel'] == 1) { //有区分店铺
                    $table_fields =  'rel.channel_id' ;
                    $table_group = 'g.id , rel.channel_id' ;
                    $fba_fields = $group = 'channel_id' ;
                }else{
                    $table_fields =  'g.id' ;
                    $table_group = 'g.id' ;
                    $fba_fields = $group = 'id' ;
                }
            }else if($datas['count_dimension'] == 'goods_channel'){
                $table_fields = 'rel.channel_id' ;
                $table_group = 'g.id , rel.channel_id' ;
                $fba_fields = $group = 'channel_id' ;
            }


            $where_arr = array() ;
            foreach($lists as $list1){
                if($datas['count_dimension'] == 'sku'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('sku' => self::escape($list1['sku']), 'channel_id' => $list1['channel_id']);
                    }else{
                        $where_arr[] = array('sku' => self::escape($list1['sku']));
                    }
                }else if($datas['count_dimension'] == 'asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('asin' => self::escape($list1['asin']), 'channel_id' => $list1['channel_id']);
                    }else{
                        $where_arr[] = array('asin' => self::escape($list1['asin']));
                    }
                }else if($datas['count_dimension'] == 'parent_asin'){
                    if($datas['is_distinct_channel'] == 1) {
                        $where_arr[] = array('parent_asin' => self::escape($list1['parent_asin']), 'channel_id' => $list1['channel_id']);
                    }else{
                        $where_arr[] = array('parent_asin' => self::escape($list1['parent_asin']));
                    }
                }else if($datas['count_dimension'] == 'class1'){
                    //分类暂时没有 ，因为需要跨库查询
                }else if($datas['count_dimension'] == 'group'){
                    $where_arr[] = array('group_id'=>$list1['group_id']) ;
                }else if($datas['count_dimension'] == 'tags'){  //标签
                    $where_arr[] = array('tags_id'=>$list1['tags_id']) ;
                }else if($datas['count_dimension'] == 'head_id'){  //负责人
                    //负责人暂时没有 ，因为需要跨库查询
                }else if($datas['count_dimension'] == 'developer_id'){ //开发人
                    //开发人暂时没有 ，因为需要跨库查询
                }else if($datas['count_dimension'] == 'isku'){ //开发人
                    $where_arr[] = array('isku_id'=>$list1['isku_id']) ;
                }else {
                    $where_arr[] = array('channel_id'=>$list1['channel_id']) ;
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
                        if($datas['count_dimension'] == 'sku'){
                            $where_strs[] = '( rel.channel_id = ' . $cid . ' AND g.seller_sku ' . ' IN (' . $str . '))' ;
                        }else{
                            $where_strs[] = '( rel.channel_id = ' . $cid . ' AND g.'.$datas['count_dimension'] . ' IN (' . $str . '))' ;
                        }

                    }
                    $where_str = !empty($where_strs) ? "(".implode(' OR ' , $where_strs).")" : "";

                }else{
                    $where_strs = array_unique(array_column($where_arr , $datas['count_dimension'])) ;
                    $str = "'" . implode("','" , $where_strs) . "'" ;
                    if($datas['count_dimension'] == 'sku') {
                        $where_str = 'g.seller_sku' . ' IN (' . $str . ') ';
                    }else{
                        $where_str = 'g.' . $datas['count_dimension'] . ' IN (' . $str . ') ';
                    }
                }
            }else if($datas['count_dimension'] == 'class1'){
                //分类暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'group'){
                $where_strs = array_unique(array_column($where_arr , 'group_id')) ;
                $where_str = 'ext.group_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'tags'){ //标签
                $where_strs = array_unique(array_column($where_arr , 'tags_id')) ;
                $where_str = 'tags_rel.tags_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else if($datas['count_dimension'] == 'head_id'){
                //负责人暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'developer_id'){
                //开发人员暂时没有 ，因为需要跨库查询
            }else if($datas['count_dimension'] == 'isku'){
                $where_strs = array_unique(array_column($where_arr , 'isku_id')) ;
                $where_str = 'ext.isku_id IN (' . implode(',' , $where_strs) . ' ) ';
            }else{
                $where_strs = array_unique(array_column($where_arr , 'channel_id')) ;
                $where_str = 'rel.channel_id IN (' . implode(',' , $where_strs) . ' ) ';
            }
        }
        $where_str = !empty($where_str) ? $where_str . " AND " : "";
        $where.= ' AND ' . $where_str." g.id > 0 " ;
        if(isset($datas['where_detail']) && $datas['where_detail']){
            $is_rel_status = false;
            if (!is_array($datas['where_detail'])){
                $datas['where_detail'] = json_decode($datas['where_detail'],true);
            }
            if (!empty($datas['where_detail']['group_id']) && !empty(trim($datas['where_detail']['group_id']))){
                if($datas['count_dimension'] != 'group' && $datas['count_dimension'] != 'tags' && $datas['count_dimension'] != 'isku'){
                    $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id " ;
                    $is_rel_status = true;
                }
                $where .= ' AND ext.group_id IN (' . $datas['where_detail']['group_id'] . ') ' ;
            }
            /*if (!empty($datas['where_detail']['transport_mode']) && !empty(trim($datas['where_detail']['transport_mode']))){
                $where .= ' AND g.Transport_mode = ' . ($datas['where_detail']['transport_mode'] == 'FBM' ? 1 : 2);
            } //FBA 信息 Transport_mode 必为 2   */
            if (!empty($datas['where_detail']['is_care']) && !empty(trim($datas['where_detail']['is_care']))){
                if($datas['count_dimension'] != 'group' && $datas['count_dimension'] != 'tags' && $datas['count_dimension'] != 'isku'){
                    if(!$is_rel_status){
                        $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id " ;
                        $is_rel_status = true;
                    }
                }
                $where .= ' AND ext.is_care = ' . (intval($datas['where_detail']['is_care'])==1?1:0);
            }
            if (!empty($datas['where_detail']['tag_id']) && !empty(trim($datas['where_detail']['tag_id']))){
                if ($datas['count_dimension'] != 'tags'){
                    if($datas['count_dimension'] == 'group' || $datas['count_dimension'] == 'isku'){
                        $table.= " LEFT JOIN g_amazon_goods_tags_rel_{$this->codeno} as tags_rel ON tags_rel.goods_id = ext.amazon_goods_id " ;
                    }else{
                        if(!$is_rel_status){
                            $table.= " LEFT JOIN g_amazon_goods_ext_{$this->codeno} as ext ON ext.amazon_goods_id = rel.amazon_goods_id " ;
                        }
                        $table.= " LEFT JOIN g_amazon_goods_tags_rel_{$this->codeno} as tags_rel ON tags_rel.goods_id = ext.amazon_goods_id ";
                    }
                }
                $where .=' AND tags_rel.tags_id IN (' .  trim($datas['where_detail']['tag_id']) . ' ) ';
            }
        }
        $table_fields = !empty($table_fields) ? $table_fields . " , " : "";
        $table_fields.= ' g.fulfillable_quantity, g.available_days  ,g.reserved_quantity , g.replenishment_quantity , g.available_stock ' ;


        $fba_fields = !empty($fba_fields) ? $fba_fields . " , " : "";
        $fba_fields .= ' SUM((CASE WHEN fulfillable_quantity < 0 THEN 0 ELSE fulfillable_quantity END )) as fba_sales_stock ,MAX(( CASE WHEN available_days < 0 THEN 0 ELSE available_days END )) as  fba_sales_day , MAX(available_days) as max_fba_sales_day , MIN(available_days) as min_fba_sales_day , MIN((CASE WHEN available_days < 0 THEN 0 ELSE available_days END ))  as min_egt0_fba_sales_day , MAX(CASE WHEN available_days < 0 THEN 0 ELSE available_days END ) as max_egt0_fba_sales_day , SUM((CASE WHEN reserved_quantity < 0 THEN 0 ELSE reserved_quantity END )) as fba_reserve_stock  , SUM(( CASE WHEN replenishment_quantity < 0 THEN 0 ELSE replenishment_quantity END ))  as fba_recommended_replenishment , MAX(replenishment_quantity) as max_fba_recommended_replenishment ,MIN((replenishment_quantity)) as min_fba_recommended_replenishment , SUM(( CASE WHEN available_stock < 0 THEN 0 ELSE available_stock END )) as fba_special_purpose , MAX(available_stock) as  max_fba_special_purpose , MIN((available_stock) )  as min_fba_special_purpose ';

        $table_group = !empty($table_group) ? " GROUP BY {$table_group}" : "";
        $table_tmp = " (SELECT {$table_fields} FROM {$table} WHERE {$where} {$table_group} ) as tmp  " ;
        $group = !empty($group) ? $group : "";
        $fbaDatas = array() ;
        $fbaData = $amazon_fba_inventory_v3_md->select('' , $fba_fields, $table_tmp ,'','',$group) ;
        $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
        $logger->info('getGoodsFbaDataTmp Mysql:', [ $amazon_fba_inventory_v3_md->getLastSql()]);

        if (!empty($fbaData)){
            foreach($fbaData as $fba){
                if($datas['count_dimension'] == 'sku'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'sku',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'asin'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'asin',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'parent_asin'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'parent_asin',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'class1'){

                }else if($datas['count_dimension'] == 'group'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'group_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'tags'){  //标签（需要刷数据）
                    $fbaDatas = $this->handleGoodsFbaData($fba,'tags_id',$datas['is_distinct_channel'],$fbaDatas);
                }else if($datas['count_dimension'] == 'head_id'){

                }else if($datas['count_dimension'] == 'developer_id'){

                }else if($datas['count_dimension'] == 'isku'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'isku_id',$datas['is_distinct_channel'],$fbaDatas);
                }elseif($datas['count_dimension'] == 'all_goods'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'channel_id',$datas['is_distinct_channel'],$fbaDatas);
                }elseif($datas['count_dimension'] == 'goods_channel'){
                    $fbaDatas = $this->handleGoodsFbaData($fba,'channel_id',$datas['is_distinct_channel'],$fbaDatas);
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

    protected function getGoodsErpData($lists = [], $fields = [], $datas = [],$rate_info = []){
        if (empty($lists)){
            return $lists;
        }
        if ($datas['show_type'] != 2){
            return $lists;
        }
        if (!in_array($datas['count_dimension'], ['sku', 'isku'])){
            return $lists;
        }
        if($datas['is_distinct_channel'] == 0 && $datas['count_dimension'] == 'sku'){
            return $lists;
        }
        if (empty($fields['ark_erp_purchasing_num']) && empty($fields['ark_erp_send_num']) && empty($fields['ark_erp_good_num']) && empty($fields['ark_erp_bad_num']) && empty($fields['ark_erp_lock_num']) && empty($fields['ark_erp_goods_cost_total'])){
            return $lists;
        }

        $iskuIds = array_unique(array_column($lists, 'isku_id'));
        $erpWhere = "user_id = {$lists[0]['user_id']} AND isku_id IN(".implode(',', $iskuIds).") AND is_delete = 0";

        $selectFields = "user_id, isku_id";
        if (!empty($fields['ark_erp_purchasing_num'])){ //采购在途
            $selectFields .= ", SUM(purchasing_num) as ark_erp_purchasing_num";
        }
        if (!empty($fields['ark_erp_send_num'])){ //调拨在途
            $selectFields .= ", SUM(send_num) as ark_erp_send_num";
        }
        if (!empty($fields['ark_erp_good_num'])){ //库存良品量
            $selectFields .= ", SUM(good_num) as ark_erp_good_num";
        }
        if (!empty($fields['ark_erp_bad_num'])){ //库存次品量
            $selectFields .= ", SUM(bad_num) as ark_erp_bad_num";
        }
        if (!empty($fields['ark_erp_lock_num'])){ //库存锁仓量
            $selectFields .= ", SUM(lock_num) + SUM(lock_num_work_order) as ark_erp_lock_num";
        }
        if (!empty($fields['ark_erp_goods_cost_total'])){ //ERP在库总成本
            //币种是人民币
            $selectFields .= ", SUM(goods_cost * total_num) as ark_erp_goods_cost_total";
        }

        $erpIskuModel = new ErpStorageWarehouseIskuMySQLModel([], $this->dbhost, $this->codeno);
        $erpIskuList = $erpIskuModel->select($erpWhere, $selectFields, "", "", "", "isku_id");
        $mapIskuList = [];
        if (!empty($erpIskuList)){
            foreach ($erpIskuList as $val){
                $mapIskuList[$val['isku_id']] = $val;
            }
        }

        foreach ($lists as $key => $val){
            if (!empty($fields['ark_erp_purchasing_num'])){
                $val['ark_erp_purchasing_num'] = isset($mapIskuList[$val['isku_id']]['ark_erp_purchasing_num']) ? $mapIskuList[$val['isku_id']]['ark_erp_purchasing_num'] : null;
            }
            if (!empty($fields['ark_erp_send_num'])){
                $val['ark_erp_send_num'] = isset($mapIskuList[$val['isku_id']]['ark_erp_send_num']) ? $mapIskuList[$val['isku_id']]['ark_erp_send_num'] : null;
            }
            if (!empty($fields['ark_erp_good_num'])){
                $val['ark_erp_good_num'] = isset($mapIskuList[$val['isku_id']]['ark_erp_good_num']) ? $mapIskuList[$val['isku_id']]['ark_erp_good_num'] : null;
            }
            if (!empty($fields['ark_erp_bad_num'])){
                $val['ark_erp_bad_num'] = isset($mapIskuList[$val['isku_id']]['ark_erp_bad_num']) ? $mapIskuList[$val['isku_id']]['ark_erp_bad_num'] : null;
            }
            if (!empty($fields['ark_erp_lock_num'])){
                $val['ark_erp_lock_num'] = isset($mapIskuList[$val['isku_id']]['ark_erp_lock_num']) ? $mapIskuList[$val['isku_id']]['ark_erp_lock_num'] : null;
            }
            if (!empty($fields['ark_erp_goods_cost_total'])){
                //币种是人民币
                $to_currency_code = $datas['currency_code'] != 'ORIGIN' ? $datas['currency_code'] : $this->amzon_site[$val['site_id']]['currency_code'];
                $val['ark_erp_goods_cost_total'] = isset($mapIskuList[$val['isku_id']]['ark_erp_goods_cost_total']) ? $this->commonService->currencyExchange($mapIskuList[$val['isku_id']]['ark_erp_goods_cost_total'],'CNY',$to_currency_code,$rate_info) : null;
            }
            $lists[$key] = $val;
        }

        return $lists;
    }

    protected function handleGoodsFbaData($fba, $field, $is_distinct_channel = 0, $fbaDatas = array())
    {
        if(empty($fba[$field])){
            return $fbaDatas ;
        }
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
        $targets_temp = $targets;//基础指标缓存

        //自定义指标
        $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
        $target_key_str = trim("'" . implode("','",explode(",",$datas['target'])) . "'");
        $custom_targets_list = $datas_ark_custom_target_md->getList("user_id = {$datas['user_id']} AND target_type in(1, 2) AND count_dimension IN (1,2) AND target_key IN ({$target_key_str})");
        //自定义公式里包含新增指标
        $custom_targets_list = $this->addNewTargets($datas_ark_custom_target_md,$datas['user_id'],$custom_targets_list);
        $targets = $this->addCustomTargets($targets,$custom_targets_list);

        //是否计算总支出(查询总支出、毛利润、毛利率时需要计算总支出)--总支出=亚马逊费用 + 退款 + promote折扣 + cpc_sp_cost + cpc_sd_cost + 商品成本 + 物流 + 测评费用 + 运营费用 + VAT
        $isCalTotalPay=in_array('cost_profit_total_pay', $targets) || in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets);

        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }
        if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
            $fields['goods_conversion_rate'] = 'sum( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)';
        }
        if (in_array('goods_rank', $targets)) { //大类目rank
            $fields['goods_rank'] = "min(nullif(report.goods_rank,0))";
        }
        if (in_array('goods_min_rank', $targets)) { //小类目rank
            $fields['goods_min_rank'] = " min(nullif(report.goods_min_rank,0))";
        }
        if (in_array('goods_views_number', $targets)) { //页面浏览次数
            $fields['goods_views_number'] = " sum( report.byorder_number_of_visits ) ";
        }

        if(in_array('goods_views_rate', $targets) || in_array('goods_buyer_visit_rate', $targets)){
            $table = "{$this->table_goods_day_report} ";
            $ym_where = $this->getYnWhere($datas['max_ym'],$datas['min_ym']);
            $where  = $ym_where . " AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
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
                        $case .=  " WHEN max(report.channel_id) = " . $total_views_numbers_list['channel_id']." THEN sum( report.byorder_number_of_visits ) * 1.0000 / round( " . $total_views_numbers_list['total_views_number'].",2) ";
                    }
                    $case .= "ELSE 0 END";
                    $fields['goods_views_rate'] = $case ;
                }else{
                    $fields['goods_views_rate'] = 0 ;
                }
            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_views_number']) > 0) {
                    $fields['goods_views_rate'] = " sum( report.byorder_number_of_visits ) * 1.0000 / round(" . intval($total_views_session_numbers['total_views_number']) .' , 2)';
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
                        $case .=  " WHEN max(report.channel_id) = " . $total_user_sessions_list['channel_id']." THEN sum( report.byorder_user_sessions ) * 1.0000 / round(" . $total_user_sessions_list['total_user_sessions'].",2)";
                    }
                    $case .= " ELSE 0 END";
                    $fields['goods_buyer_visit_rate'] =  $case  ;
                }else{
                    $fields['goods_buyer_visit_rate'] = 0 ;
                }
            }else{
                if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_user_sessions']) > 0) {
                    $fields['goods_buyer_visit_rate'] = " sum( report.byorder_user_sessions ) * 1.0000 / round(" . intval($total_views_session_numbers['total_user_sessions']).',2)';
                }else{
                    $fields['goods_buyer_visit_rate'] =0 ;
                }
            }
        }
        if (in_array('goods_buybox_rate', $targets)) { //购买按钮赢得率
            $fields['goods_buybox_rate'] = " (sum( byorder_buy_button_winning_num ) * 1.0000 / nullif(sum( report.byorder_number_of_visits ) ,0) ) ";
        }
        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_volume'] = " sum( report.byorder_sales_volume +  report.byorder_group_id ) ";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_volume'] = " sum( report.report_sales_volume +  report.report_group_id ) ";
            }
        }
        if (in_array('sale_many_channel_sales_volume', $targets)) { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.byorder_group_id )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.report_group_id )";
            }
        }
        if (in_array('sale_sales_quota', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('amazon_fee_rate', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('operate_fee_rate', $targets) || in_array('evaluation_fee_rate', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_turnover_rate', $targets)) {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('sale_return_goods_number', $targets) || in_array('sale_refund_rate', $targets)) {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['sale_return_goods_number'] = "sum(report.byorder_refund_num )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_return_goods_number'] = "sum(report.report_refund_num )";
            }
        }
        if (in_array('sale_refund', $targets) || $isCalTotalPay) {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.byorder_refund )";
                } else {
                    $fields['sale_refund'] = "sum( (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "sum( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = $fields['sale_return_goods_number'] . " * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('promote_discount', $targets) || $isCalTotalPay) {  //promote折扣
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
        if (in_array('promote_refund_discount', $targets) || $isCalTotalPay) {  //退款返还promote折扣
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

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)
            || $isCalTotalPay) {  //采购成本
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.first_purchasing_cost ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum(report.first_purchasing_cost ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    }
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)
            || $isCalTotalPay) {  // 物流/头程
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum(  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum(  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }
                }
            }
        }

        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets) || $isCalTotalPay) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'sum(report.byorder_goods_amazon_fee)';
                } else {
                    $fields['amazon_fee'] = 'sum(report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = "sum(report.report_goods_amazon_fee {$estimated_monthly_storage_fee_field})";
                } else {
                    $fields['amazon_fee'] = 'sum((report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) )';
                }
            }
        }

        if (in_array('amazon_sales_commission', $targets)) {  //亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "sum( report.byorder_platform_sales_commission + report.byorder_reserved_field21) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( (report.byorder_platform_sales_commission + report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "sum( report.report_platform_sales_commission + report.report_reserved_field21 ) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( (report.report_platform_sales_commission + report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.byorder_estimated_monthly_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( {$estimated_monthly_storage_fee_field} )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( {$estimated_monthly_storage_fee_field} * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
            $fields['amazon_fee_rate'] = '(' . $fields['amazon_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = '(' . $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets) || $isCalTotalPay) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "sum( 0- report.byorder_reserved_field16 ) ";
            } else {
                $fields['operate_fee'] = "sum( (0 -  report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = '(' . $fields['operate_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets) || $isCalTotalPay) {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = '(' . $fields['evaluation_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('cpc_sp_cost', $targets) || $isCalTotalPay) {  //CPC_SP花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sp_cost'] = " sum( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets) || $isCalTotalPay) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " sum( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " sum( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }

        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
            } else {
                $fields['cpc_cost'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = '(' . $fields['cpc_cost'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 )";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks )";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = '('.$fields['cpc_click_number'].')' . " * 1.0000 / nullif( " . $fields['cpc_exposure'] . " , 0 ) ";
        }
        if (in_array('cpc_order_number', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
            if ($datas['sale_datas_origin'] == '1') {
                $fields['cpc_order_rate'] = '(' . $fields['cpc_order_number'] . ") * 1.0000 / nullif( SUM(report.byorder_sales_volume+report.byorder_group_id ) , 0 )  ";
            }else{
                $fields['cpc_order_rate'] = '(' . $fields['cpc_order_number'] . ") * 1.0000 / nullif( SUM(report.report_sales_volume +report.report_group_id  ) , 0 ) ";
            }

        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = '('.$fields['cpc_order_number'] . ") * 1.0000 / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  )';
            } else {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = '(' . $fields['cpc_turnover'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = '('.$fields['cpc_cost'] . ") * 1.0000 / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = '('.$fields['cpc_cost'] . ") * 1.0000 / nullif( " . $fields['cpc_turnover'] . " , 0 ) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = '(' . $fields['cpc_direct_sales_volume'] . ") * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU"  )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_sales_quota', $targets)) {  //CPC销售额=CPC直接销售额+CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sales_quota'] = 'sum( report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d")';
            } else {
                $fields['cpc_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}  + report."byorder_sp_attributedSales7d" / COALESCE(rates.rate ,1) * {:RATE}) ';
            }
        }

        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = '(' . $fields['cpc_indirect_sales_volume'] . ") * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('other_vat_fee', $targets) || $isCalTotalPay) { //VAT
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

        //费用分摊
        $goodsSplitFields = $this->default_goods_split_fields;

        if (in_array('goods_promote_coupon', $targets))
        {
            $tempField = $goodsSplitFields['goods_promote_coupon'];

            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['goods_promote_coupon'] = "SUM(report.monthly_sku_{$tempField})";
            } else {
                $fields['goods_promote_coupon'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('goods_other_review_enrollment_fee', $targets))
        {
            $tempField = $goodsSplitFields['goods_other_review_enrollment_fee'];

            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['goods_other_review_enrollment_fee'] = "SUM(report.monthly_sku_{$tempField})";
            } else {
                $fields['goods_other_review_enrollment_fee'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
            }
        }

        if (in_array('goods_promote_run_lightning_deal_fee', $targets))
        {
            $tempField = $goodsSplitFields['goods_promote_run_lightning_deal_fee'];

            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['goods_promote_run_lightning_deal_fee'] = "SUM(report.monthly_sku_{$tempField})";
            } else {
                $fields['goods_promote_run_lightning_deal_fee'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
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

        $erp_value = $datas['is_distinct_channel'] == 0 && $datas['count_dimension'] == 'sku' ? 'NULL' : '1';
        if (in_array('ark_erp_purchasing_num', $targets)) { //采购在途
            $fields['ark_erp_purchasing_num'] = $erp_value;
        }
        if (in_array('ark_erp_send_num', $targets)) { //调拨在途
            $fields['ark_erp_send_num'] = $erp_value;
        }
        if (in_array('ark_erp_good_num', $targets)) { //库存良品量
            $fields['ark_erp_good_num'] = $erp_value;
        }
        if (in_array('ark_erp_bad_num', $targets)) { //库存次品量
            $fields['ark_erp_bad_num'] = $erp_value;
        }
        if (in_array('ark_erp_lock_num', $targets)) { //库存锁仓量
            $fields['ark_erp_lock_num'] = $erp_value;
        }
        if (in_array('ark_erp_goods_cost_total', $targets)) { //ERP在库总成本
            $fields['ark_erp_goods_cost_total'] = $erp_value;
        }

        if (in_array('cost_profit_total_income', $targets) || $isCalTotalPay) {   //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "sum( report.byorder_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "sum( report.report_sales_quota )";
                } else {
                    $fields['cost_profit_total_income'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }


        }

        //调整总收入、总支出、毛利润计算方式：
        //总收入=商品销售额+ 退款返还Promote折扣
        //总支出=亚马逊费用 + 退款 + promote折扣 + cpc_sp_cost + cpc_sd_cost + 商品成本 + 物流 + 测评费用 + 运营费用 + VAT
        //毛利润=总收入+总支出（总支出为负值），毛利率=毛利润/总收入
        if (in_array('cost_profit_total_pay', $targets) || $isCalTotalPay) {   //总支出
            $fields['cost_profit_total_pay'] ="{$fields['amazon_fee']}+{$fields['sale_refund']}+{$fields['promote_discount']}+{$fields['cpc_sp_cost']}
                                                +{$fields['cpc_sd_cost']}+{$fields['purchase_logistics_purchase_cost']}+{$fields['purchase_logistics_logistics_cost']}+{$fields['evaluation_fee']}
                                                +{$fields['operate_fee']}+{$fields['other_vat_fee']}+{$fields['promote_refund_discount']}";
        }
//        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {
//            $fields['cost_profit_profit'] = $fields['cost_profit_total_income'] . "+" . $fields['cost_profit_total_pay'];
//            if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
//                $fields['cost_profit_profit_rate'] = '('.$fields['cost_profit_profit'] . ") * 1.0000 / nullif( " . $fields['cost_profit_total_income'] . " ,0) ";
//            }
//        }
        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //毛利润
            //商品利润聚合数据只聚合财务维度 。 如果销售额或退款维度与财务不一致，需要转换修复
            $repair_data = '' ;
            if ($datas['finance_datas_origin'] == '1') {
                if($datas['sale_datas_origin'] == '2'){
                    $repair_data.= " + report.report_sales_quota - report.byorder_sales_quota  " ;
                }
                if($datas['refund_datas_origin'] == '2'){
                    $repair_data.= empty($repair_data) ? "  + report.byorder_refund - report.report_refund " : " + report.byorder_refund - report.report_refund " ;
                }

                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = '(SUM(report.byorder_goods_profit'.$repair_data.')'  . '+'. $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM((report.byorder_goods_profit'.$repair_data.') * ({:RATE} / COALESCE(rates.rate ,1)))' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                }

            } else {
                if($datas['sale_datas_origin'] == '1'){
                    $repair_data.= " + report.byorder_sales_quota - report.report_sales_quota  " ;
                }
                if($datas['refund_datas_origin'] == '1'){
                    $repair_data.= empty($repair_data) ? " + report.report_refund - report.byorder_refund " : " + report.report_refund - report.byorder_refund" ;
                }
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_profit'] = '(SUM(report.report_goods_profit'.$repair_data.$estimated_monthly_storage_fee_field.')+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                } else {
                    $fields['cost_profit_profit'] = '(SUM((report.report_goods_profit'.$repair_data.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)))' . '+' . $fields['purchase_logistics_purchase_cost'] . '+' . $fields['purchase_logistics_logistics_cost'].')';
                }

            }

        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = $fields['cost_profit_profit'] . " /  nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        $this->getUnTimeFields($fields,$datas,$targets);

        //加入自定义指标
        $fba_target_key = [];
        $is_count = !empty($datas['is_count']) ? $datas['is_count'] : 0;
        $this->getCustomTargetFields($fields,$custom_targets_list,$targets,$targets_temp, $datas,$fba_target_key,$is_count);
        return ['fields' => $fields,'fba_target_key' => $fba_target_key];
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

            $fields['min_transport_mode'] = ' min(report.goods_Transport_mode) ' ;
            $fields['max_transport_mode'] = ' max(report.goods_Transport_mode) ' ;
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
                $fields['goods_fnsku']                  = 'max(amazon_goods.goods_fnsku)';
                $fields['channel_id'] = 'max(report.channel_id)';
                $fields['site_id'] = 'max(report.site_id)';
                $fields['class1'] = 'max(report.goods_product_category_name_1)';
                $fields['group'] = 'max(report.goods_group_name)';
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

        if($datas['time_target'] == 'goods_views_rate' || $datas['time_target'] == 'goods_buyer_visit_rate'){
            $table = "{$this->table_goods_day_report} ";
            $ym_where = $this->getYnWhere($datas['max_ym'],$datas['min_ym']);
            $where  = $ym_where . " AND  report.user_id_mod = " . ($datas['user_id'] % 20) ." AND " . $datas['origin_where'];
            if($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1){
                $totals_view_session_lists = $this->select($where." AND byorder_number_of_visits>0", 'report.channel_id,SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table,'','',"report.channel_id");
            }else{
                $total_views_session_numbers = $this->get_one($where, 'SUM(report.byorder_number_of_visits) as total_views_number , SUM(report.byorder_user_sessions) as total_user_sessions', $table);
            }
        }

        $target_key = $datas['time_target'];
        $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
        //自定义算法
        $custom_target = $datas_ark_custom_target_md->get_one("user_id = {$datas['user_id']} AND target_type IN(1,2) AND target_key = '{$target_key}' AND status = 1 AND count_dimension IN (1,2)");
        $keys = [];
        $new_target_keys = [];
        if($custom_target && $custom_target['target_type'] == 2){
            $time_targets = explode(",",$custom_target['formula_fields']);
            //公式所涉及到的新增指标
            $target_key_str = trim("'" . implode("','",$time_targets) . "'");
            $new_target = $datas_ark_custom_target_md->getList("user_id = {$datas['user_id']} AND target_type = 1 AND count_dimension IN (1,2) AND target_key IN ($target_key_str)","target_key,month_goods_field,format_type");
            $keys = array_column($new_target,'target_key');
            $new_target_keys = array_column($new_target,null,'target_key');
        }else{
            $time_targets = array($target_key);
        }
        $time_fields = array();
        $time_fields_arr = array();
        foreach ($time_targets as $time_target) {
            if ($time_target == 'goods_visitors') {  // 买家访问次数
                $fields['count_total'] = "SUM(report.byorder_user_sessions)";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_user_sessions');
            } else if ($time_target == 'goods_conversion_rate') { //订单商品数量转化率
                $fields['count_total'] = 'sum( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
            } else if ($time_target == 'goods_rank') { //大类目rank
                $fields['count_total'] = "min(nullif(report.goods_rank,0))";
                $time_fields = $this->getTimeFields($time_line, 'nullif(report.goods_rank,0)', '', 'MIN');
            } else if ($time_target == 'goods_min_rank') { //小类目rank
                $fields['count_total'] = "min(nullif(report.goods_min_rank,0))";
                $time_fields = $this->getTimeFields($time_line, 'nullif(report.goods_min_rank,0)', '', 'MIN');
            } else if ($time_target == 'goods_views_number') { //页面浏览次数
                $fields['count_total'] = "SUM(report.byorder_number_of_visits)";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_number_of_visits');
            } else if ($time_target == 'goods_views_rate') { //页面浏览次数百分比 (需要计算)

                if ($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1) {
                    if (!empty($totals_view_session_lists)) {
                        $case = "CASE ";
                        foreach ($totals_view_session_lists as $total_views_numbers_list) {
                            $case .= " WHEN max(report.channel_id) = " . $total_views_numbers_list['channel_id'] . " THEN sum( report.byorder_number_of_visits ) / round(" . $total_views_numbers_list['total_views_number'] . ",2) ";
                        }
                        $case .= " ELSE 0 END";
                        $fields['goods_views_rate'] = $case;
                        $time_fields = $this->getTimeFields($time_line, $case);
                    } else {
                        $fields['count_total'] = 0;
                        $time_fields = $this->getTimeFields($time_line, 0);
                    }

                } else {
                    if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_views_number']) > 0) {
                        $fields['count_total'] = "sum( report.byorder_number_of_visits ) / round(" . intval($total_views_session_numbers['total_views_number']) . ',2)';
                        $time_fields = $this->getTimeFields($time_line, "  report.byorder_number_of_visits  / round(" . intval($total_views_session_numbers['total_views_number']) . ',2)');
                    } else {
                        $fields['count_total'] = 0;
                        $time_fields = $this->getTimeFields($time_line, 0);
                    }
                }
            } else if ($time_target == 'goods_buyer_visit_rate') { //买家访问次数百分比 （需要计算）
                if ($datas['is_distinct_channel'] == 1 && ($datas['count_dimension'] == 'sku' or $datas['count_dimension'] == 'asin' or $datas['count_dimension'] == 'parent_asin') && $datas['is_count'] != 1) {
                    if (!empty($totals_view_session_lists)) {
                        $case = "CASE ";
                        foreach ($totals_view_session_lists as $total_user_sessions_list) {
                            $case .= " WHEN max(report.channel_id) = " . $total_user_sessions_list['channel_id'] . " THEN sum( report.byorder_user_sessions ) / round(" . $total_user_sessions_list['total_user_sessions'] . ",2)";
                        }
                        $case .= " ELSE 0 END";

                        $fields['count_total'] = $case;
                        $time_fields = $this->getTimeFields($time_line, $case);
                    } else {
                        $fields['count_total'] = 0;
                        $time_fields = $this->getTimeFields($time_line, 0);
                    }
                } else {
                    if (!empty($total_views_session_numbers) && intval($total_views_session_numbers['total_user_sessions']) > 0) {
                        $fields['count_total'] = " sum( report.byorder_user_sessions ) / round(" . intval($total_views_session_numbers['total_user_sessions']) . ",2)";
                        $time_fields = $this->getTimeFields($time_line, " report.byorder_user_sessions  / round(" . intval($total_views_session_numbers['total_user_sessions']) . ",2)");
                    } else {
                        $fields['count_total'] = 0;
                        $time_fields = $this->getTimeFields($time_line, 0);
                    }
                }
            } else if ($time_target == 'goods_buybox_rate') { //购买按钮赢得率
                $fields['count_total'] = " (sum( byorder_buy_button_winning_num )  * 1.0000 / nullif(sum( report.byorder_number_of_visits ) ,0) ) ";
                $time_fields = $this->getTimeFields($time_line, "byorder_buy_button_winning_num * 1.0000", "report.byorder_number_of_visits");
            } else if ($time_target == 'sale_sales_volume') { //销售量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = " sum( report.byorder_sales_volume  +  report.byorder_group_id) ";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_volume  +  report.byorder_group_id");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = " sum( report.report_sales_volume  +  report.byorder_group_id ) ";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_volume  +  report.report_group_id");
                }
            } else if ($time_target == 'sale_many_channel_sales_volume') { //多渠道数量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "sum( report.byorder_group_id )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_group_id");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = "sum( report.report_group_id )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_group_id");
                }
            } else if ($time_target == 'sale_sales_quota') {  //商品销售额
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_sales_quota )";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_sales_quota )";
                        $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
            } else if ($time_target == 'sale_return_goods_number') {  //退款量
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "sum(report.byorder_refund_num )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum(report.report_refund_num )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_num");
                }
            } else if ($time_target == 'sale_refund') {  //退款
                if ($datas['refund_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( 0 - report.byorder_refund )";
                        $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund )");
                    } else {
                        $fields['count_total'] = "sum( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                } elseif ($datas['refund_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_refund )";
                        $time_fields = $this->getTimeFields($time_line, "report.report_refund ");
                    } else {
                        $fields['count_total'] = "sum( report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, "report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                }
            } else if ($time_target == 'sale_refund_rate') {  //退款率
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "sum(report.byorder_refund_num) * 1.0000 / nullif(SUM(report.byorder_sales_volume + report.byorder_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0000 ", "report.byorder_sales_volume + report.byorder_group_id");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum(report.report_refund_num) * 1.0000 / nullif(SUM(report.report_sales_volume + report.report_group_id),0)";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0000", "report.report_sales_volume + report.report_group_id");
                }
            } else if ($time_target == 'promote_discount') {  //promote折扣
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
            } else if ($time_target == 'promote_refund_discount') {  //退款返还promote折扣
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
            } else if ($time_target == 'cost_profit_profit') {  //毛利润
                //商品利润聚合数据只聚合财务维度 。 如果销售额或退款维度与财务不一致，需要转换修复

                $repair_data = '';
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '2') {
                        $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '2') {
                        $repair_data .= " + report.byorder_refund - report.report_refund ";
                    }

                    if ($datas['currency_code'] == 'ORIGIN') {
                        if ($datas['cost_count_type'] == '1') {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course)';
                                $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course');
                            } else {
                                $fields['count_total'] = "SUM(report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course  {$repair_data} )";
                                $time_fields = $this->getTimeFields($time_line, " report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course  {$repair_data} ");
                            }
                        } else {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(  report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit )';
                                $time_fields = $this->getTimeFields($time_line, ' report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit ');
                            } else {
                                $fields['count_total'] = "SUM(  report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit  {$repair_data} )";
                                $time_fields = $this->getTimeFields($time_line, " report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit  {$repair_data}");
                            }
                        }
                    } else {
                        if ($datas['cost_count_type'] == '1') {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM( (report.byorder_goods_profit  ' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))    )';
                                $time_fields = $this->getTimeFields($time_line, ' (report.byorder_goods_profit  ' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        } else {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(  report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($time_line, ' report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM(  (report.first_purchasing_cost  ' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost ' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        }

                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    $estimated_monthly_storage_fee_field = "";
                    if ($datas['is_month_table'] == 1){
                        $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                    }
                    if ($datas['sale_datas_origin'] == '1') {
                        $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '1') {
                        $repair_data .= " + report.report_refund - report.byorder_refund ";
                    }
                    if ($datas['currency_code'] == 'ORIGIN') {
                        if ($datas['cost_count_type'] == '1') {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$estimated_monthly_storage_fee_field.')';
                                $time_fields = $this->getTimeFields($time_line, ' report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$estimated_monthly_storage_fee_field);
                            } else {
                                $fields['count_total'] = "SUM(report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course  {$repair_data} {$estimated_monthly_storage_fee_field})";
                                $time_fields = $this->getTimeFields($time_line, " report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course  {$repair_data} {$estimated_monthly_storage_fee_field}");
                            }

                        } else {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM( report.first_purchasing_cost + report.first_logistics_head_course +  report.report_goods_profit'.$estimated_monthly_storage_fee_field.')';
                                $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit'.$estimated_monthly_storage_fee_field);
                            } else {
                                $fields['count_total'] = "SUM( report.first_purchasing_cost + report.first_logistics_head_course +  report.report_goods_profit  {$repair_data} {$estimated_monthly_storage_fee_field})";
                                $time_fields = $this->getTimeFields($time_line, "(report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit {$repair_data} {$estimated_monthly_storage_fee_field}");
                            }
                        }
                    } else {
                        if ($datas['cost_count_type'] == '1') {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM((report.report_goods_profit'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($time_line, ' (report.report_goods_profit'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM((report.report_goods_profit  ' . $repair_data .$estimated_monthly_storage_fee_field. ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($time_line, ' (report.report_goods_profit  ' . $repair_data.$estimated_monthly_storage_fee_field . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        } else {
                            if (empty($repair_data)) {
                                $fields['count_total'] = 'SUM(  ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + (report.report_goods_profit'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($time_line, ' ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + (report.report_goods_profit'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) ');
                            } else {
                                $fields['count_total'] = 'SUM(  ( (report.first_purchasing_cost  ' . $repair_data.$estimated_monthly_storage_fee_field . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($time_line, ' ( (report.first_purchasing_cost ' . $repair_data.$estimated_monthly_storage_fee_field . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) + report.report_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ');
                            }

                        }

                    }
                }
            } else if ($time_target == 'cost_profit_profit_rate') {  //毛利率
                $repair_data = '' ;
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['sale_datas_origin'] == 1) {

                    if ($datas['finance_datas_origin'] == '1') {
                        if($datas['sale_datas_origin'] == '2'){
                            $repair_data.= " +report.report_sales_quota - report.byorder_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '2'){
                            $repair_data.= empty($repair_data) ? "  +report.byorder_refund - report.report_refund " : " + report.byorder_refund - report.report_refund " ;
                        }
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data.") {$rate_fields}))  * 1.0000 / nullif(sum(report.byorder_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data . ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( ( (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit'.$repair_data.") {$rate_fields}))  * 1.0000 / nullif(sum(report.byorder_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, '((report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit'.$repair_data . ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        }
                    } elseif ($datas['finance_datas_origin'] == '2') {

                        if($datas['sale_datas_origin'] == '1'){
                            $repair_data.= " +report.byorder_sales_quota - report.report_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '1'){
                            $repair_data.= empty($repair_data) ? "  +report.report_refund - report.byorder_refund " : " + report.report_refund - report.byorder_refund" ;
                        }
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data.$estimated_monthly_storage_fee_field.") {$rate_fields}))  * 1.0000 / nullif(sum(report.byorder_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data .$estimated_monthly_storage_fee_field. ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( (( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit '.$repair_data.$estimated_monthly_storage_fee_field.") {$rate_fields}))  * 1.0000 / nullif(sum(report.byorder_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, ' (( report.first_purchasing_cost + report.first_logistics_head_course ) + report.report_goods_profit'.$repair_data . ") {$rate_fields}{$estimated_monthly_storage_fee_field}", 'report.byorder_sales_quota' . $rate_fields);
                        }
                    }
                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['cost_count_type'] == '1') {
                            if($datas['sale_datas_origin'] == '2'){
                                $repair_data.= " +report.report_sales_quota - report.byorder_sales_quota  " ;
                            }
                            if($datas['refund_datas_origin'] == '2'){
                                $repair_data.= empty($repair_data) ? "  +report.byorder_refund - report.report_refund " : " + report.byorder_refund - report.report_refund " ;
                            }
                            $fields['count_total'] = '(SUM((report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data.") {$rate_fields}))  * 1.0000 / nullif(sum(report.report_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, '( report.byorder_goods_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data . ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( ((report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit '.$repair_data.") {$rate_fields}))  * 1.0000 / nullif(sum(report.report_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, '( (report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_goods_profit'.$repair_data . ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        }
                    } elseif ($datas['finance_datas_origin'] == '2') {
                        if($datas['sale_datas_origin'] == '1'){
                            $repair_data.= " +report.byorder_sales_quota - report.report_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '1'){
                            $repair_data.= empty($repair_data) ? "  +report.report_refund - report.byorder_refund " : " + report.report_refund - report.byorder_refund" ;
                        }
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data.$estimated_monthly_storage_fee_field.") {$rate_fields}))  * 1.0000 / nullif(sum(report.report_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_goods_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data.$estimated_monthly_storage_fee_field . ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( (( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit '.$repair_data.$estimated_monthly_storage_fee_field.") {$rate_fields})) * 1.0000 / nullif(sum(report.report_sales_quota {$rate_fields}),0)";
                            $time_fields = $this->getTimeFields($time_line, ' (( report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_goods_profit'.$repair_data.$estimated_monthly_storage_fee_field . ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        }
                    }
                }
            } else if ($time_target == 'amazon_fee') {  //亚马逊费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(report.byorder_goods_amazon_fee)';
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee');
                    } else {
                        $fields['count_total'] = 'SUM(report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    $estimated_monthly_storage_fee_field = "";
                    if ($datas['is_month_table'] == 1){
                        $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                    }
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.')';
                        $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field);
                    } else {
                        $fields['count_total'] = 'SUM((report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) )';
                        $time_fields = $this->getTimeFields($time_line, '(report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'amazon_sales_commission') {  //亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_platform_sales_commission + report.byorder_reserved_field21 ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission + report.byorder_reserved_field21)');
                    } else {
                        $fields['count_total'] = "sum( (report.byorder_platform_sales_commission+report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.byorder_platform_sales_commission+report.byorder_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_platform_sales_commission +report.report_reserved_field21  ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission +report.report_reserved_field21)');
                    } else {
                        $fields['count_total'] = "sum( (report.report_platform_sales_commission +report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.report_platform_sales_commission+report.report_reserved_field21) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_delivery_fee') {  //FBA代发货费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_profit ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_profit ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_settlement_fee') {  //结算费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_other_fee') {  //其他亚马逊费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_goods_amazon_other_fee) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_goods_amazon_other_fee) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee');
                    } else {
                        $fields['count_total'] = "sum( report.report_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_goods_amazon_other_fee  * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_return_shipping_fee') {  //返还运费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_returnshipping )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_returnshipping )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_refund_deducted_commission') {  //退款扣除佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_estimated_monthly_storage_fee )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( {$estimated_monthly_storage_fee_field} )";
                        $time_fields = $this->getTimeFields($time_line, $estimated_monthly_storage_fee_field);
                    } else {
                        $fields['count_total'] = "sum( {$estimated_monthly_storage_fee_field} * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "{$estimated_monthly_storage_fee_field} * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                }
            } elseif ($time_target == 'amazon_long_term_storage_fee') { //FBA长期仓储费
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
            } else if ($time_target == 'amazon_fee_rate') {  //亚马逊费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "sum( report.byorder_goods_amazon_fee {$rate_fields}) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee ' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "sum( report.report_goods_amazon_fee {$rate_fields} {$estimated_monthly_storage_fee_field} ) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee ' . $rate_fields.$estimated_monthly_storage_fee_field, 'report.byorder_sales_quota' . $rate_fields);
                    }
                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "sum( report.byorder_goods_amazon_fee {$rate_fields} ) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_goods_amazon_fee ' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "sum( report.report_goods_amazon_fee {$rate_fields} {$estimated_monthly_storage_fee_field} ) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_goods_amazon_fee ' . $rate_fields.$estimated_monthly_storage_fee_field, 'report.report_sales_quota' . $rate_fields);
                    }
                }
            } else if ($time_target == 'purchase_logistics_purchase_cost') {  //采购成本
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.byorder_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.report_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(  (report.first_purchasing_cost) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                    }
                }

            } else if ($time_target == 'purchase_logistics_logistics_cost') {  // 物流/头程
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(( report.first_logistics_head_course) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_logistics_head_course)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )');
                    }
                }
            } else if ($time_target == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost) ' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = " sum( report.report_logistics_head_course + report.report_purchasing_cost {$rate_fields} ) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_logistics_head_course + report.report_purchasing_cost)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost) {$rate_fields}) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost) ' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = " sum( (report.report_logistics_head_course + report.report_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_logistics_head_course + report.report_purchasing_cost)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost) {$rate_fields}) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    }
                }
            } else if ($time_target == 'operate_fee') {  //运营费用
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "sum(0 -  report.byorder_reserved_field16 ) ";
                    $time_fields = $this->getTimeFields($time_line, '0 - report.byorder_reserved_field16');
                } else {
                    $fields['count_total'] = "sum((0 -  report.byorder_reserved_field16 )* ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, '  (0 - report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'operate_fee_rate') {  //运营费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM( (0 - report.byorder_reserved_field16) {$rate_fields} )  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, '(0 - report.byorder_reserved_field16)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = "SUM( (0 - report.byorder_reserved_field16) {$rate_fields})  * 1.0000 / nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, '(0 - report.byorder_reserved_field16)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                }
            } else if ($time_target == 'evaluation_fee') {  //测评费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }

            } else if ($time_target == 'evaluation_fee_rate') {  //测评费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10 {$rate_fields})  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10 {$rate_fields})  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    }

                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10 {$rate_fields})  * 1.0000 / nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10 {$rate_fields})  * 1.0000 / nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    }

                }
            } else if ($time_target == 'cpc_sp_cost') {  //CPC SP 花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost  ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))  ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))  ');
                }
            } else if ($time_target == 'cpc_sd_cost') {  //CPC SD 花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_sd_cost  ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_cost');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))  ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))  ');
                }
            } else if ($time_target == 'cpc_cost') {  //CPC花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) ";
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost ');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($time_line, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_cost_rate') {  //CPC花费占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM( (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) {$rate_fields})  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, ' (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) ' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = "SUM( (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) {$rate_fields} )  * 1.0000 / nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, ' (report.byorder_cpc_cost + report.byorder_cpc_sd_cost)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                }
            } else if ($time_target == 'cpc_exposure') {  //CPC曝光量
                $fields['count_total'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 )";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field1 + report.byorder_reserved_field2');
            } else if ($time_target == 'cpc_click_number') {  //CPC点击次数
                $fields['count_total'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks )";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
            } else if ($time_target == 'cpc_click_rate') {  //CPC点击率
                $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks)) * 1.0000 / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2), 0 ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks', 'report.byorder_reserved_field1 + report.byorder_reserved_field2');
            } else if ($time_target == 'cpc_order_number') {  //CPC订单数
                $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) ';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"');
            } else if ($time_target == 'cpc_order_rate') {  //cpc订单占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) * 1.0000 / nullif(sum( report.byorder_sales_volume +report.byorder_group_id  ) ,0) ';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', '(report.byorder_sales_volume + report.byorder_group_id)');
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ) * 1.0000 / nullif(sum( report.report_sales_volume + report.report_group_id ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', '(report.report_sales_volume + report.report_group_id)');
                }
            } else if ($time_target == 'cpc_click_conversion_rate') {  //cpc点击转化率
                $fields['count_total'] = '(sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" ))  * 1.0000 / nullif (SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks) , 0 )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d"', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
            } else if ($time_target == 'cpc_turnover') {  //CPC成交额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_turnover_rate') {  //CPC成交额占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")'. $rate_fields .") * 1.0000 / nullif( SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, ' (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = 'sum( (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")' . $rate_fields . ") * 1.0000 / nullif( SUM(report.report_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, ' (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d")' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                }
            } else if ($time_target == 'cpc_avg_click_cost') {  //CPC平均点击花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ),0) ';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
                } else {
                    $fields['count_total'] = 'sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ),0) ';
                    $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks');
                }

            } else if ($time_target == 'cpc_acos') {  // ACOS
                $fields['count_total'] = 'SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) * 1.0000 / nullif( sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  ) , 0 ) ';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" ');
            } else if ($time_target == 'cpc_direct_sales_volume') {  //CPC直接销量
                $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"');
            } else if ($time_target == 'cpc_direct_sales_quota') {  //CPC直接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" )';
                    $time_fields = $this->getTimeFields($time_line, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU"');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )  * 1.0000 / nullif(sum( report.byorder_sales_volume ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"', 'report.byorder_sales_volume');
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" )  * 1.0000 / nullif(sum( report.report_sales_volume ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU"', 'report.report_sales_volume');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume') {  //CPC间接销量
                $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ) ';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU"');
            } else if ($time_target == 'cpc_indirect_sales_quota') {  //CPC间接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU"  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" ');
                } else {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" )  * 1.0000 / nullif(sum( report.byorder_sales_volume ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ', 'report.byorder_sales_volume');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" )  * 1.0000 / nullif(sum( report.report_sales_volume ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" ', 'report.report_sales_volume');
                }
            } else if ($time_target == 'other_vat_fee') { //VAT
                if ($datas['finance_datas_origin'] == 1) {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                        $time_fields = $this->getTimeFields($time_line, '0-report.byorder_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                        $time_fields = $this->getTimeFields($time_line, '0-report.report_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, '(0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            }
            else if ($time_target == 'cost_profit_total_income') { //总收入
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = "sum( report.byorder_sales_quota )";
                            $time_fields = $this->getTimeFields($time_line, 'report.byorder_sales_quota ');
                        } else {
                            $fields['count_total'] = "sum( (report.byorder_sales_quota  ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_sales_quota ) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    } elseif ($datas['sale_datas_origin'] == '2') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = "sum( report.report_sales_quota  )";
                            $time_fields = $this->getTimeFields($time_line, ' report.report_sales_quota  ');
                        } else {
                            $fields['count_total'] = "sum( (report.report_sales_quota ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                            $time_fields = $this->getTimeFields($time_line, '(report.report_sales_quota ) * ({:RATE} / COALESCE(rates.rate ,1))  ');
                        }
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['sale_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = "sum( report.byorder_sales_quota)";
                            $time_fields = $this->getTimeFields($time_line, ' report.byorder_sales_quota  ');
                        } else {
                            $fields['count_total'] = "sum( (report.byorder_sales_quota  ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                            $time_fields = $this->getTimeFields($time_line, '  (report.byorder_sales_quota ) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    } elseif ($datas['sale_datas_origin'] == '2') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = "sum( report.report_sales_quota  )";
                            $time_fields = $this->getTimeFields($time_line, '  report.report_sales_quota  ');
                        } else {
                            $fields['count_total'] = "sum( (report.report_sales_quota ) * ({:RATE} / COALESCE(rates.rate ,1)) )";
                            $time_fields = $this->getTimeFields($time_line, '   (report.report_sales_quota + report.report_refund_promote_discount) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    }
                }
            } else if ($time_target == 'cost_profit_total_pay') { //总支出
                $cost_profit_total_income_str = '';
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '1') {
                        $cost_profit_total_income_str = " - report.byorder_sales_quota + report.byorder_refund_promote_discount ";
                    } elseif ($datas['sale_datas_origin'] == '2') {
                        $cost_profit_total_income_str = " - report.report_sales_quota + report.byorder_refund_promote_discount ";
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['sale_datas_origin'] == '1') {
                        $cost_profit_total_income_str = " - report.report_sales_quota + report.byorder_refund_promote_discount ";
                    } elseif ($datas['sale_datas_origin'] == '2') {
                        $cost_profit_total_income_str = " - report.report_sales_quota + report.byorder_refund_promote_discount ";
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
            }
            elseif ($time_target == 'goods_promote_coupon') {
                $goodsSplitField = $this->default_goods_split_fields;
                $tempField = $goodsSplitField['goods_promote_coupon'];

                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField})";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField}");
                } else {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($time_target == 'goods_other_review_enrollment_fee') {
                $goodsSplitField = $this->default_goods_split_fields;
                $tempField = $goodsSplitField['goods_other_review_enrollment_fee'];

                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField})";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField}");
                } else {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } elseif ($time_target == 'goods_promote_run_lightning_deal_fee') {
                $goodsSplitField = $this->default_goods_split_fields;
                $tempField = $goodsSplitField['goods_promote_run_lightning_deal_fee'];

                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField})";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField}");
                } else {
                    $fields['count_total'] = "SUM(report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($time_line, "report.monthly_sku_{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            }
            elseif ($custom_target && $custom_target['target_type'] == 1) {
                $tempField = "report.monthly_sku_" . $custom_target['month_goods_field'];
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $custom_target['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($time_line, $tempField);
                }
            } elseif (in_array($time_target, $keys)) {
                $tempField = "report.monthly_sku_" . $new_target_keys[$time_target]['month_goods_field'];
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $new_target_keys[$time_target]['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($time_line, $tempField);
                }
            }
            else {
                $datas['time_target'] = $time_target;
                $fields_tmp = $this->getTimeField($datas, $time_line);
                $fields['count_total'] = $fields_tmp['count_total'];
                $time_fields = $fields_tmp['time_fields'];
            }

            $fields[$time_target] = $fields['count_total'];
            $time_fields_arr[$time_target] = $time_fields;
        }
        if($custom_target && $custom_target['target_type'] == 2){
            $this->dealTimeTargets($fields,$custom_target,$time_line,$time_fields_arr,$target_key);
        }else {
            if (!empty($time_fields) && is_array($time_fields)) {
                foreach ($time_fields as $kt => $time_field) {
                    $fields[$kt] = $time_field;
                }
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
                $fields[strval($time['key'])] = "({$fun}(CASE WHEN (report.create_time>={$time['start']} and report.create_time<={$time['end']}) THEN ({$field1}) ELSE 0 END)) * 1.0000 / nullif({$fun}(CASE WHEN (report.create_time>={$time['start']} and report.create_time<={$time['end']}) THEN ({$field2}) ELSE 0 END),0) ";
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
                if($type == 1){//商品的fbm刷的没问题
                    $first_fields = 'report.fbm_first_logistics_head_course';
                }else{//店铺和运营人员的fbm刷的有问题，特殊处理
                    $first_fields = '(report.first_logistics_head_course - report.fba_first_logistics_head_course)';
                }
                $goods_time_filed       = $this->handleTimeFields($datas,$timeLine,5,'report.byorder_fbm_logistics_head_course','report.report_fbm_logistics_head_course',$first_fields);
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
                        $fields['count_total'] = "SUM(({$report_fields}) * ({:RATE} / COALESCE(rates.rate ,1)))";
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
                    $fields['count_total'] = "sum({$by_order_fields} )";
                    $time_fields = $this->getTimeFields($timeLine, "{$by_order_fields}");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum({$report_fields})";
                    $time_fields = $this->getTimeFields($timeLine, "{$report_fields}");
                }
                break;
            case 5://财务数据源包含货币包含先进先出
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( {$by_order_fields} ) ";
                            $time_fields = $this->getTimeFields($timeLine, $by_order_fields);
                        } else {
                            $fields['count_total'] = " sum( {$by_order_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$by_order_fields} * ({:RATE} / COALESCE(rates.rate ,1))");
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( {$report_fields} ) ";
                            $time_fields = $this->getTimeFields($timeLine, $report_fields);
                        } else {
                            $fields['count_total'] = " sum( {$report_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, "{$report_fields} * ({:RATE} / COALESCE(rates.rate ,1))");
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(( {$first_fields}) ) ";
                        $time_fields = $this->getTimeFields($timeLine, " ({$first_fields})");
                    } else {
                        $fields['count_total'] = " sum( ({$first_fields} * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
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
                        $fields['count_total'] = "SUM(({$report_fields}) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, "($report_fields) * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
                break;
            default:
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "sum(report.byorder_refund_num )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum(report.report_refund_num )";
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
                    $fields['fba_sales_quota'] = "SUM((report.report_fba_sales_quota) * ({:RATE} / COALESCE(rates.rate ,1)))";
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
                    $fields['fbm_sales_quota'] = "SUM((report.report_fbm_sales_quota) * ({:RATE} / COALESCE(rates.rate ,1)))";
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
                if($type == 1){//商品的fbm刷的没问题
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = "SUM(report.fbm_first_logistics_head_course)";
                    } else {
                        $fields['fbm_logistics_head_course'] = "SUM((report.fbm_first_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)))";
                    }
                }else{//店铺和运营人员的fbm刷的有问题，特殊处理
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['fbm_logistics_head_course'] = " sum( (report.first_logistics_head_course - report.fba_first_logistics_head_course) ) ";
                    } else {
                        $fields['fbm_logistics_head_course'] = " sum(( (report.first_logistics_head_course - report.fba_first_logistics_head_course) * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }
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

    private function getIsMysql($params){
        $isMysql = false;

//        if ($params['origin_create_start_time'] >= '1619798400' && $params['origin_create_end_time'] < '1622476800' &&  !in_array($params['count_dimension'],array("site_group","admin_id","department","operators")) && !($params['count_periods'] == 3 || $params['count_periods'] == 4 || $params['count_periods'] == 5) && $params['cost_count_type'] != 2) {
//            $isMysql = true;
//        }
        $today = strtotime(date("Y-m-d"));
        $start_time = $today - 15*86400;
        if ($params['origin_create_start_time'] >= $start_time && $params['origin_create_end_time'] < ($today+86400) &&  in_array($params['count_dimension'],array("channel_id","site_id")) && !($params['count_periods'] == 3 || $params['count_periods'] == 4 || $params['count_periods'] == 5) && $params['cost_count_type'] != 2) {
            $isMysql = true;
        }

        return $isMysql;
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
        array $rateInfo = [],
        int $day_param = 1
    ) {
        $fields = [];
        $isMysql = $this->getIsMysql($params);
        $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
        //没有按周期统计 ， 按指标展示
        if ($params['show_type'] == 2) {

            $target_key_str = trim("'" . implode("','",explode(",",$params['target'])) . "'");
            $customTargetsList = $datas_ark_custom_target_md->getList("user_id = {$userId} AND target_type IN(1, 2) AND target_key IN ({$target_key_str}) AND count_dimension IN (1,3)");
            $this->customTargetsList = $customTargetsList;

            $fields_arr = $this->getUnGoodsFields($params,$isMysql);
            $fields = $fields_arr['fields'];
            $fba_target_key = $fields_arr['fba_target_key'];
        } else {

            $customTarget = $datas_ark_custom_target_md->get_one("user_id = {$userId} AND target_type IN(1, 2) AND target_key = '{$params['time_target']}' AND count_dimension IN (1,3)");
            $this->timeCustomTarget = $customTarget;
            if ($customTarget && $customTarget['target_type'] == 1 && $customTarget['count_dimension'] == 3){
                $this->countDimensionChannel = true;
            }

            $fields = $this->getUnGoodsTimeFields($params, $timeLine,$isMysql);
        }

        if (empty($fields)) {
            return [];
        }

        $mod_where = "report.user_id_mod = " . ($params['user_id'] % 20);


        $ym_where = $this->getYnWhere($params['max_ym'] , $params['min_ym'] ) ;
        $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;

        if(($params['count_periods'] == 0 || $params['count_periods'] == 1) && $params['cost_count_type'] != 2){ //按天或无统计周期
            $table = "{$this->table_channel_day_report} AS report LEFT JOIN {$this->table_channel} as channel ON report.channel_id = channel.id ";

        }else if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){  //按周
//            $table = "{$this->table_channel_day_report} AS report" ;
            $table = "{$this->table_channel_day_report} AS report LEFT JOIN {$this->table_channel} as channel ON report.channel_id = channel.id ";
//            $where = $ym_where . " AND report.available = 1 "   . (empty($where) ? "" : " AND " . $where) ;
        }else if($params['count_periods'] == 3 || $params['count_periods'] == 4 || $params['count_periods'] == 5 ){
//            $isMysql = false;
//            $table = "{$this->table_channel_month_report} AS report" ;
            $table = "{$this->table_channel_month_report} AS report LEFT JOIN {$this->table_channel} as channel ON report.channel_id = channel.id ";
        }else if($params['cost_count_type'] == 2 ){
//            $isMysql = false;
//            $table = "{$this->table_channel_month_report} AS report" ;
            $table = "{$this->table_channel_month_report} AS report LEFT JOIN {$this->table_channel} as channel ON report.channel_id = channel.id ";
        } else {
            return [];
        }


        //部门维度统计
        if ($params['count_dimension'] == 'department') {
            $table .= " LEFT JOIN {$this->table_department_channel} as dc ON dc.user_id = report.user_id AND dc.channel_id = report.channel_id  LEFT JOIN {$this->table_user_department} as ud ON ud.id = dc.user_department_id ";
            $where .= " AND ud.status < 3 AND dc.status = 1 ";
            $admin_info = UserAdminModel::query()->select('is_master', 'is_responsible', 'user_department_id')->where('user_id', $userId)->where('id', $adminId)->first();
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
            if ($isMysql) {
                $fields_arr[] = $field . " AS '" . $field_name . "'";
            } else {
                $fields_arr[] = $field . ' AS "' . $field_name . '"';
            }
        }


        $field_data = str_replace("{:RATE}", $exchangeCode, str_replace("COALESCE(rates.rate ,1)","(COALESCE(rates.rate ,1)*1.000000)", implode(',', $fields_arr)));//去除presto除法把数据只保留4位导致精度异常，如1/0.1288 = 7.7639751... presto=7.7640
        $field_data = str_replace("{:DAY}", $day_param, $field_data);

        if ($params['currency_code'] != 'ORIGIN') {
            if (empty($currencyInfo) || $currencyInfo['currency_type'] == '1') {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = 0 ";
            } else {
                $table .= " LEFT JOIN {$this->table_site_rate} as rates ON rates.site_id = report.site_id AND rates.user_id = report.user_id ";
            }
        }

        if ($this->countDimensionChannel){
            //新增指标是店铺维度时  f_monthly_profit_report_001表year month为text，需转成int连表
            $table .= " LEFT JOIN {$this->table_channel_monthly_profit_report} as monthly_profit ON monthly_profit.db_num='{$this->dbhost}' AND monthly_profit.user_id = report.user_id AND monthly_profit.channel_id = report.channel_id AND CAST(monthly_profit.year AS INTEGER) = report.myear AND CAST(monthly_profit.month AS INTEGER) = report.mmonth";
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
                    if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){
                        $group = 'report.channel_id , report.mweekyear , report.mweek ';
                        $orderby = 'report.channel_id , report.mweekyear , report.mweek ';
                    }else{
                        $group = 'report.channel_id_group ';
                        $orderby = 'report.channel_id_group ';
                    }
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
                    if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){
                        $group = 'report.site_id , report.mweekyear , report.mweek ';
                        $orderby = 'report.site_id , report.mweekyear , report.mweek ';
                    }else{
                        $group = 'report.site_id_group ';
                        $orderby = 'report.site_id_group ';
                    }
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
                    if($params['count_periods'] == 2 && $params['cost_count_type'] != 2){
                        $group = 'report.area_id , report.mweekyear , report.mweek ';
                        $orderby = 'report.area_id , report.mweekyear , report.mweek ';
                    }else{
                        $group = 'report.area_id_group ';
                        $orderby = 'report.area_id_group ';
                    }

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
        $group = str_replace("{:DAY}", $day_param, $group);
        $orderby = str_replace("{:DAY}", $day_param, $orderby);
        $limit_num = 0 ;
        $count = 0 ;
        if($params['show_type'] == 2 && $params['limit_num'] > 0 ){
            $limit_num = $params['limit_num'] ;
        }
        if ($count_tip == 2) { //仅统计总条数
            $count = $this->getTotalNum($where, $table, $group, false, $isMysql);
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        } else if ($count_tip == 1) {  //仅仅统计列表
            if ($params['is_count'] == 1){
                $where = $this->getLimitWhere($where,$params,$table,$limit,$orderby,$group);
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields, $exchangeCode, $day_param, $field_data, $table, $where, $group, false, $isMysql);
                }else {
                    $lists = $this->select($where, $field_data, $table, $limit,'','',false,null,300, $isMysql);
                }
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group,false,null,300,$isMysql);
                if($params['show_type'] = 2 && ( !empty($fields['fba_goods_value']) || !empty($fields['fba_stock']) || !empty($fields['fba_need_replenish']) || !empty($fields['fba_predundancy_number']) )){
                    $lists = $this->getUnGoodsFbaData($lists , $fields , $params,$channel_arr, $currencyInfo, $exchangeCode,$isMysql) ;
                }
                //自定义公式涉及到fba
                if ($params['show_type'] == 2 && !empty($lists)) {
                    foreach ($lists as $k => $item) {
                        foreach ($item as $key => $value) {
                            if (in_array($key, $fba_target_key)) {
                                $item[$key] = $this->count_custom_formula($value, $item);
                            }
                        }
                        $lists[$k] = $item;
                    }
                }
            }
        } else {  //统计列表和总条数
            if ($params['is_count'] == 1){
                $where = $this->getLimitWhere($where,$params,$table,$limit,$orderby,$group);
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields, $exchangeCode, $day_param, $field_data, $table, $where, $group, false, $isMysql);
                }else {
                    $lists = $this->select($where, $field_data, $table, $limit, '', '', false, null, 300, $isMysql);
                }
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByUnGoods Total Request', [$this->getLastSql()]);
            }else{

                $parallel = new Parallel();
                $parallel->add(function () use($where, $field_data, $table, $limit, $orderby, $group, $isMysql) {
                    $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group, false, null, 300, $isMysql);

                    return $lists;
                });
                $parallel->add(function () use($where, $table, $group, $isMysql) {
                    $count = $this->getTotalNum($where, $table, $group, false, $isMysql);
                    return $count;
                });

                try{
                    // $results 结果为 [1, 2]
                    $results = $parallel->wait();
                    $lists = $results[0];
                    $count = $results[1];
                } catch(ParallelExecutionException $e){
                    // $e->getResults() 获取协程中的返回值。
                    // $e->getThrowables() 获取协程中出现的异常。
                }


                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByUnGoods Request', [$this->getLastSql()]);
                if($params['show_type'] = 2 && ( !empty($fields['fba_goods_value']) || !empty($fields['fba_stock']) || !empty($fields['fba_need_replenish']) || !empty($fields['fba_predundancy_number']) )){
                    $lists = $this->getUnGoodsFbaData($lists , $fields , $params,$channel_arr, $currencyInfo, $exchangeCode,$isMysql) ;
                }
                //自定义公式涉及到fba
                if ($params['show_type'] == 2 && !empty($lists)) {
                    foreach ($lists as $k => $item) {
                        foreach ($item as $key => $value) {
                            if (in_array($key, $fba_target_key)) {
                                $item[$key] = $this->count_custom_formula($value, $item);
                            }
                        }
                        $lists[$k] = $item;
                    }
                }
            }
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
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

    /**
     * @param $datas
     * @return array
     */
    private function getUnGoodsFields($datas,$isMysql = false)
    {
        $fields = [];
        $fields['user_id'] = 'max(report.user_id)';
        $fields['site_country_id'] = 'max(report.site_id)';

        if ($datas['count_dimension'] === 'channel_id') {
            $fields['site_id'] = 'max(report.site_id)';
            $fields['channel_id'] = 'max(report.channel_id)';
            $fields['operators'] = 'max(report.operation_user_admin_name)';
            $fields['operation_user_admin_id'] = 'max(channel.operation_user_admin_id)';
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
        $targets_temp = $targets;//基础指标缓存

        //自定义指标
        $targets = $this->addCustomTargets($targets, $this->customTargetsList);

        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }
        if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
            $fields['goods_conversion_rate'] = 'sum( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)';
        }
        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_volume'] = " sum( report.byorder_sales_volume +  report.byorder_group_id ) ";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_volume'] = " sum( report.report_sales_volume + report.report_group_id ) ";
            }
        }
        if (in_array('sale_many_channel_sales_volume', $targets)) { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.byorder_group_id )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.report_group_id )";
            }
        }
        //订单数
        if (in_array('sale_order_number', $targets)) {
            $fields['sale_order_number'] = "sum( report.bychannel_sales_volume )";
        }

        if (in_array('sale_sales_quota', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('amazon_fee_rate', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('operate_fee_rate', $targets) || in_array('evaluation_fee_rate', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_cost_rate', $targets)) {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        //订单金额
        if (in_array('sale_sales_dollars', $targets) ) {
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['sale_sales_dollars'] = "sum( report.bychannel_sales_quota )";
            } else {
                $fields['sale_sales_dollars'] = "sum( report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
            }
        }

        if (in_array('sale_return_goods_number', $targets) || in_array('sale_refund_rate', $targets)) {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['sale_return_goods_number'] = "sum(report.byorder_refund_num )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_return_goods_number'] = "sum(report.report_refund_num )";
            }
        }
        if (in_array('sale_refund', $targets)) {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.byorder_refund )";
                } else {
                    $fields['sale_refund'] = "sum( ( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "sum( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = '('.$fields['sale_return_goods_number'] . ") * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
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

        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('cost_profit_total_pay', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    }
                }
            }

        }
        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets) || in_array('cost_profit_total_pay', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum(  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( ( report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            }

        }

        if (in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('cost_profit_total_pay', $targets) ) {  //毛利润
            $repair_data = '' ;
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['sale_datas_origin'] == '2') {
                    $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                }
                if ($datas['refund_datas_origin'] == '2') {
                    $repair_data .=  " + report.byorder_refund - report.report_refund ";
                }
            }else{
                if ($datas['sale_datas_origin'] == '1') {
                    $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                }
                if ($datas['refund_datas_origin'] == '1') {
                    $repair_data .=  " + report.report_refund - report.byorder_refund ";
                }
            }
            if(empty($repair_data)){
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(report.byorder_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM(report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(report.report_channel_profit + report.bychannel_channel_profit) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    }
                }
            }else{
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(report.byorder_channel_profit + report.bychannel_channel_profit {$repair_data}) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM((report.byorder_channel_profit".$repair_data.") * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(report.report_channel_profit + report.bychannel_channel_profit {$repair_data}) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM((report.report_channel_profit".$repair_data.") * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))) + {$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']})";
                    }
                }
            }

        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = "({$fields['cost_profit_profit']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
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
                    $fields['amazon_sales_commission'] = "sum( report.byorder_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "sum( report.report_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_other_fee', $targets)) {  //其他亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "sum( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "sum( report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "sum( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ";
                } else {
                    $fields['amazon_other_fee'] = "sum( report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_fba_return_processing_fee', $targets)) {  //FBA退货处理费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.bychannel_fba_storage_fee )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }
        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = "({$fields['amazon_fee']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
        }


        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = "({$fields['purchase_logistics_purchase_cost']} + {$fields['purchase_logistics_logistics_cost']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "sum( report.bychannel_operating_fee ) ";
            } else {
                $fields['operate_fee'] = "sum( report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = "({$fields['operate_fee']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0)";
        }
        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1'){
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = "({$fields['evaluation_fee']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
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
                $fields['cpc_sp_cost'] = " sum( report.byorder_cpc_cost) ";
            } else {
                $fields['cpc_sp_cost'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }
        if (in_array('cpc_sd_cost', $targets)) {  //CPC_SD花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_sd_cost'] = " sum( report.byorder_cpc_sd_cost) ";
            } else {
                $fields['cpc_sd_cost'] = " sum( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) ";
            }
        }


        if (in_array('cpc_cost', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_avg_click_cost', $targets) || in_array('cpc_acos', $targets)) {  //CPC花费
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_cost'] = " sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
            } else {
                $fields['cpc_cost'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = "({$fields['cpc_cost']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3)";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = "({$fields['cpc_click_number']}) * 1.0000 / nullif({$fields['cpc_exposure']}, 0) ";
        }
        // 注！此处将字段名用引号包起来是为避免报错，有些数据库会自动将字段大小写转换，会导致报字段不存在的错误
        if (in_array('cpc_order_number', $targets) || in_array('cpc_order_rate', $targets) || in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
        }
        if (in_array('cpc_order_rate', $targets)) {  //cpc订单占比
            $fields['cpc_order_rate'] = "({$fields['cpc_order_number']}) * 1.0000 / nullif(SUM(report.bychannel_sales_volume), 0) ";
        }
        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = "({$fields['cpc_order_number']}) * 1.0000 / nullif({$fields['cpc_click_number']}, 0) ";
        }
        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )';
            } else {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = "({$fields['cpc_turnover']}) * 1.0000 / nullif({$fields['sale_sales_quota']}, 0) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = "({$fields['cpc_cost']}) * 1.0000 / nullif({$fields['cpc_click_number']}, 0) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = "({$fields['cpc_cost']}) * 1.0000 / nullif({$fields['cpc_turnover']}, 0) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = "({$fields['cpc_direct_sales_volume']}) * 1.0000 / nullif({$fields['sale_sales_volume']}, 0) ";
        }
        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8) ';
        }
        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report.bychannel_reserved_field5 - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report.bychannel_reserved_field6 )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_reserved_field5 * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report.bychannel_reserved_field6 * ({:RATE} / COALESCE(rates.rate ,1))   )';
            }
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = "({$fields['cpc_indirect_sales_volume']}) * 1.0000 / nullif({$fields['sale_sales_volume']}, 0) ";
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
            $fields['fba_sales_volume'] = 'sum( report.bychannel_fba_sales_volume )';
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

        //店铺月销售额目标
        if (in_array('sale_channel_month_goal', $targets)) {
            $this->countDimensionChannel = true;
            if ($datas['currency_code'] != 'ORIGIN'){
                $fields['sale_channel_month_goal'] = 'SUM(monthly_profit.reserved_field11 / COALESCE(rates.rate, 1) * {:RATE})';
            }else{
                $fields['sale_channel_month_goal'] = 'SUM(monthly_profit.reserved_field11)';
            }
        }

        if (in_array('cost_profit_total_income', $targets) || in_array('cost_profit_total_pay', $targets)  ) {  //总收入
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "sum( report.byorder_sales_quota + report.channel_fbm_safe_t_claim_demage)";
                } else {
                    $fields['cost_profit_total_income'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) + report.channel_fbm_safe_t_claim_demage * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['cost_profit_total_income'] = "sum( report.report_sales_quota + report.channel_fbm_safe_t_claim_demage )";
                } else {
                    $fields['cost_profit_total_income'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) +  report.channel_fbm_safe_t_claim_demage * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('cost_profit_total_pay', $targets)) {  //总支出
            $filed2 = " + report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund";//广告结款
            $filed6 = " + report.bychannel_operating_fee";//运营费用
            $filed8 = " + report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee";//其他
            if ($datas['finance_datas_origin'] == '1') {
                $filed1 = "report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee";//亚马逊费用
                $filed3 = " - report.byorder_reserved_field17";//VAT
                $filed4 = " + report.byorder_purchasing_cost";//采购成本
                $filed5 = " + report.byorder_logistics_head_course";//物流/头程
                $filed7 = " + report.byorder_reserved_field10";//测评费用
                $filed9 = " + report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee";//店铺促销费用
                $filed10 = " + report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee";//商品调整
            }else{
                $filed1 = "report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee";//亚马逊费用
                $filed3 = " - report.report_reserved_field17";//VAT
                $filed4 = " + report.report_purchasing_cost";//采购成本
                $filed5 = " + report.report_logistics_head_course";//物流/头程
                $filed7 = " + report.report_reserved_field10";//测评费用
                $filed9 = " + report.report_refund_promote_discount + report.report_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee";//店铺促销费用
                $filed10 = " + report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee";//商品调整费用
            }
            if ($datas['refund_datas_origin'] == '1') {
                $filed11 = "  - report.byorder_refund";//退款
            } else {
                $filed11 = "  - report.report_refund";//退款
            }
            $file_total = $filed1.$filed2.$filed3.$filed4.$filed5.$filed6.$filed7.$filed8.$filed9.$filed10.$filed11;
            if ($datas['currency_code'] != 'ORIGIN') {
                $file_total = str_replace("+"," * ({:RATE} / COALESCE(rates.rate ,1)) +" , $file_total);
                $file_total = str_replace("-"," * ({:RATE} / COALESCE(rates.rate ,1)) -" , $file_total);
                $file_total .= " * ({:RATE} / COALESCE(rates.rate ,1))";
            }
            $fields['cost_profit_total_pay'] = "sum({$file_total})";
        }
        $this->getUnTimeFields($fields, $datas, $targets, 2);

        //加入自定义指标
        $fba_target_key = [];
        $is_count = !empty($datas['is_count']) ? $datas['is_count'] : 0;
        $this->getCustomTargetFields($fields,$this->customTargetsList,$targets,$targets_temp, $datas,$fba_target_key,$is_count,$isMysql);
        return ['fields' => $fields,'fba_target_key' => $fba_target_key];
    }

    //按非商品维度,时间展示字段（新增统计维度完成）
    protected function getUnGoodsTimeFields($datas, $timeLine,$isMysql = false)
    {
        $fields = [];
        $fields['user_id'] = 'max(report.user_id)';
        $fields['site_country_id'] = 'max(report.site_id)';
        if ($datas['count_dimension'] == 'channel_id') {
            $fields['site_id'] = 'max(report.site_id)';
            $fields['channel_id'] = 'max(report.channel_id)';
            $fields['operators'] = 'max(report.operation_user_admin_name)';
            $fields['operation_user_admin_id'] = 'max(channel.operation_user_admin_id)';
        } else if ($datas['count_dimension'] == 'site_id') {
            $fields['site_id'] = 'max(report.site_id)';
        } else if ($datas['count_dimension'] == 'site_group') {
            $fields['site_group'] = 'max(report.area_id)';
        }else if($datas['count_dimension'] == 'department'){
            $fields['user_department_id'] = 'max(dc.user_department_id)';
        }else if($datas['count_dimension'] == 'admin_id'){
            $fields['admin_id'] = 'max(uc.admin_id)';
            $fields['user_admin_id'] = 'max(uc.admin_id)';
        }

        $target_key = $datas['time_target'];
        $keys = [];
        $new_target_keys = [];
        if($this->timeCustomTarget && $this->timeCustomTarget['target_type'] == 2){
            $time_targets = explode(",",$this->timeCustomTarget['formula_fields']);
            //公式所涉及到的新增指标
            $target_key_str = trim("'" . implode("','",$time_targets) . "'");
            $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
            $new_target = $datas_ark_custom_target_md->getList("user_id = {$datas['user_id']} AND target_type = 1 AND count_dimension IN (1,2) AND target_key IN ($target_key_str)","target_key,month_goods_field,format_type");
            $keys = array_column($new_target,'target_key');
            $new_target_keys = array_column($new_target,null,'target_key');
        }else{
            $time_targets = array($target_key);
        }
        $time_fields = [];
        $time_fields_arr = array();
        foreach ($time_targets as $time_target) {
            if ($time_target == 'goods_visitors') {  // 买家访问次数
                $fields['count_total'] = "SUM(report.byorder_user_sessions)";
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_user_sessions');
            } else if ($time_target == 'goods_conversion_rate') { //订单商品数量转化率
                $fields['count_total'] = "SUM(report.byorder_quantity_of_goods_ordered) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)";
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
            } else if ($time_target == 'sale_sales_volume') { //销售量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = " sum( report.byorder_sales_volume + report.byorder_group_id ) ";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_volume + report.byorder_group_id ");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = " sum( report.report_sales_volume + report.report_group_id) ";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_sales_volume + report.report_group_id");
                }
            } else if ($time_target == 'sale_many_channel_sales_volume') { //多渠道数量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "sum( report.byorder_group_id )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_group_id");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = "sum( report.report_group_id )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_group_id");
                }
            } else if ($time_target == 'sale_order_number') {//订单数
                $fields['count_total'] = "sum( report.bychannel_sales_volume )";
                $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_volume");
            } else if ($time_target == 'sale_sales_quota') {  //商品销售额
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_sales_quota )";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_sales_quota )";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
            } else if ($time_target == 'sale_sales_dollars') { //订单金额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "sum( report.bychannel_sales_quota )";
                    $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota");
                } else {
                    $fields['count_total'] = "sum( report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, "report.bychannel_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                }
            } else if ($time_target == 'sale_return_goods_number') {  //退款量
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "sum(report.byorder_refund_num )";
                    $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum(report.report_refund_num )";
                    $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num");
                }
            } else if ($time_target == 'sale_refund') {  //退款
                if ($datas['refund_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( 0 - report.byorder_refund )";
                        $time_fields = $this->getTimeFields($timeLine, " 0 - report.byorder_refund ");
                    } else {
                        $fields['count_total'] = "sum( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, " (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                } elseif ($datas['refund_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_refund )";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_refund ");
                    } else {
                        $fields['count_total'] = "sum( report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_refund * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                }
            } else if ($time_target == 'sale_refund_rate') {  //退款率
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['refund_datas_origin'] == '1') {
                        $fields['count_total'] = "sum(report.byorder_refund_num) * 1.0000 / nullif(SUM((report.byorder_sales_volume + report.byorder_group_id)),0)";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num * 1.0000", "(report.byorder_sales_volume+ report.byorder_group_id)");
                    } elseif ($datas['refund_datas_origin'] == '2') {
                        $fields['count_total'] = "sum(report.report_refund_num) * 1.0000 / nullif(SUM((report.byorder_sales_volume+ report.byorder_group_id)),0)";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num * 1.0000 ", "(report.byorder_sales_volume+ report.byorder_group_id)");
                    }
                } else {
                    if ($datas['refund_datas_origin'] == '1') {
                        $fields['count_total'] = "sum(report.byorder_refund_num) * 1.0000  / nullif(SUM((report.report_sales_volume+ report.report_group_id)),0)";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_refund_num * 1.0000 ", "(report.report_sales_volume+ report.report_group_id)");
                    } elseif ($datas['refund_datas_origin'] == '2') {
                        $fields['count_total'] = "sum(report.report_refund_num) * 1.0000 / nullif(SUM((report.report_sales_volume+ report.report_group_id)),0)";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_refund_num * 1.0000 ", "(report.report_sales_volume+ report.report_group_id)");
                    }
                }
            } else if ($time_target == 'promote_discount') {  //promote折扣
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
            } else if ($time_target == 'promote_refund_discount') {  //退款返还promote折扣
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
            } else if ($time_target == 'promote_store_fee') { //店铺促销费用
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
            } else if ($time_target == 'cost_profit_profit') {  //毛利润
                $repair_data = '';
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '2') {
                        $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '2') {
                        $repair_data .= " + report.byorder_refund - report.report_refund ";
                    }
                } else {
                    if ($datas['sale_datas_origin'] == '1') {
                        $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '1') {
                        $repair_data .= " + report.report_refund - report.byorder_refund ";
                    }
                }
                if (empty($repair_data)) {
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
                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            if ($datas['cost_count_type'] == '1') {
                                $fields['count_total'] = "SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course {$repair_data})";
                                $time_fields = $this->getTimeFields($timeLine, "report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course {$repair_data}");
                            } else {
                                $fields['count_total'] = "SUM((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit {$repair_data})";
                                $time_fields = $this->getTimeFields($timeLine, "(report.first_purchasing_cost + report.first_logistics_head_course {$repair_data}) + report.byorder_channel_profit + report.bychannel_channel_profit");
                            }
                        } else {
                            if ($datas['cost_count_type'] == '1') {
                                $fields['count_total'] = 'SUM((report.byorder_channel_profit' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($timeLine, '(report.byorder_channel_profit' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM(  ((report.first_purchasing_cost' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($timeLine, '  ((report.first_purchasing_cost' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) +  report.byorder_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            if ($datas['cost_count_type'] == '1') {
                                $fields['count_total'] = "SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course {$repair_data} )";
                                $time_fields = $this->getTimeFields($timeLine, "report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course {$repair_data}");
                            } else {
                                $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course ' . $repair_data . ') + report.report_channel_profit + report.bychannel_channel_profit )';
                                $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course ' . $repair_data . ') + report.report_channel_profit + report.bychannel_channel_profit');
                            }
                        } else {
                            if ($datas['cost_count_type'] == '1') {
                                $fields['count_total'] = 'SUM((report.report_channel_profit' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($timeLine, '(report.report_channel_profit' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM(((report.first_purchasing_cost' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))  )';
                                $time_fields = $this->getTimeFields($timeLine, '((report.first_purchasing_cost' . $repair_data . ') * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) ');
                            }
                        }
                    }
                }

            } else if ($time_target == 'cost_profit_profit_rate') {  //毛利率
                $repair_data = '' ;
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? "" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['finance_datas_origin'] == '1') {
                        if($datas['sale_datas_origin'] == '2'){
                            $repair_data.= " +report.report_sales_quota - report.byorder_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '2'){
                            $repair_data.= empty($repair_data) ? "  +report.byorder_refund - report.report_refund " : " + report.byorder_refund - report.report_refund " ;
                        }
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data.") {$rate_fields})) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data . ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( ((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit'.$repair_data." + report.bychannel_channel_profit  ) {$rate_fields})) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, ' ((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit '.$repair_data . ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        }
                    } else {
                        if($datas['sale_datas_origin'] == '1'){
                            $repair_data.= " +report.byorder_sales_quota - report.report_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '1'){
                            $repair_data.= empty($repair_data) ? "  +report.report_refund - report.byorder_refund " : " + report.report_refund - report.byorder_refund" ;
                        }

                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data.") {$rate_fields})) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course'.$repair_data. ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM((( report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  '.$repair_data.") {$rate_fields})) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(( report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit'.$repair_data. ") {$rate_fields}", 'report.byorder_sales_quota' . $rate_fields);
                        }
                    }
                } else {
                    if ($datas['finance_datas_origin'] == '1') {

                        if($datas['sale_datas_origin'] == '2'){
                            $repair_data.= " +report.report_sales_quota - report.byorder_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '2'){
                            $repair_data.= empty($repair_data) ? "  +report.byorder_refund - report.report_refund " : " + report.byorder_refund - report.report_refund " ;
                        }

                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data.") {$rate_fields})) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course'.$repair_data. ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM((( report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_channel_profit + report.bychannel_channel_profit'.$repair_data.") {$rate_fields})) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(( report.first_purchasing_cost + report.first_logistics_head_course) +  report.byorder_channel_profit + report.bychannel_channel_profit'.$repair_data. ") {$rate_fields}", 'report.report_sales_quota' . $rate_fields);
                        }
                    } else {
                        if($datas['sale_datas_origin'] == '1'){
                            $repair_data.= " +report.byorder_sales_quota - report.report_sales_quota  " ;
                        }
                        if($datas['refund_datas_origin'] == '1'){
                            $repair_data.= empty($repair_data) ? "  +report.report_refund - report.byorder_refund " : " + report.report_refund - report.byorder_refund" ;
                        }
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = '(SUM((report.report_channel_profit '.$repair_data."+ report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course) {$rate_fields})) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '(report.report_channel_profit'.$repair_data." + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course) {$rate_fields}", 'report.report_sales_quota'. $rate_fields);
                        } else {
                            $fields['count_total'] = '(SUM( ( (report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_channel_profit + report.bychannel_channel_profit'.$repair_data.") {$rate_fields})) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                            $time_fields = $this->getTimeFields($timeLine, '( (report.first_purchasing_cost + report.first_logistics_head_course) +  report.report_channel_profit + report.bychannel_channel_profit'.$repair_data. ") {$rate_fields}", 'report.report_sales_quota'. $rate_fields);
                        }
                    }
                }

            } else if ($time_target == 'amazon_fee') {  //亚马逊费用
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
            } else if ($time_target == 'amazon_sales_commission') {  //亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_platform_sales_commission ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_platform_sales_commission ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_delivery_fee') {  //FBA代发货费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_profit ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_profit ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_settlement_fee') {  //结算费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }

            } else if ($time_target == 'amazon_other_fee') {  //其他亚马逊费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee');
                    } else {
                        $fields['count_total'] = "sum( report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_return_shipping_fee') {  //返还运费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_returnshipping )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_returnshipping )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_refund_deducted_commission') {  //退款扣除佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.bychannel_fba_storage_fee )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                    } else {
                        $fields['count_total'] = "sum( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.bychannel_fba_storage_fee )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee');
                    } else {
                        $fields['count_total'] = "sum( report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'amazon_fee_rate') {  //亚马逊费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM((report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) {$rate_fields}) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)' . $rate_fields , 'report.byorder_sales_quota' . $rate_fields);

                    } elseif ($datas['finance_datas_origin'] == '2') {
                        $fields['count_total'] = "SUM((report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) {$rate_fields}) /  nullif( sum(report.byorder_sales_quota {$rate_fields}) , 0 ) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    }
                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM((report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) {$rate_fields}) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);

                    } elseif ($datas['finance_datas_origin'] == '2') {
                        $fields['count_total'] = "SUM((report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) {$rate_fields}) /  nullif( sum(report.report_sales_quota {$rate_fields}) , 0 ) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    }
                }
            } else if ($time_target == 'purchase_logistics_purchase_cost') {  //采购成本
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == 1) {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    }

                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(( report.first_purchasing_cost )) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_purchasing_cost / COALESCE(rates.rate ,1)) * {:RATE} ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                    }
                }
            } else if ($time_target == 'purchase_logistics_logistics_cost') {  // 物流/头程
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == 1) {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($timeLine, ' report.byorder_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($timeLine, ' report.report_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($timeLine, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    }

                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course) ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                        $time_fields = $this->getTimeFields($timeLine, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)))');
                    }
                }
            } else if ($time_target == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($timeLine, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost)  ' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = " sum( (report.report_logistics_head_course + report.report_purchasing_cost) {$rate_fields}  )* 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($timeLine, ' (report.report_logistics_head_course + report.report_purchasing_cost)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.byorder_sales_quota {$rate_fields} ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course + report.first_purchasing_cost)' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost) {$rate_fields} ) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($timeLine, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost) ' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                        } else {
                            $fields['count_total'] = " sum( (report.report_logistics_head_course + report.report_purchasing_cost) {$rate_fields}  )* 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                            $time_fields = $this->getTimeFields($timeLine, ' (report.report_logistics_head_course + report.report_purchasing_cost)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost) {$rate_fields}) * 1.0000 / nullif(sum( report.report_sales_quota {$rate_fields} ),0)  ";
                        $time_fields = $this->getTimeFields($timeLine, ' (report.first_logistics_head_course + report.first_purchasing_cost)' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    }
                }
            } else if ($time_target == 'operate_fee') {  //运营费用
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "sum( report.bychannel_operating_fee ) ";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee');
                } else {
                    $fields['count_total'] = "sum( report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'operate_fee_rate') {  //运营费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM(report.bychannel_operating_fee {$rate_fields}) /nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_operating_fee {$rate_fields}) /nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_operating_fee' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                }
            } else if ($time_target == 'evaluation_fee') {  //测评费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($timeLine, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'evaluation_fee_rate') {  //测评费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10 {$rate_fields} ) /nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10 {$rate_fields}) /nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                    }

                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10 {$rate_fields} ) /nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field10' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10 {$rate_fields} ) /nullif(SUM(report.report_sales_quota {$rate_fields}),0)";
                        $time_fields = $this->getTimeFields($timeLine, 'report.report_reserved_field10' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                    }
                }
            } else if ($time_target == 'other_vat_fee') {//VAT
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                        $time_fields = $this->getTimeFields($timeLine, '0-report.byorder_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                        $time_fields = $this->getTimeFields($timeLine, '0-report.report_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($timeLine, '(0-report.report_reserved_field17 )* ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }

            } else if ($time_target == 'other_other_fee') { //其他
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment + report.bychannel_review_enrollment_fee');
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'other_review_enrollment_fee') { //早期评论者计划
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee');
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))  )";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'cpc_ad_settlement') { //广告结款
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund)";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund ');
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1)))";
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_sp_cost') {  //CPC SP 花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost  ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost ');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_sp_cost') {  //CPC SD 花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_sd_cost  ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_sd_cost ');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_cost') {  //CPC花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)');
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_cost_rate') {  //CPC花费占比
                $sale_denominator = "report.byorder_sales_quota";
                if ($datas['sale_datas_origin'] == '2') {
                    $sale_denominator = "report.report_sales_quota";
                }

                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0) )  * 1.0000 / nullif( sum({$sale_denominator} ) , 0 )";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)', $sale_denominator);
                } else {
                    $fields['count_total'] = " sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )  * 1.0000 / nullif( sum({$sale_denominator} * ({:RATE} / COALESCE(rates.rate ,1)) ) , 0 ) ";
                    $time_fields = $this->getTimeFields($timeLine, ' report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) -  COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1))  ', $sale_denominator.' * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'cpc_exposure') {  //CPC曝光量
                $fields['count_total'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3)";
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3  ');
            } else if ($time_target == 'cpc_click_number') {  //CPC点击次数
                $fields['count_total'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)";
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');

            } else if ($time_target == 'cpc_click_rate') {  //CPC点击率
                $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4)) * 1.0000 / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3), 0 ) ";
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4', 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');
            } else if ($time_target == 'cpc_order_number') {  //CPC订单数
                $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7) ';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7');
            } else if ($time_target == 'cpc_order_rate') {  //cpc订单占比
                $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7) * 1.0000 / nullif( SUM(report.bychannel_sales_volume) , 0 )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.bychannel_sales_volume');
            } else if ($time_target == 'cpc_click_conversion_rate') {  //cpc点击转化率
                $fields['count_total'] = '(sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7)) * 1.0000 / nullif( SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4) , 0 )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            } else if ($time_target == 'cpc_turnover') {  //CPC成交额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5"');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'cpc_turnover_rate') {  //CPC成交额占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = '(sum((report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )' .$rate_fields ."))/nullif( SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($timeLine, '( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )' . $rate_fields, 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = '(sum((report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )' .$rate_fields ."))/nullif( SUM(report.report_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($timeLine, ' (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5" )' . $rate_fields, 'report.report_sales_quota' . $rate_fields);
                }
            } else if ($time_target == 'cpc_avg_click_cost') {  //CPC平均点击花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = '(sum( report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0))) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost - COALESCE(report.bychannel_cpc_sb_cost,0)', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
                } else {
                    $fields['count_total'] = '(sum( report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) - COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) )) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                    $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) - COALESCE(report.bychannel_cpc_sb_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
                }
            } else if ($time_target == 'cpc_acos') {  // ACOS
                $fields['count_total'] = '(SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost -  COALESCE(report.bychannel_cpc_sb_cost,0) )) * 1.0000 / nullif( sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" + report."bychannel_reserved_field5"  ) , 0 ) ';
                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_cpc_cost + report.byorder_cpc_sd_cost -  COALESCE(report.bychannel_cpc_sb_cost,0)', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d"  + report."bychannel_reserved_field5" ');
            } else if ($time_target == 'cpc_direct_sales_volume') {  //CPC直接销量

                $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
                $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ');
            } else if ($time_target == 'cpc_direct_sales_quota') {  //CPC直接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6"  )';
                    $time_fields = $this->getTimeFields($timeLine, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" ');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  )  * 1.0000 / nullif(sum( report.byorder_sales_volume+ report.byorder_group_id ) ,0)';
                    $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.byorder_sales_volume+ report.byorder_group_id');
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )  * 1.0000 / nullif(sum( report.report_sales_volume + report.report_group_id  ) ,0)';
                    $time_fields = $this->getTimeFields($timeLine, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.report_sales_volume+ report.report_group_id');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume') {  //CPC间接销量
                $fields['count_total'] = ' SUM(report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8 )';
                $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8');
            } else if ($time_target == 'cpc_indirect_sales_quota') {  //CPC间接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5" - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6" )';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6"');
                } else {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" )  * 1.0000 / nullif(sum( report.byorder_sales_volume + report.byorder_group_id) ,0)';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.byorder_sales_volume + report.byorder_group_id');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" )  * 1.0000 / nullif(sum( report.report_sales_volume + report.report_group_id) ,0)';
                    $time_fields = $this->getTimeFields($timeLine, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.report_sales_volume+ report.report_group_id');
                }
            } else if ($time_target == 'fba_sales_volume') {  //FBA销量
                $fields['count_total'] = 'sum( report.bychannel_fba_sales_volume )';
                $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_fba_sales_volume');
            } else if ($time_target == 'promote_coupon') { //coupon优惠券
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax"');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($time_target == 'promote_run_lightning_deal_fee') {  //RunLightningDealFee';
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($timeLine, 'report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($time_target == 'amazon_order_fee') {  //亚马逊-订单费用
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
            } elseif ($time_target == 'amazon_refund_fee') { //亚马逊-退货退款费用
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
            } elseif ($time_target == 'amazon_stock_fee') { //亚马逊-库存费用
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
            } elseif ($time_target == 'amazon_long_term_storage_fee') { //FBA长期仓储费
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
            } elseif ($time_target == 'goods_adjust_fee') { //商品调整费用
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
            } else if ($time_target == 'cost_profit_total_income') { //总收入
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_sales_quota  + report.channel_fbm_safe_t_claim_demage)";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota + report.channel_fbm_safe_t_claim_demage");
                    } else {
                        $fields['count_total'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) +  report.channel_fbm_safe_t_claim_demage * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) + report.channel_fbm_safe_t_claim_demage * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_sales_quota )";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($timeLine, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
            } elseif ($time_target == 'sale_channel_month_goal') { //店铺月销售额目标
                $this->countDimensionChannel = true;
                if ($datas['currency_code'] != 'ORIGIN'){
                    $fields['count_total'] = "SUM(monthly_profit.reserved_field11 / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, "monthly_profit.reserved_field11 / COALESCE(rates.rate, 1) * {:RATE}");
                } else {
                    $fields['count_total'] = "SUM(monthly_profit.reserved_field11)";
                    $time_fields = $this->getTimeFields($timeLine, "monthly_profit.reserved_field11");
                }
            } else if ($time_target == 'cost_profit_total_pay') { //总支出
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        if ($datas['cost_count_type'] == '1') {
                            if ($datas['sale_datas_origin'] == '1') {
                                $filed1 = "report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee";//亚马逊费用
                                $filed2 = " + report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund";//广告结款
                                $filed3 = " - report.byorder_reserved_field17";//VAT
                                $filed4 = " + report.byorder_purchasing_cost";//采购成本
                                $filed5 = " + report.byorder_logistics_head_course";//物流/头程
                                $filed6 = " + report.bychannel_operating_fee";//运营费用
                                $filed7 = " + report.byorder_reserved_field10";//测评费用
                                $filed8 = " + report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee";//其他
                                $filed9 = " + report.byorder_refund_promote_discount + report.byorder_promote_discount + report.bychannel_coupon_redemption_fee  + report.bychannel_run_lightning_deal_fee";//店铺促销
                                $filed10 = " + report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee";//商品调整
                                $filed11 = "  - report.byorder_refund";//退款
                                $file_total = $filed1 . $filed2 . $filed3 . $filed4 . $filed5 . $filed6 . $filed7 . $filed8 . $filed9 . $filed10 . $filed11;
                                $fields['count_total'] = "SUM({$file_total})";
                                $time_fields = $this->getTimeFields($timeLine, $file_total);
                            } else {
                                $fields['count_total'] = 'SUM(report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.byorder_sales_quota)';
                                $time_fields = $this->getTimeFields($timeLine, 'report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course - report.report_sales_quota');
                            }
                        } else {
                            if ($datas['sale_datas_origin'] == '1') {
                                $fields['count_total'] = 'SUM((report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota)';
                                $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost + report.first_logistics_head_course) + report.byorder_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota');
                            } else {
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
                            } else {
                                $fields['count_total'] = 'SUM(report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.report_sales_quota )';
                                $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit + report.bychannel_channel_profit + report.report_purchasing_cost + report.report_logistics_head_course - report.report_sales_quota');
                            }
                        } else {
                            if ($datas['sale_datas_origin'] == '1') {
                                $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota)';
                                $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.byorder_sales_quota');
                            } else {
                                $fields['count_total'] = 'SUM(  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota)';
                                $time_fields = $this->getTimeFields($timeLine, '  (report.first_purchasing_cost + report.first_logistics_head_course) + report.report_channel_profit + report.bychannel_channel_profit  - report.report_sales_quota');
                            }
                        }
                    } else {
                        if ($datas['cost_count_type'] == '1') {
                            if ($datas['sale_datas_origin'] == '1') {
                                $fields['count_total'] = 'SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM(report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)))';
                                $time_fields = $this->getTimeFields($timeLine, 'report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        } else {
                            if ($datas['sale_datas_origin'] == '1') {
                                $fields['count_total'] = 'SUM((report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                            } else {
                                $fields['count_total'] = 'SUM((report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )';
                                $time_fields = $this->getTimeFields($timeLine, '(report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))) + report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))');
                            }
                        }
                    }
                }
            } elseif ($this->timeCustomTarget && $this->timeCustomTarget['target_type'] == 1) {
                if ($this->timeCustomTarget['count_dimension'] == 3) {
                    $tempField = "monthly_profit." . $this->timeCustomTarget['month_channel_field'];
                } else {
                    $tempField = "report.monthly_sku_" . $this->timeCustomTarget['month_goods_field'];
                }
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $this->timeCustomTarget['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($timeLine, $tempField);
                }
            } elseif (in_array($time_target, $keys)) {
                if ($this->timeCustomTarget['count_dimension'] == 3) {
                    $tempField = "monthly_profit." . $new_target_keys[$time_target]['month_channel_field'];
                } else {
                    $tempField = "report.monthly_sku_" . $new_target_keys[$time_target]['month_goods_field'];
                }
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $new_target_keys[$time_target]['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($timeLine, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($timeLine, $tempField);
                }
            } else {
                $datas['time_target'] = $time_target;
                $fields_tmp = $this->getTimeField($datas, $timeLine, 2);
                $fields['count_total'] = $fields_tmp['count_total'];
                $time_fields = $fields_tmp['time_fields'];

            }
            $fields[$time_target] = $fields['count_total'] ;
            $time_fields_arr[$time_target] = $time_fields ;
        }
        if($this->timeCustomTarget && $this->timeCustomTarget['target_type'] == 2){
            $this->dealTimeTargets($fields, $this->timeCustomTarget,$timeLine,$time_fields_arr,$target_key,$isMysql);
        }else {
            if (!empty($time_fields) && is_array($time_fields)) {
                foreach ($time_fields as $kt => $time_field) {
                    $fields[$kt] = $time_field;
                }
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
    protected function getUnGoodsFbaData($lists = [], $fields = [], $datas = [], $channel_arr = [], $currencyInfo = [], $exchangeCode = '1',$isMysql = false)
    {
        $isMysql = false;
        if(empty($lists)){
            return $lists ;
        } else {
            $table = "{$this->table_amazon_fba_inventory_by_channel} as c";
            $where = 'c.user_id = ' . $lists[0]['user_id'] ." AND c.db_num = '".$this->dbhost."'";
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
                $isMysql = false;

            }else if($datas['count_dimension'] == 'admin_id'){
                $where_strs = array_unique(array_column($where_arr , 'admin_id')) ;
                $where_str = 'uc.admin_id IN (' . implode(',' , $where_strs) . ")" ;
                $isMysql = false;
            }else{
                $where_str = '1=1' ;
            }
        }

        $amazon_fba_inventory_by_channel_md = new AmazonFbaInventoryByChannelPrestoModel($this->dbhost, $this->codeno);
        $amazon_fba_inventory_by_channel_md->dryRun(env('APP_TEST_RUNNING', false));
        $where.= ' AND ' . $where_str ;
        if ($datas['currency_code'] == 'ORIGIN') {
            $fba_fields .= " , sum(DISTINCT(c.yjzhz))  as fba_goods_value";
        } else {
            $fba_fields .= " , sum(DISTINCT(c.yjzhz * ({:RATE} / COALESCE(rates.rate ,1))))  as fba_goods_value";
        }
        $fba_fields.= ' ,SUM(DISTINCT(c.total_fulfillable_quantity)) as fba_stock , SUM(DISTINCT(c.replenishment_sku_nums)) as fba_need_replenish ,SUM(DISTINCT(c.redundancy_sku)) as fba_predundancy_number';
        $fba_fields = str_replace("{:RATE}", $exchangeCode, $fba_fields);
        $fbaData =$amazon_fba_inventory_by_channel_md->select($where , $fba_fields ,$table ,'' , '' ,$group,'',null,300,$isMysql);
        if ($isMysql && !empty($fba_data)){
            foreach ($fba_data as $key => $value){
                $fba_data[$key] = (array) $value;
            }
        }

        $fbaDatas = array() ;
        if($fbaData){
            foreach($fbaData as $fba){
                if($datas['count_dimension'] == 'channel_id'){
                    $fbaDatas[$fba['channel_id']] = $fba ;
                }else if($datas['count_dimension'] == 'site_id'){
                    $fbaDatas[$fba['site_id']] = $fba ;
                }else if($datas['count_dimension'] == 'department'){
                    if (empty($fbaDatas[$fba['user_department_id']])){
                        $fbaDatas[$fba['user_department_id']]['fba_goods_value']= $fba['fba_goods_value'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_stock']= $fba['fba_stock'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_need_replenish']= $fba['fba_need_replenish'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_predundancy_number']= $fba['fba_predundancy_number'] ;
                    }else{
                        $fbaDatas[$fba['user_department_id']]['fba_goods_value']+= $fba['fba_goods_value'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_stock']+= $fba['fba_stock'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_need_replenish']+= $fba['fba_need_replenish'] ;
                        $fbaDatas[$fba['user_department_id']]['fba_predundancy_number']+= $fba['fba_predundancy_number'] ;
                    }

                }else if($datas['count_dimension'] == 'admin_id'){
                    if (empty($fbaDatas[$fba['admin_id']])){
                        $fbaDatas[$fba['admin_id']]['fba_goods_value']= $fba['fba_goods_value'] ;
                        $fbaDatas[$fba['admin_id']]['fba_stock']= $fba['fba_stock'] ;
                        $fbaDatas[$fba['admin_id']]['fba_need_replenish']= $fba['fba_need_replenish'] ;
                        $fbaDatas[$fba['admin_id']]['fba_predundancy_number']= $fba['fba_predundancy_number'] ;
                    }else{
                        $fbaDatas[$fba['admin_id']]['fba_goods_value']+= $fba['fba_goods_value'] ;
                        $fbaDatas[$fba['admin_id']]['fba_stock']+= $fba['fba_stock'] ;
                        $fbaDatas[$fba['admin_id']]['fba_need_replenish']+= $fba['fba_need_replenish'] ;
                        $fbaDatas[$fba['admin_id']]['fba_predundancy_number']+= $fba['fba_predundancy_number'] ;
                    }

                }
            }
        }
        foreach($lists as $k=>$list2){
            if($datas['count_dimension'] == 'channel_id'){
                $fba_data = empty($fbaDatas[$list2['channel_id']]) ? array() :  $fbaDatas[$list2['channel_id']];
            }else if($datas['count_dimension'] == 'site_id'){
                $fba_data = empty($fbaDatas[$list2['site_id']]) ? array() :  $fbaDatas[$list2['site_id']];
            }else if($datas['count_dimension'] == 'department'){
                $fba_data = empty($fbaDatas[$list2['user_department_id']]) ? array() :  $fbaDatas[$list2['user_department_id']];
            }else if($datas['count_dimension'] == 'admin_id'){
                $fba_data = empty($fbaDatas[$list2['admin_id']]) ? array() :  $fbaDatas[$list2['admin_id']];
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
        array $rateInfo = [],
        int $day_param = 1
    ) {
        $datas['is_month_table'] = 0;
        $ym_where = $this->getYnWhere($datas['max_ym'] , $datas['min_ym'] ) ;
        if(($datas['count_periods'] == 0 || $datas['count_periods'] == 1) && $datas['cost_count_type'] != 2){ //按天或无统计周期
//            $where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;
//            $table = "{$this->table_operation_day_report} AS report" ;
            $table = $this->operationTable($datas,$ym_where,'day');
        }else if($datas['count_periods'] == 2 && $datas['cost_count_type'] != 2){  //按周
//            $where = $ym_where . " AND report.available = 1 "   . (empty($where) ? "" : " AND " . $where) ;
            $table = $this->operationTable($datas,$ym_where,'week');
        }else if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
//            $where = $ym_where . " AND report.available = 1 "   . (empty($where) ? "" : " AND " . $where) ;
//            $table = "{$this->table_operation_month_report} AS report";
            $table = $this->operationTable($datas,$ym_where,'month');
            $datas['is_month_table'] = 1;
        }else if($datas['cost_count_type'] == 2){//先进先出只能读取月报
//            $where = $ym_where . " AND report.available = 1 "   . (empty($where) ? "" : " AND " . $where) ;
//            $table = "{$this->table_operation_month_report} AS report";
            $table = $this->operationTable($datas,$ym_where,'month');
            $datas['is_month_table'] = 1;
        } else {
            return [];
        }
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


        //$where = $ym_where . " AND " .$mod_where . " AND report.available = 1 " .  (empty($where) ? "" : " AND " . $where) ;
//        $field_data = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_arr));
        $field_data = str_replace("{:RATE}", $exchangeCode, str_replace("COALESCE(rates.rate ,1)","(COALESCE(rates.rate ,1)*1.000000)", implode(',', $fields_arr)));//去除presto除法把数据只保留4位导致精度异常，如1/0.1288 = 7.7639751... presto=7.7640
        $field_data = str_replace("{:DAY}", $day_param, $field_data);




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
        $group = str_replace("{:DAY}", $day_param, $group);
        $orderby = str_replace("{:DAY}", $day_param, $orderby);
        $limit_num = 0 ;
        $count = 0 ;
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
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields,$exchangeCode,$day_param,$field_data,$table,$where,$group);
                }else {
                    $lists = $this->select($where, $field_data, $table, $limit);
                }
            }else{
                $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);

            }
        } else {  //统计列表和总条数
            if ($datas['is_count'] == 1){
                $where = $this->getLimitWhere($where,$datas,$table,$limit,$orderby,$group);
                if(!empty($where_detail['target'])){
                    $lists = $this->queryList($fields,$exchangeCode,$day_param,$field_data,$table,$where,$group);
                }else {
                    $lists = $this->select($where, $field_data, $table, $limit);
                }
                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByOperators Total Request', [$this->getLastSql()]);
            }else{
                $parallel = new Parallel();
                $parallel->add(function () use($where, $field_data, $table, $limit, $orderby, $group){
                    $lists = $this->select($where, $field_data, $table, $limit, $orderby, $group);
                    return $lists;
                });
                $parallel->add(function () use($where, $table, $group){
                    $count = $this->getTotalNum($where, $table, $group);
                    return $count;
                });

                try{
                    // $results 结果为 [1, 2]
                    $results = $parallel->wait();
                    $lists = $results[0];
                    $count = $results[1];
                } catch(ParallelExecutionException $e){
                    // $e->getResults() 获取协程中的返回值。
                    // $e->getThrowables() 获取协程中出现的异常。
                }

                $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'debug');
                $logger->info('getListByOperators Request', [$this->getLastSql()]);
            }
            if($limit_num > 0 && $count > $limit_num){
                $count = $limit_num ;
            }
        }
        if(!empty($lists) && $datas['show_type'] == 2 && $datas['limit_num'] > 0 && !empty($order) && !empty($sort) && !empty($fields[$sort]) && !empty($fields[$datas['sort_target']]) && !empty($datas['sort_target']) && !empty($datas['sort_order'])){
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
        $targets_temp = $targets;//基础指标缓存

        //自定义指标
        $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
        $target_key_str = trim("'" . implode("','",explode(",",$datas['target'])) . "'");
        $custom_targets_list = $datas_ark_custom_target_md->getList("user_id = {$datas['user_id']} AND target_type IN(1, 2) AND target_key IN ({$target_key_str}) AND count_dimension = 1");
        $targets = $this->addCustomTargets($targets,$custom_targets_list);
        if (in_array('goods_visitors', $targets)) {  // 买家访问次数
            $fields['goods_visitors'] = 'SUM(report.byorder_user_sessions)';
        }
        if (in_array('goods_conversion_rate', $targets)) { //订单商品数量转化率
            $fields['goods_conversion_rate'] = 'sum( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)';
        }

        if (in_array('sale_sales_volume', $targets) || in_array('sale_refund_rate', $targets)  || in_array('cpc_direct_sales_volume_rate', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) { //销售量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_sales_volume'] = " sum( report.byorder_sales_volume +  report.byorder_group_id ) ";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_sales_volume'] = " sum( report.report_sales_volume +  report.report_group_id ) ";
            }
        }
        if (in_array('sale_many_channel_sales_volume', $targets)) { //多渠道数量
            if ($datas['sale_datas_origin'] == '1') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.byorder_group_id )";
            } elseif ($datas['sale_datas_origin'] == '2') {
                $fields['sale_many_channel_sales_volume'] = "sum( report.report_group_id )";
            }
        }
        if (in_array('sale_sales_quota', $targets) || in_array('cost_profit_profit_rate', $targets) || in_array('amazon_fee_rate', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('operate_fee_rate', $targets) || in_array('evaluation_fee_rate', $targets) || in_array('cpc_cost_rate', $targets) || in_array('cpc_turnover_rate', $targets)) {  //商品销售额
            if ($datas['sale_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['sale_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota )";
                } else {
                    $fields['sale_sales_quota'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('sale_return_goods_number', $targets) || in_array('sale_refund_rate', $targets)) {  //退款量
            if ($datas['refund_datas_origin'] == '1') {
                $fields['sale_return_goods_number'] = "sum(report.byorder_refund_num )";
            } elseif ($datas['refund_datas_origin'] == '2') {
                $fields['sale_return_goods_number'] = "sum(report.report_refund_num )";
            }
        }
        if (in_array('sale_refund', $targets)) {  //退款
            if ($datas['refund_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.byorder_refund )";
                } else {
                    $fields['sale_refund'] = "sum( (0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['refund_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['sale_refund'] = "sum( 0 - report.report_refund )";
                } else {
                    $fields['sale_refund'] = "sum( (0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('sale_refund_rate', $targets)) {  //退款率
            $fields['sale_refund_rate'] = $fields['sale_return_goods_number'] . " * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " ,0) ";
        }

        if (in_array('amazon_fee', $targets) || in_array('amazon_fee_rate', $targets)) {  //亚马逊费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END)';
                } else {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )';
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee +    report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee  ) END )';
                } else {
                    $fields['amazon_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN ((report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)))  ELSE (report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))   ) END)';
                }
            }

        }

        if (in_array('amazon_fee_rate', $targets)) {  //亚马逊费用占比
            $fields['amazon_fee_rate'] = '(' . $fields['amazon_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
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
                    $fields['amazon_sales_commission'] = "sum( report.byorder_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_sales_commission'] = "sum( report.report_platform_sales_commission ) ";
                } else {
                    $fields['amazon_sales_commission'] = "sum( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }

        if (in_array('amazon_fba_delivery_fee', $targets)) {  //FBA代发货费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                } else {
                    $fields['amazon_fba_delivery_fee'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_multi_channel_delivery_fee', $targets)) {  //多渠道配送费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit ) ";
                } else {
                    $fields['amazon_multi_channel_delivery_fee'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }

        if (in_array('amazon_settlement_fee', $targets)) {  //结算费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                } else {
                    $fields['amazon_settlement_fee'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                } else {
                    $fields['amazon_fba_return_processing_fee'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                }
            }
        }
        if (in_array('amazon_return_sale_commission', $targets)) {  //返还亚马逊销售佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission )";
                } else {
                    $fields['amazon_return_sale_commission'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_return_shipping_fee', $targets)) {  //返还运费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping )";
                } else {
                    $fields['amazon_return_shipping_fee'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            }
        }

        if (in_array('amazon_refund_deducted_commission', $targets)) {  //退款扣除佣金
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission )";
                } else {
                    $fields['amazon_refund_deducted_commission'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
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
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.report_channel_amazon_storage_fee) ELSE '.$estimated_monthly_storage_fee_field.' END )';
                } else {
                    $fields['amazon_stock_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE ('.$estimated_monthly_storage_fee_field.'* ({:RATE} / COALESCE(rates.rate ,1))) END )';
                }
            }
        }

        if (in_array('amazon_fba_monthly_storage_fee', $targets)) {  //FBA月仓储费
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END  )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE {$estimated_monthly_storage_fee_field}  END )";
                } else {
                    $fields['amazon_fba_monthly_storage_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE {$estimated_monthly_storage_fee_field} * ({:RATE} / COALESCE(rates.rate ,1)) END  )";
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
                    $fields['amazon_other_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee ) ELSE report.byorder_goods_amazon_other_fee END ) ";
                } else {
                    $fields['amazon_other_fee'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END   ) ";
                }
            } elseif ($datas['finance_datas_origin'] == '2') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['amazon_other_fee'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END   ) ";
                } else {
                    $fields['amazon_other_fee'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ) ";
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
                $fields['cpc_ad_fee'] = " sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END ) ";
            } else {
                $fields['cpc_ad_fee'] = " sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1))) END) ";
            }
        }
        if (in_array('cpc_cost_rate', $targets)) {  //CPC花费占比
            $fields['cpc_cost_rate'] = '(' . $fields['cpc_ad_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_exposure', $targets) || in_array('cpc_click_rate', $targets)) {  //CPC曝光量
            $fields['cpc_exposure'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3 )";
        }
        if (in_array('cpc_click_number', $targets) || in_array('cpc_click_rate', $targets) || in_array('cpc_click_conversion_rate', $targets) || in_array('cpc_avg_click_cost', $targets)) {  //CPC点击次数
            $fields['cpc_click_number'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )";
        }
        if (in_array('cpc_click_rate', $targets)) {  //CPC点击率
            $fields['cpc_click_rate'] = '('.$fields['cpc_click_number'].')' . " * 1.0000 / nullif( " . $fields['cpc_exposure'] . " , 0 ) ";
        }
        if (in_array('cpc_order_number', $targets) ||  in_array('cpc_click_conversion_rate', $targets)) {  //CPC订单数
            $fields['cpc_order_number'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
        }

        if (in_array('cpc_click_conversion_rate', $targets)) {  //cpc点击转化率
            $fields['cpc_click_conversion_rate'] = '('.$fields['cpc_order_number'] . ") * 1.0000 / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }

        if (in_array('cpc_turnover', $targets) || in_array('cpc_turnover_rate', $targets) || in_array('cpc_acos', $targets)) {  //CPC成交额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5" )';
            } else {
                $fields['cpc_turnover'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }

        if (in_array('cpc_turnover_rate', $targets)) {  //CPC成交额占比
            $fields['cpc_turnover_rate'] = '(' . $fields['cpc_turnover'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }
        if (in_array('cpc_avg_click_cost', $targets)) {  //CPC平均点击花费
            $fields['cpc_avg_click_cost'] = '('.$fields['cpc_ad_fee'] . ") * 1.0000 / nullif( " . $fields['cpc_click_number'] . " , 0 ) ";
        }
        if (in_array('cpc_acos', $targets)) {  // ACOS
            $fields['cpc_acos'] = '('.$fields['cpc_ad_fee'] . ") * 1.0000 / nullif( " . $fields['cpc_turnover'] . " , 0 ) ";
        }
        if (in_array('cpc_direct_sales_volume', $targets) || in_array('cpc_direct_sales_volume_rate', $targets)) {  //CPC直接销量
            $fields['cpc_direct_sales_volume'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  )';
        }
        if (in_array('cpc_direct_sales_quota', $targets)) {  //CPC直接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" )';
            } else {
                $fields['cpc_direct_sales_quota'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
            }
        }
        if (in_array('cpc_direct_sales_volume_rate', $targets)) {  // CPC直接销量占比
            $fields['cpc_direct_sales_volume_rate'] = '(' . $fields['cpc_direct_sales_volume'] . ") * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('cpc_indirect_sales_volume', $targets) || in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量
            $fields['cpc_indirect_sales_volume'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8) ';
        }

        if (in_array('cpc_indirect_sales_quota', $targets)) {  //CPC间接销售额
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report.bychannel_reserved_field5 - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report.bychannel_reserved_field6 )';
            } else {
                $fields['cpc_indirect_sales_quota'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_reserved_field5 * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report.bychannel_reserved_field6 * ({:RATE} / COALESCE(rates.rate ,1))   )';
            }
        }
        if (in_array('cpc_indirect_sales_volume_rate', $targets)) {  //CPC间接销量占比
            $fields['cpc_indirect_sales_volume_rate'] = '(' . $fields['cpc_indirect_sales_volume'] . ") * 1.0000 / nullif( " . $fields['sale_sales_volume'] . " , 0 ) ";
        }

        if (in_array('goods_adjust_fee', $targets)) { //商品调整费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE 0 END)';
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['goods_adjust_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END)';
                } else {
                    $fields['goods_adjust_fee'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE 0 END)';
                }
            }
        }

        if (in_array('evaluation_fee', $targets) || in_array('evaluation_fee_rate', $targets)) {  //测评费用
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }else{
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 ) ";
                } else {
                    $fields['evaluation_fee'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                }
            }
        }
        if (in_array('evaluation_fee_rate', $targets)) {  //测评费用占比
            $fields['evaluation_fee_rate'] = '(' . $fields['evaluation_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }


        if (in_array('purchase_logistics_purchase_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets) || in_array('cost_profit_profit_rate', $targets)) {  //采购成本
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    }
                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum((report.first_purchasing_cost) ) ";
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_purchase_cost'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_purchase_cost'] = " sum( ( report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ) ";
                    }
                }
            }

        }

        if (in_array('purchase_logistics_logistics_cost', $targets) || in_array('purchase_logistics_cost_rate', $targets) || in_array('cost_profit_profit', $targets)  || in_array('cost_profit_profit_rate', $targets)) {  // 物流/头程
            if ($datas['finance_datas_origin'] == 1) {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum(  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            } else {
                if ($datas['currency_code'] == 'ORIGIN') {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum(  (report.first_logistics_head_course) ) ";
                    }

                } else {
                    if ($datas['cost_count_type'] == '1') {
                        $fields['purchase_logistics_logistics_cost'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                    } else {
                        $fields['purchase_logistics_logistics_cost'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )) ";
                    }

                }
            }
        }

        if (in_array('purchase_logistics_cost_rate', $targets)) {  // 成本/物流费用占比
            $fields['purchase_logistics_cost_rate'] = '(' . $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        if (in_array('operate_fee', $targets) || in_array('operate_fee_rate', $targets)) {  //运营费用
            if ($datas['currency_code'] == 'ORIGIN') {
                $fields['operate_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 1 THEN  (0- report.byorder_reserved_field16 ) ELSE report.bychannel_operating_fee END) ";
            } else {
                $fields['operate_fee'] = "sum( CASE WHEN report.goods_operation_pattern = 1 THEN  (0 -  report.byorder_reserved_field16) * ({:RATE} / COALESCE(rates.rate ,1)) ELSE report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) END) ";
            }
        }
        if (in_array('operate_fee_rate', $targets)) {  //运营费用占比
            $fields['operate_fee_rate'] = '(' . $fields['operate_fee'] . ") * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
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
            $repair_data = '' ;
            if ($datas['finance_datas_origin'] == '1') {
                if ($datas['sale_datas_origin'] == '2') {
                    $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                }
                if ($datas['refund_datas_origin'] == '2') {
                    $repair_data .=  " + report.byorder_refund - report.report_refund ";
                }
            }else{
                if ($datas['sale_datas_origin'] == '1') {
                    $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                }
                if ($datas['refund_datas_origin'] == '1') {
                    $repair_data .=  " + report.report_refund - report.byorder_refund ";
                }
            }
            $estimated_monthly_storage_fee_field = "";
            if ($datas['is_month_table'] == 1){
                $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
            }

            $purchase_logistics = $fields['purchase_logistics_purchase_cost'] . ' + ' . $fields['purchase_logistics_logistics_cost'];
            if(empty($repair_data)){
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.byorder_goods_profit ) END )+ $purchase_logistics)";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) END  ) + $purchase_logistics) ";
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.report_goods_profit {$estimated_monthly_storage_fee_field}) END  ) + $purchase_logistics)";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE ((report.report_goods_profit {$estimated_monthly_storage_fee_field}) * ({:RATE} / COALESCE(rates.rate ,1)) ) END )+$purchase_logistics) ";
                    }
                }
            }else{
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.byorder_goods_profit ) END )+ $purchase_logistics + SUM( (0 {$repair_data})))";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) END  ) + $purchase_logistics + SUM( (0 {$repair_data}) * ({:RATE} / COALESCE(rates.rate ,1))))";
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['cost_profit_profit'] = "(SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) ) ELSE (report.report_goods_profit{$estimated_monthly_storage_fee_field} ) END  ) + $purchase_logistics + SUM( (0 {$repair_data})))";
                    } else {
                        $fields['cost_profit_profit'] = "(SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE ((report.report_goods_profit{$estimated_monthly_storage_fee_field}) * ({:RATE} / COALESCE(rates.rate ,1)) ) END )+$purchase_logistics  + SUM( (0 {$repair_data}) * ({:RATE} / COALESCE(rates.rate ,1))))";
                    }
                }
            }
        }
        if (in_array('cost_profit_profit_rate', $targets)) {  //毛利率
            $fields['cost_profit_profit_rate'] = "(" . $fields['cost_profit_profit'] . ")" . " * 1.0000 / nullif( " . $fields['sale_sales_quota'] . " , 0 ) ";
        }

        $this->getUnTimeFields($fields,$datas,$targets,3);

        //加入自定义指标
        $this->getCustomTargetFields($fields,$custom_targets_list,$targets,$targets_temp, $datas);
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
        $target_key = $datas['time_target'];
        //自定义指标
        $datas_ark_custom_target_md = new DatasArkCustomTargetMySQLModel([], $this->dbhost, $this->codeno);
        $custom_target = $datas_ark_custom_target_md->get_one("user_id = {$datas['user_id']} AND target_type IN(1, 2) AND target_key = '{$target_key}' AND status = 1 AND count_dimension = 1");
        $keys = [];
        $new_target_keys = [];
        if($custom_target && $custom_target['target_type'] == 2){
            $time_targets = explode(",",$custom_target['formula_fields']);
            //公式所涉及到的新增指标
            $target_key_str = trim("'" . implode("','",$time_targets) . "'");
            $new_target = $datas_ark_custom_target_md->getList("user_id = {$datas['user_id']} AND target_type = 1 AND count_dimension IN (1,2) AND target_key IN ($target_key_str)","target_key,month_goods_field,format_type");
            $keys = array_column($new_target,'target_key');
            $new_target_keys = array_column($new_target,null,'target_key');
        }else{
            $time_targets = array($target_key);
        }
        $time_fields = array();
        $time_fields_arr = array();
        foreach ($time_targets as $time_target) {
            if ($time_target == 'goods_visitors') {  // 买家访问次数
                $fields['count_total'] = "SUM(report.byorder_user_sessions)";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_user_sessions');
            } else if ($time_target == 'goods_conversion_rate') { //订单商品数量转化率
                $fields['count_total'] = 'sum( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif(sum( report.byorder_user_sessions ) ,0)';
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_quantity_of_goods_ordered', 'report.byorder_user_sessions');
            } else if ($time_target == 'sale_sales_volume') { //销售量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = " sum( report.byorder_sales_volume  +  report.byorder_group_id) ";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_volume  +  report.byorder_group_id");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = " sum( report.report_sales_volume  +  report.byorder_group_id ) ";
                    $time_fields = $this->getTimeFields($time_line, "report.report_sales_volume  +  report.byorder_group_id");
                }
            } else if ($time_target == 'sale_many_channel_sales_volume') { //多渠道数量
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "sum( report.byorder_group_id )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_group_id");
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = "sum( report.report_group_id )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_group_id");
                }
            } else if ($time_target == 'sale_sales_quota') {  //商品销售额
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_sales_quota )";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                } elseif ($datas['sale_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_sales_quota )";
                        $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota");
                    } else {
                        $fields['count_total'] = "sum( report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))");
                    }
                }
            } else if ($time_target == 'sale_return_goods_number') {  //退款量
                if ($datas['refund_datas_origin'] == '1') {
                    $fields['count_total'] = "sum(report.byorder_refund_num )";
                    $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num");
                } elseif ($datas['refund_datas_origin'] == '2') {
                    $fields['count_total'] = "sum(report.report_refund_num )";
                    $time_fields = $this->getTimeFields($time_line, "report.report_refund_num");
                }
            } else if ($time_target == 'sale_refund') {  //退款
                if ($datas['refund_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( 0 - report.byorder_refund )";
                        $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund )");
                    } else {
                        $fields['count_total'] = "sum( ( 0 - report.byorder_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, "( 0 - report.byorder_refund ) * ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                } elseif ($datas['refund_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( 0 - report.report_refund )";
                        $time_fields = $this->getTimeFields($time_line, " ( 0 - report.report_refund ) ");
                    } else {
                        $fields['count_total'] = "sum( ( 0 - report.report_refund) * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, " (0 - report.report_refund )* ({:RATE} / COALESCE(rates.rate ,1)) ");
                    }
                }
            } else if ($time_target == 'sale_refund_rate') {  //退款率
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['refund_datas_origin'] == '1') {
                        $fields['count_total'] = "sum(report.byorder_refund_num ) * 1.0000 / nullif(SUM(report.byorder_sales_volume + report.byorder_group_id),0)";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0000", "report.byorder_sales_volume+ report.byorder_group_id");
                    } elseif ($datas['refund_datas_origin'] == '2') {
                        $fields['count_total'] = "sum(report.report_refund_num  ) * 1.0000 / nullif(SUM(report.byorder_sales_volume+ report.byorder_group_id),0)";
                        $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0000 ", "report.byorder_sales_volume+ report.byorder_group_id");
                    }
                } else {
                    if ($datas['refund_datas_origin'] == '1') {
                        $fields['count_total'] = "sum(report.byorder_refund_num ) * 1.0000  / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                        $time_fields = $this->getTimeFields($time_line, "report.byorder_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                    } elseif ($datas['refund_datas_origin'] == '2') {
                        $fields['count_total'] = "sum(report.report_refund_num ) * 1.0000 / nullif(SUM(report.report_sales_volume+ report.report_group_id),0)";
                        $time_fields = $this->getTimeFields($time_line, "report.report_refund_num * 1.0 ", "report.report_sales_volume+ report.report_group_id");
                    }
                }
            } else if ($time_target == 'amazon_fee') {  //亚马逊费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )');
                    } else {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE  (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ) ';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE  (report.byorder_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    $estimated_monthly_storage_fee_field = "";
                    if ($datas['is_month_table'] == 1){
                        $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                    }
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee) END )');
                    } else {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN ((report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (  report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN ((report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.') * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE (  report.report_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_order_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_refund_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))  + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END )');
                    }
                }
            } else if ($time_target == 'amazon_fee_rate') {  //亚马逊费用占比
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END ) * 1.0000 / nullif( sum(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))) , 0 ) ';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END )', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');

                    } elseif ($datas['finance_datas_origin'] == '2') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.')* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END ) * 1.0000 / nullif( sum(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))) , 0 ) ';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.')* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END )', 'report.byorder_sales_quota');
                    }
                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee * ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END ) * 1.0000 / nullif( sum(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))) , 0 ) ';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN report.byorder_goods_amazon_fee* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.byorder_channel_amazon_order_fee + report.byorder_channel_amazon_refund_fee + report.byorder_channel_amazon_storage_fee + report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END )', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');

                    } elseif ($datas['finance_datas_origin'] == '2') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.')* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END ) * 1.0000 / nullif( sum(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))) , 0 ) ';
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 1 THEN (report.report_goods_amazon_fee'.$estimated_monthly_storage_fee_field.')* ({:RATE} / COALESCE(rates.rate ,1)) ELSE ( report.report_channel_amazon_order_fee + report.report_channel_amazon_refund_fee + report.report_channel_amazon_storage_fee + report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_order_fee + report.bychannel_channel_amazon_refund_fee + report.bychannel_channel_amazon_storage_fee + report.bychannel_channel_amazon_other_fee)* ({:RATE} / COALESCE(rates.rate ,1)) END )', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_order_fee') {  //亚马逊-订单费用
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
            } else if ($time_target == 'amazon_sales_commission') {  //亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_platform_sales_commission ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_platform_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_platform_sales_commission ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_platform_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_platform_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_fba_delivery_fee') {  //FBA代发货费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.byorder_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) -report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit)";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_fba_generation_delivery_cost + report.report_fbaperorderfulfillmentfee + report.report_fbaweightbasedfee - report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_generation_delivery_cost * ({:RATE} / COALESCE(rates.rate ,1))+ report.report_fbaperorderfulfillmentfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbaweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)) - report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }

            } else if ($time_target == 'amazon_multi_channel_delivery_fee') {  //多渠道配送费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_profit ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_profit');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_profit ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_profit');
                    } else {
                        $fields['count_total'] = "sum( report.report_profit * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_profit * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_settlement_fee') {  //结算费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee + report.report_fixedclosingfee + report.report_refund_variableclosingfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_order_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fixedclosingfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_refund_variableclosingfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_refund_fee') { //亚马逊-退货退款费用
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
            } else if ($time_target == 'amazon_fba_return_processing_fee') {  //FBA退货处理费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee+report.byorder_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee + report.report_fbacustomerreturnperorderfee + report.report_fbacustomerreturnweightbasedfee');
                    } else {
                        $fields['count_total'] = "sum( report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_fba_refund_treatment_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnperorderfee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_fbacustomerreturnweightbasedfee * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_return_sale_commission') {  //返还亚马逊销售佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_sales_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'amazon_return_shipping_fee') {  //返还运费
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_returnshipping )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_returnshipping )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_returnshipping ');
                    } else {
                        $fields['count_total'] = "sum( report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_returnshipping * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }
            } else if ($time_target == 'amazon_refund_deducted_commission') {  //退款扣除佣金
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission');
                    } else {
                        $fields['count_total'] = "sum( report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1)) )";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_return_and_return_commission * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } elseif ($time_target == 'amazon_stock_fee') { //亚马逊-库存费用
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.byorder_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END )';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.byorder_channel_amazon_storage_fee) ELSE report.byorder_estimated_monthly_storage_fee END');
                    } else {
                        $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE}) END )';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee / COALESCE(rates.rate ,1) * {:RATE}) END ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee+report.report_channel_amazon_storage_fee) ELSE '.$estimated_monthly_storage_fee_field.' END )';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee + report.report_channel_amazon_storage_fee) ELSE '.$estimated_monthly_storage_fee_field.' END');
                    } else {
                        $fields['count_total'] = 'SUM( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE ('.$estimated_monthly_storage_fee_field.' / COALESCE(rates.rate ,1) * {:RATE}) END )';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_channel_amazon_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE ('.$estimated_monthly_storage_fee_field.' / COALESCE(rates.rate ,1) * {:RATE}) END');
                    }
                }
            } else if ($time_target == 'amazon_fba_monthly_storage_fee') {  //FBA月仓储费
                $estimated_monthly_storage_fee_field = "report.report_estimated_monthly_storage_fee";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END  )";
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE report.byorder_estimated_monthly_storage_fee END');
                    } else {
                        $fields['count_total'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_estimated_monthly_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) END ');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE {$estimated_monthly_storage_fee_field} END  )";
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN report.bychannel_fba_storage_fee ELSE '.$estimated_monthly_storage_fee_field.' END');
                    } else {
                        $fields['count_total'] = "sum( CASE  WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE ({$estimated_monthly_storage_fee_field} * ({:RATE} / COALESCE(rates.rate ,1))) END  )";
                        $time_fields = $this->getTimeFields($time_line, 'CASE  WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_fba_storage_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE ('.$estimated_monthly_storage_fee_field.' * ({:RATE} / COALESCE(rates.rate ,1))) END ');
                    }
                }
            } elseif ($time_target == 'amazon_long_term_storage_fee') { //FBA长期仓储费
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
            } else if ($time_target == 'amazon_other_fee') {  //其他亚马逊费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.byorder_goods_amazon_other_fee END ) ";
                        $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.byorder_goods_amazon_other_fee END )');
                    } else {
                        $fields['count_total'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  ) ";
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )');
                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END ) ";
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee + report.bychannel_channel_amazon_other_fee) ELSE report.report_goods_amazon_other_fee END )');
                    } else {
                        $fields['count_total'] = "sum(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  ) ";
                        $time_fields = $this->getTimeFields($time_line, '(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.report_goods_amazon_other_fee * ({:RATE} / COALESCE(rates.rate ,1))) END  )');
                    }
                }
            } else if ($time_target == 'promote_discount') {  //promote折扣
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
            } else if ($time_target == 'promote_refund_discount') {  //退款返还promote折扣
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
            } else if ($time_target == 'promote_store_fee') { //店铺促销费用
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
            } else if ($time_target == 'promote_coupon') { //coupon优惠券
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax")';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_coupon_redemption_fee + report."bychannel_coupon_payment_eventList_tax"');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_coupon_redemption_fee * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_coupon_payment_eventList_tax" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } elseif ($time_target == 'promote_run_lightning_deal_fee') {  //RunLightningDealFee';
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee)';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_run_lightning_deal_fee');
                } else {
                    $fields['count_total'] = 'SUM(report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) )';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_run_lightning_deal_fee * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_ad_fee') {  //广告费用
                if ($datas['currency_code'] == 'ORIGIN') {//由于byorder_cpc_cost 和report_cpc_cost 实际一样，所以不用区分
                    $fields['count_total'] = " sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END ) ";
                    $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END )');
                } else {
                    $fields['count_total'] = " sum( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) END) ";
                    $time_fields = $this->getTimeFields($time_line, '( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))) ELSE (report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) END) ');
                }
            } else if ($time_target == 'cpc_cost_rate') {  //CPC花费占比
                $fields_tmp = "( CASE WHEN report.goods_operation_pattern = 2 THEN (report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund) ELSE (report.byorder_cpc_cost + report.byorder_cpc_sd_cost) END ) * ({:RATE} / COALESCE(rates.rate ,1)) ";
                if ($datas['sale_datas_origin'] == '1') {

                    $fields_denominator = "report.byorder_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))";

                } else {

                    $fields_denominator = "report.report_sales_quota * ({:RATE} / COALESCE(rates.rate ,1))";
                }
                $fields['count_total'] = "SUM(  $fields_tmp )  * 1.0000 / nullif(SUM($fields_denominator),0)";
                $time_fields = $this->getTimeFields($time_line, $fields_tmp, $fields_denominator);
            } else if ($time_target == 'cpc_exposure') {  //CPC曝光量
                $fields['count_total'] = "sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3 )";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');
            } else if ($time_target == 'cpc_click_number') {  //CPC点击次数
                $fields['count_total'] = "sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            } else if ($time_target == 'cpc_click_rate') {  //CPC点击率
                $fields['count_total'] = "(SUM( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 )) * 1.0000 / nullif( SUM(report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3), 0 ) ";
                $time_fields = $this->getTimeFields($time_line, 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4', 'report.byorder_reserved_field1 + report.byorder_reserved_field2 + report.bychannel_reserved_field3');

            } else if ($time_target == 'cpc_order_number') {  //CPC订单数
                $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ) ';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7');
            } else if ($time_target == 'cpc_click_conversion_rate') {  //cpc点击转化率
                $fields['count_total'] = '(sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 ))  * 1.0000 / nullif (SUM(report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4) , 0 )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
            } else if ($time_target == 'cpc_turnover') {  //CPC成交额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_turnover_rate') {  //CPC成交额占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum(  (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5")* ({:RATE} / COALESCE(rates.rate ,1)) ) * 1.0000 / nullif( SUM(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)';
                    $time_fields = $this->getTimeFields($time_line, ' (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5") * ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                } else {
                    $fields['count_total'] = 'sum(  (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5") * ({:RATE} / COALESCE(rates.rate ,1))) * 1.0000 / nullif( SUM(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)';
                    $time_fields = $this->getTimeFields($time_line, '  (report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5") * ({:RATE} / COALESCE(rates.rate ,1))', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'cpc_avg_click_cost') {  //CPC平均点击花费
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
                } else {
                    $fields['count_total'] = 'sum(report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))  +  report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) * 1.0000 / nullif(sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4 ),0) ';
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_product_ads_payment_eventlist_refund * ({:RATE} / COALESCE(rates.rate ,1))  +  report.byorder_cpc_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_cpc_sd_cost * ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks + report.bychannel_reserved_field4');
                }

            } else if ($time_target == 'cpc_acos') {  // ACOS
                $fields['count_total'] = 'SUM(report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost  ) * 1.0000 / nullif( sum( report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5"  ) , 0 ) ';
                $time_fields = $this->getTimeFields($time_line, 'report.bychannel_product_ads_payment_eventlist_charge + report.bychannel_product_ads_payment_eventlist_refund + report.byorder_cpc_cost + report.byorder_cpc_sd_cost ', 'report."byorder_sp_attributedSales7d" + report."byorder_sd_attributedSales7d" +  report."bychannel_reserved_field5" ');

            } else if ($time_target == 'cpc_direct_sales_volume') {  //CPC直接销量
                $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )';
                $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ');
            } else if ($time_target == 'cpc_direct_sales_quota') {  //CPC直接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6"  )';
                    $time_fields = $this->getTimeFields($time_line, '  report."byorder_sd_attributedSales7dSameSKU" + report."byorder_sp_attributedSales7dSameSKU" + report."bychannel_reserved_field6" ');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_direct_sales_volume_rate') {  // CPC直接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8  )  * 1.0000 / nullif(sum( report.byorder_sales_volume+ report.byorder_group_id ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.byorder_sales_volume+ report.byorder_group_id');
                } elseif ($datas['sale_datas_origin'] == '2') {
                    $fields['count_total'] = 'sum( report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 )  * 1.0000 / nullif(sum( report.report_sales_volume + report.report_group_id  ) ,0)';
                    $time_fields = $this->getTimeFields($time_line, ' report."byorder_sd_attributedConversions7dSameSKU" + report."byorder_sp_attributedConversions7dSameSKU" + report.bychannel_reserved_field8 ', 'report.report_sales_volume+ report.report_group_id');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume') {  //CPC间接销量
                $fields['count_total'] = ' SUM(report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8 )';
                $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report.bychannel_reserved_field7 - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report.bychannel_reserved_field8');
            } else if ($time_target == 'cpc_indirect_sales_quota') {  //CPC间接销售额
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5" - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6" )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" + report."byorder_sp_attributedSales7d" + report."bychannel_reserved_field5"  - report."byorder_sd_attributedSales7dSameSKU" - report."byorder_sp_attributedSales7dSameSKU" - report."bychannel_reserved_field6"');
                } else {
                    $fields['count_total'] = 'sum(report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1))  )';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sd_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1))  + report."byorder_sp_attributedSales7d" * ({:RATE} / COALESCE(rates.rate ,1)) + report."bychannel_reserved_field5" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sd_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1))  - report."byorder_sp_attributedSales7dSameSKU" * ({:RATE} / COALESCE(rates.rate ,1)) - report."bychannel_reserved_field6" * ({:RATE} / COALESCE(rates.rate ,1)) ');
                }
            } else if ($time_target == 'cpc_indirect_sales_volume_rate') {  //CPC间接销量占比
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" )  * 1.0000 / nullif(sum( report.byorder_sales_volume + report.byorder_group_id) ,0)';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" + report."bychannel_reserved_field7" - report."byorder_sd_attributedConversions7dSameSKU" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.byorder_sales_volume + report.byorder_group_id');
                } else {
                    $fields['count_total'] = 'sum( report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8" )  * 1.0000 / nullif(sum( report.report_sales_volume + report.report_group_id) ,0)';
                    $time_fields = $this->getTimeFields($time_line, 'report."byorder_sp_attributedConversions7d" + report."byorder_sd_attributedConversions7d" - report."byorder_sd_attributedConversions7dSameSKU" + report."bychannel_reserved_field7" - report."byorder_sp_attributedConversions7dSameSKU" - report."bychannel_reserved_field8"', 'report.report_sales_volume+ report.report_group_id');
                }
            } else if ($time_target == 'goods_adjust_fee') { //商品调整费用

                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END)';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END');
                    } else {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE 0 END)';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE 0 END');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END)';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee + report.bychannel_channel_goods_adjustment_fee) ELSE 0 END');
                    } else {
                        $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1)) ) ELSE 0 END)';
                        $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_goods_adjustment_fee  * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_channel_goods_adjustment_fee * ({:RATE} / COALESCE(rates.rate ,1))) ELSE 0 END');
                    }
                }

            } else if ($time_target == 'evaluation_fee') {  //测评费用
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.byorder_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "sum( report.report_reserved_field10 ) ";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10');
                    } else {
                        $fields['count_total'] = "sum( report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' report.report_reserved_field10 * ({:RATE} / COALESCE(rates.rate ,1)) ');
                    }
                }

            } else if ($time_target == 'evaluation_fee_rate') {  //测评费用占比
                if ($datas['sale_datas_origin'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1)) )  * 1.0000 / nullif(SUM(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1)) )  * 1.0000 / nullif(SUM(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    }

                } else {
                    if ($datas['finance_datas_origin'] == '1') {
                        $fields['count_total'] = "SUM(report.byorder_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1)) )  * 1.0000 / nullif(SUM(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.byorder_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1))', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    } else {
                        $fields['count_total'] = "SUM(report.report_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1)) )  * 1.0000 / nullif(SUM(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)";
                        $time_fields = $this->getTimeFields($time_line, 'report.report_reserved_field10* ({:RATE} / COALESCE(rates.rate ,1))', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    }

                }
            } else if ($time_target == 'purchase_logistics_purchase_cost') {  //采购成本
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.byorder_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.byorder_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_purchasing_cost ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.report_purchasing_cost');
                        } else {
                            $fields['count_total'] = " sum( report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ');
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(  (report.first_purchasing_cost) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.first_purchasing_cost)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1))) ');
                    }
                }

            } else if ($time_target == 'purchase_logistics_logistics_cost') {  // 物流/头程
                if ($datas['cost_count_type'] == '1') {
                    if ($datas['finance_datas_origin'] == '1') {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.byorder_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        if ($datas['currency_code'] == 'ORIGIN') {
                            $fields['count_total'] = " sum( report.report_logistics_head_course ) ";
                            $time_fields = $this->getTimeFields($time_line, ' report.report_logistics_head_course');
                        } else {
                            $fields['count_total'] = " sum( report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ";
                            $time_fields = $this->getTimeFields($time_line, 'report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = " sum(( report.first_logistics_head_course) ) ";
                        $time_fields = $this->getTimeFields($time_line, ' (report.first_logistics_head_course)');
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) ) ) ";
                        $time_fields = $this->getTimeFields($time_line, '(report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1)) )');
                    }
                }
            } else if ($time_target == 'purchase_logistics_cost_rate') {  // 成本/物流费用占比
                if ($datas['sale_datas_origin'] == 1) {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1)) ) * 1.0000 / nullif(sum( report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)) ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                        } else {
                            $fields['count_total'] = " sum( (report.report_logistics_head_course + report.report_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))  ) * 1.0000 / nullif(sum( report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)) ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_logistics_head_course + report.report_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))) * 1.0000 / nullif(sum( report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)) ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))', 'report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['cost_count_type'] == '1') {
                        if ($datas['finance_datas_origin'] == '1') {
                            $fields['count_total'] = " sum( (report.byorder_logistics_head_course + report.byorder_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1)) ) * 1.0000 / nullif(sum( report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)) ),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.byorder_logistics_head_course  + report.byorder_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1)) ', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                        } else {
                            $fields['count_total'] = " sum( (report.report_logistics_head_course + report.report_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))  ) * 1.0000 / nullif(sum( report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))),0)  ";
                            $time_fields = $this->getTimeFields($time_line, ' (report.report_logistics_head_course + report.report_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                        }
                    } else {
                        $fields['count_total'] = " sum( (report.first_logistics_head_course + report.first_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))) * 1.0000 / nullif(sum( report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)) ),0)  ";
                        $time_fields = $this->getTimeFields($time_line, '( report.first_logistics_head_course + report.first_purchasing_cost)* ({:RATE} / COALESCE(rates.rate ,1))', 'report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }
            } else if ($time_target == 'operate_fee') {  //运营费用
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "sum(CASE WHEN report.goods_operation_pattern = 1 THEN (0- report.byorder_reserved_field16)  ELSE report.bychannel_operating_fee END) ";
                    $time_fields = $this->getTimeFields($time_line, 'CASE WHEN report.goods_operation_pattern = 1 THEN (0 - report.byorder_reserved_field16) ELSE report.bychannel_operating_fee END');
                } else {
                    $fields['count_total'] = "sum(CASE WHEN report.goods_operation_pattern = 1 THEN (0 -  report.byorder_reserved_field16 )* ({:RATE} / COALESCE(rates.rate ,1)) ELSE report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) END) ";
                    $time_fields = $this->getTimeFields($time_line, ' CASE WHEN report.goods_operation_pattern = 1 THEN (0 -  report.byorder_reserved_field16 )* ({:RATE} / COALESCE(rates.rate ,1)) ELSE report.bychannel_operating_fee * ({:RATE} / COALESCE(rates.rate ,1)) END ');
                }
            } else if ($time_target == 'operate_fee_rate') {  //运营费用占比
                $rate_fields = $datas['currency_code'] == 'ORIGIN' ? " * 1.0000" : " * ({:RATE} / COALESCE(rates.rate ,1))";
                if ($datas['sale_datas_origin'] == '1') {
                    $fields['count_total'] = "SUM( CASE WHEN report.goods_operation_pattern = 1 THEN (0 - report.byorder_reserved_field16 ) {$rate_fields} ElSE (report.bychannel_operating_fee) {$rate_fields} END)  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, "(CASE WHEN report.goods_operation_pattern = 1 THEN (0-report.byorder_reserved_field16) {$rate_fields} ELSE report.bychannel_operating_fee {$rate_fields} END) * 1.0000" , 'report.byorder_sales_quota' . $rate_fields);
                } else {
                    $fields['count_total'] = "SUM( CASE WHEN report.goods_operation_pattern = 1 THEN (0 - report.byorder_reserved_field16 ) {$rate_fields} ElSE (report.bychannel_operating_fee {$rate_fields}) END)  * 1.0000 / nullif(SUM(report.byorder_sales_quota {$rate_fields}),0)";
                    $time_fields = $this->getTimeFields($time_line, "(CASE WHEN report.goods_operation_pattern = 1 THEN (0-report.byorder_reserved_field16) {$rate_fields} ELSE report.bychannel_operating_fee {$rate_fields} END) * 1.0000", 'report.report_sales_quota' . $rate_fields );
                }
            } else if ($time_target == 'other_vat_fee') {//VAT
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.byorder_reserved_field17)";
                        $time_fields = $this->getTimeFields($time_line, '0-report.byorder_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, '(0-report.byorder_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                } else {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        $fields['count_total'] = "SUM(0-report.report_reserved_field17)";
                        $time_fields = $this->getTimeFields($time_line, '0-report.report_reserved_field17');
                    } else {
                        $fields['count_total'] = "SUM((0-report.report_reserved_field17) * ({:RATE} / COALESCE(rates.rate ,1)))";
                        $time_fields = $this->getTimeFields($time_line, '(0-report.report_reserved_field17 )* ({:RATE} / COALESCE(rates.rate ,1))');
                    }
                }

            } else if ($time_target == 'other_other_fee') { //其他
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.bychannel_loan_payment +  report.bychannel_review_enrollment_fee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_loan_payment + report.bychannel_review_enrollment_fee');
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1)) )";
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_loan_payment * ({:RATE} / COALESCE(rates.rate ,1)) +  report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'other_review_enrollment_fee') { //早期评论者计划
                if ($datas['currency_code'] == 'ORIGIN') {
                    $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee)";
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_review_enrollment_fee');
                } else {
                    $fields['count_total'] = "SUM(report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))  )";
                    $time_fields = $this->getTimeFields($time_line, 'report.bychannel_review_enrollment_fee * ({:RATE} / COALESCE(rates.rate ,1))');
                }
            } else if ($time_target == 'cost_profit_profit') {  //毛利润
                $repair_data = '';
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '2') {
                        $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '2') {
                        $repair_data .= " + report.byorder_refund - report.report_refund ";
                    }
                } else {
                    if ($datas['sale_datas_origin'] == '1') {
                        $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '1') {
                        $repair_data .= " + report.report_refund - report.byorder_refund ";
                    }
                }
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        if ($datas['cost_count_type'] == '1') {
                            $fields['count_total'] = 'SUM(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) + COALESCE(report.byorder_purchasing_cost,0) + COALESCE(report.byorder_logistics_head_course,0)) ELSE (report.byorder_goods_profit+ report.byorder_purchasing_cost + report.byorder_logistics_head_course  END ) + SUM(0 ' . $repair_data . ')';
                            $time_fields = $this->getTimeFields($time_line, ' (CASE WHEN report.goods_operation_pattern = 2 THEN (report.byorder_channel_profit + report.bychannel_channel_profit + report.byorder_purchasing_cost + report.byorder_logistics_head_course) ELSE (report.byorder_goods_profit+ report.byorder_purchasing_cost + report.byorder_logistics_head_course  END ) ' . $repair_data);
                        } else {
                            $fields['count_total'] = 'SUM(  CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.first_purchasing_cost,0) + COALESCE(report.first_logistics_head_course,0)  +COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0)) ELSE (report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit) END  ) + SUM(0 ' . $repair_data . ')';
                            $time_fields = $this->getTimeFields($time_line, ' (  CASE WHEN report.goods_operation_pattern = 2 THEN (report.first_purchasing_cost + report.first_logistics_head_course  +report.byorder_channel_profit + report.bychannel_channel_profit) ELSE (report.first_purchasing_cost + report.first_logistics_head_course  + report.byorder_goods_profit) END  )' . $repair_data);
                        }
                    } else {
                        if ($datas['cost_count_type'] == '1') {
                            $purchasing_logistics = "COALESCE(report.byorder_purchasing_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) + report.byorder_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                        } else {
                            $purchasing_logistics = "COALESCE(report.first_purchasing_cost,0) * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                        }
                        $fields_tmp = "CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + COALESCE(report.bychannel_channel_profit,0) * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics ) ELSE (report.byorder_goods_profit * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics ) END + (0 " . $repair_data . ") * ({:RATE} / COALESCE(rates.rate ,1))";

                        $fields['count_total'] = "SUM( $fields_tmp )";
                        $time_fields = $this->getTimeFields($time_line, "( $fields_tmp )");

                    }
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['currency_code'] == 'ORIGIN') {
                        if ($datas['cost_count_type'] == '1') {
                            $purchasing_logistics = "report.report_purchasing_cost  + report.report_logistics_head_course";
                        } else {
                            $purchasing_logistics = "report.first_purchasing_cost  + report.first_logistics_head_course";
                        }
                        $fields_tmp = "CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_profit + report.bychannel_channel_profit + $purchasing_logistics) ELSE (report.report_goods_profit + $purchasing_logistics {$estimated_monthly_storage_fee_field} ) END {$repair_data}";

                    } else {
                        if ($datas['cost_count_type'] == '1') {
                            $purchasing_logistics = "report.report_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.report_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";
                        } else {
                            $purchasing_logistics = "report.first_purchasing_cost * ({:RATE} / COALESCE(rates.rate ,1)) + report.first_logistics_head_course * ({:RATE} / COALESCE(rates.rate ,1))";

                        }
                        $fields_tmp = "CASE WHEN report.goods_operation_pattern = 2 THEN (report.report_channel_profit * ({:RATE} / COALESCE(rates.rate ,1)) + report.bychannel_channel_profit * ({:RATE} / COALESCE(rates.rate ,1))  + $purchasing_logistics) ELSE ( (report.report_goods_profit{$estimated_monthly_storage_fee_field}) * ({:RATE} / COALESCE(rates.rate ,1)) + $purchasing_logistics) END + ( 0 {$repair_data}) * ({:RATE} / COALESCE(rates.rate ,1)) ";

                    }
                    $fields['count_total'] = "SUM( $fields_tmp )";
                    $time_fields = $this->getTimeFields($time_line, "( $fields_tmp )");
                }

            } else if ($time_target == 'cost_profit_profit_rate') {  //毛利率
                $repair_data = '';
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['sale_datas_origin'] == '2') {
                        $repair_data .= " + report.report_sales_quota - report.byorder_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '2') {
                        $repair_data .= " + report.byorder_refund - report.report_refund ";
                    }
                } else {
                    if ($datas['sale_datas_origin'] == '1') {
                        $repair_data .= " + report.byorder_sales_quota - report.report_sales_quota  ";
                    }
                    if ($datas['refund_datas_origin'] == '1') {
                        $repair_data .= " + report.report_refund - report.byorder_refund ";
                    }
                }
                $estimated_monthly_storage_fee_field = "";
                if ($datas['is_month_table'] == 1){
                    $estimated_monthly_storage_fee_field = " - report.report_estimated_monthly_storage_fee + report.monthly_sku_estimated_monthly_storage_fee";
                }
                if ($datas['sale_datas_origin'] == 1) {
                    $fields_denominator = '(report.byorder_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)))';
                } else {
                    $fields_denominator = '(report.report_sales_quota* ({:RATE} / COALESCE(rates.rate ,1)))';
                }
                if ($datas['finance_datas_origin'] == '1') {
                    if ($datas['cost_count_type'] == '1') {
                        $purchasing_logistics = "report.byorder_purchasing_cost + report.byorder_logistics_head_course ";
                    } else {
                        $purchasing_logistics = "report.first_purchasing_cost  + report.first_logistics_head_course ";
                    }
                    $fields_tmp = "(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.byorder_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) + {$purchasing_logistics}) ELSE (report.byorder_goods_profit+ {$purchasing_logistics})  END $repair_data) * ({:RATE} / COALESCE(rates.rate ,1)) ";
                    $fields['count_total'] = "(SUM($fields_tmp) * 1.0000 / nullif(sum($fields_denominator),0))";
                    $time_fields = $this->getTimeFields($time_line, $fields_tmp, $fields_denominator);
                } elseif ($datas['finance_datas_origin'] == '2') {
                    if ($datas['cost_count_type'] == '1') {
                        $purchasing_logistics = "report.report_purchasing_cost + report.report_logistics_head_course ";
                    } else {
                        $purchasing_logistics = "report.first_purchasing_cost  + report.first_logistics_head_course ";
                    }
                    $fields_tmp = "(CASE WHEN report.goods_operation_pattern = 2 THEN (COALESCE(report.report_channel_profit,0) + COALESCE(report.bychannel_channel_profit,0) + {$purchasing_logistics}) ELSE (report.report_goods_profit+ {$purchasing_logistics} {$estimated_monthly_storage_fee_field})  END $repair_data) * ({:RATE} / COALESCE(rates.rate ,1)) ";
                    $fields['count_total'] = "(SUM($fields_tmp) * 1.0000 / nullif(sum($fields_denominator),0))";
                    $time_fields = $this->getTimeFields($time_line, $fields_tmp, $fields_denominator);
                }

            } elseif ($custom_target && $custom_target['target_type'] == 1) {
                $tempField = "report.monthly_sku_" . $custom_target['month_goods_field'];
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $custom_target['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($time_line, $tempField);
                }

            } elseif (in_array($time_target, $keys)) {
                $tempField = "report.monthly_sku_" . $new_target_keys[$time_target]['month_goods_field'];
                //新增指标
                if ($datas['currency_code'] != 'ORIGIN' && $new_target_keys[$time_target]['format_type'] == 4) {
                    $fields['count_total'] = "sum({$tempField} / COALESCE(rates.rate, 1) * {:RATE})";
                    $time_fields = $this->getTimeFields($time_line, "{$tempField} * ({:RATE} / COALESCE(rates.rate ,1))");
                } else {
                    $fields['count_total'] = "SUM({$tempField})";
                    $time_fields = $this->getTimeFields($time_line, $tempField);
                }
            } else {
                $datas['time_target'] = $time_target;
                $fields_tmp = $this->getTimeField($datas, $time_line, 3);
                $fields['count_total'] = $fields_tmp['count_total'];
                $time_fields = $fields_tmp['time_fields'];

            }

            $fields[$time_target] = $fields['count_total'];
            $time_fields_arr[$time_target] = $time_fields;
        }
        if($custom_target && $custom_target['target_type'] == 2){
            $this->dealTimeTargets($fields,$custom_target,$time_line,$time_fields_arr,$target_key);
        }else {
            if (!empty($time_fields) && is_array($time_fields)) {
                foreach ($time_fields as $kt => $time_field) {
                    $fields[$kt] = $time_field;
                }
            }
        }
        //$fields = array_merge($fields, $time_fields);
        return $fields;
    }

    private function getCustomTargetFields(&$fields,$custom_targets_list,$targets = array(),$targets_temp = array(), $datas = [],&$fba_target_key = array(),$is_count = 0,$isMysql = false){
        $fba_field_exits = [];
        $operational_char_arr = array(".","+", "-", "*", "/", "", "(", ")");
        if($custom_targets_list){
            foreach ($custom_targets_list as $item){
                if ($item['target_type'] == 1)
                {
                    //指标
                    if (in_array($item['count_dimension'], [1, 2]))
                    {
                        if ($datas['currency_code'] != 'ORIGIN' && $item['format_type'] == 4){
                            $fields[$item['target_key']] = 'SUM(report.monthly_sku_'.$item['month_goods_field'].' / COALESCE(rates.rate, 1) * {:RATE})';
                        }else{
                            $fields[$item['target_key']] = 'SUM(report.monthly_sku_'.$item['month_goods_field'].')';
                        }
                    }
                    elseif($item['count_dimension'] == 3)
                    {
                        if ($datas['currency_code'] != 'ORIGIN' && $item['format_type'] == 4){
                            $fields[$item['target_key']] = 'SUM(monthly_profit.'.$item['month_channel_field'].' / COALESCE(rates.rate, 1) * {:RATE})';
                        }else{
                            $fields[$item['target_key']] = 'SUM(monthly_profit.'.$item['month_channel_field'].')';
                        }
                    }
                }
            }
            foreach ($custom_targets_list as $item){
                if ($item['target_type'] == 2)
                {
                    $formula_json_arr = $item['formula_json'] ? json_decode($item['formula_json'],true) : [];
                    $formula_fields_arr = $item['formula_fields'] ? explode(",",$item['formula_fields']) : [];
                    //自定义算法
                    $count = 0;
                    if(in_array($item['target_key'],$targets)){
                        $str = $item['formula'] ;
                        $str = str_replace('/(', ' * 1.0000 /(', $str);//指标数据数据类型为整数
                        foreach ($formula_json_arr as $k => $f_key) {
                            if(!in_array($f_key,$operational_char_arr)) {
                                if (!is_numeric($f_key)) {
                                    $str = str_replace('/{' . $f_key . '}', ' * 1.0000 /NULLIF({' . $f_key . '},0)', $str);//分母为0的处理
                                }
                            }
                        }
                        foreach ($formula_fields_arr as $field) {
                            if(in_array($field,$this->fba_fields_arr)){
                                $count++;
                            }
                            if(!empty($fields[$field])){
                                $str = str_replace('{'.$field.'}' , $fields[$field] , $str);
                            }else{
                                $str = 'NULL';
                            }
                        }
                        if($count){
                            $fba_field_exits = array_merge($fba_field_exits,$formula_fields_arr);
                            $fba_target_key[] = $item['target_key'];
                            $str = $is_count ? "1" : "'{$item['formula']}'";
                        }
                        $fields[$item['target_key']] = $isMysql ? $str : "try(" . $str . ")";
                    }
                }
            }
        }
        $fba_field_exits = $fba_field_exits ? array_unique($fba_field_exits) : [];
        //unset没选的字段
        if($targets){
            foreach ($targets as $k => $v){
                if(!in_array($v,$targets_temp) && !in_array($v,$fba_field_exits)){
                    unset($fields[$v]);
                }
            }
        }
    }

    private function addNewTargets($datas_ark_custom_target_md,$user_id,$custom_targets_list = array()){
        //自定义公式里包含新增指标
        $formula_fields = "";
        if($custom_targets_list){
            foreach ($custom_targets_list as $item){
                if($item['formula_fields']){
                    $formula_fields .= $item['formula_fields'] . ",";
                }
            }
            $formula_fields = trim($formula_fields,",");
            $formula_target = "'" . implode("','",array_values(array_unique(explode(",",$formula_fields)))) . "'";
            $formula_targets_list = $datas_ark_custom_target_md->getList("user_id = {$user_id} AND target_type = 1 AND count_dimension IN (1,2) AND target_key IN ({$formula_target})");
            $custom_targets_list = array_merge($custom_targets_list,$formula_targets_list);
        }
        return $custom_targets_list;
    }

    private function addCustomTargets($targets = array(),$custom_targets_list = array()){
        if($custom_targets_list){
            foreach ($custom_targets_list as $item){
                if ($item['target_type'] == 2){
                    $custom_targets = explode(',', $item['formula_fields']);
                    $targets = array_merge($targets,$custom_targets);
                }elseif ($item['target_type'] == 1){
                    if ($item['count_dimension'] == 3){
                        $this->countDimensionChannel = true;
                    }
                }
            }
        }
        $targets = array_values(array_unique($targets));
        return $targets;
    }

    private function dealTimeTargets(&$fields,$custom_target,$time_line = array(),$time_fields_arr = array(),$target_key = "",$isMysql = false){
        $str = $custom_target['formula'] ;
        $str = str_replace('/(', ' * 1.0000 /(', $str);//指标数据数据类型为整数
        $time_targets = $custom_target['formula_json'] ? json_decode($custom_target['formula_json'],true) : [] ;
        $formula_fields_arr = $custom_target['formula_fields'] ? explode(",",$custom_target['formula_fields']) : [];
        $operational_char_arr = array(".","+", "-", "*", "/", "", "(", ")");
        foreach ($time_targets as $k => $f_key) {
            if(!in_array($f_key,$operational_char_arr)){
                if(!is_numeric($f_key)){
                    $str = str_replace('/{' . $f_key . '}', ' * 1.0000 /NULLIF({' . $f_key . '},0)', $str);//分母为0的处理
                }
            }
        }
        foreach ($formula_fields_arr as $field) {
            if(!empty($fields[$field])){
                $str = str_replace('{'.$field.'}' , $fields[$field] , $str);
            }else{
                $str = 'NULL';
            }
        }
        $fields['count_total'] =  $isMysql ? $str : "try(" . $str . ")";
        $fields[$target_key] =  $isMysql ? $str : "try(" . $str . ")";

        $time_list = array_column($time_line, "key");
        foreach ($time_list as $date) {
            $str = $custom_target['formula'] ;
            foreach ($time_targets as $k => $f_key) {
                if(!in_array($f_key,$operational_char_arr)){
                    if(!is_numeric($f_key)){
                        $str = str_replace('/{' . $f_key . '}', ' * 1.0000 /NULLIF({' . $f_key . '},0)', $str);//分母为0的处理
                    }
                }
            }
            foreach ($formula_fields_arr as $field) {
                if(!empty($time_fields_arr[$field][$date])){
                    $str = str_replace('{'.$field.'}' , $time_fields_arr[$field][$date] , $str);
                }else{
                    $str = 'NULL';
                }
            }
            $fields[strval($date)] = $isMysql ? $str : "try(" . $str . ")";
        }
        //unset没选的字段
        if($time_targets){
            foreach ($time_targets as $target){
                unset($fields[$target]);
            }
        }
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

    public function count_custom_formula($formula = '' , $data = array()){
        $str = $formula ;
        foreach ($data as $key => $value) {
            if(is_null($value)){
                $value = 'NULL';
            }
            $str = str_replace('{'.$key.'}' , " " . $value . " ", $str);
        }
        $str = preg_replace('/{[a-z,A-Z,0-9,-,_]*}/',0,$str);
        if(strpos($str,' NULL ') !== false || strpos($str,'/ 0 ') !== false || strpos($str,'* 0 ') !== false){
            $rt = null;
        }else{
            $rt = eval("return $str;");
        }
        return $rt ;

    }

    private function queryList($fields,$exchangeCode,$day_param,$field_data,$table,$where,$group,$isJoin = false,$isMysql=false){
        $fields_tmp = [];
        foreach ($fields as $key => $value){
            $key_value = $key;
            if($key == "group"){
                $key = '"group"';
            }
            if(stripos($value,"min(") !== false){
                $fields_tmp[] = "min(report_tmp.{$key}) " . ' AS "' . $key_value . '"';
            }elseif (stripos($value,"max(") !== false){
                $fields_tmp[] = "max(report_tmp.{$key}) " . ' AS "' . $key_value . '"';
            }elseif($value == 'NULL'){
                $fields_tmp[] = "NULL" . ' AS "' . $key_value . '"';
            }else{
                $fields_tmp[] = "SUM(report_tmp.{$key}) " . ' AS "' . $key_value . '"';
            }
        }
        $field_data_tmp = str_replace("{:RATE}", $exchangeCode, implode(',', $fields_tmp));
        $field_data_tmp = str_replace("{:DAY}", $day_param, $field_data_tmp);
        $sql = "SELECT {$field_data_tmp} FROM (SELECT {$field_data} FROM {$table} WHERE {$where} GROUP BY {$group}) AS report_tmp";
        //商品维度
        if($isJoin){
            foreach ($this->goodsCols as $key => $value){
                if (!is_array($value)) {
//                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report."' . $key.'"', 'amazon_goods.' . $value, $sql);

                } else {

                    if (strpos($table, '_day_report_')) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['day'], $sql);
                    } elseif (strpos($table,'_week_report_' )) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['week'], $sql);
                    } elseif (strpos($table,'_month_report_')) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['month'], $sql);
                    }
                }
            }
        }

        $lists = $this->query($sql, [], null, 300, $isMysql);
        return $lists;
    }

    public function getUnGoodsChannelMothGoalData($lists = [], $datas = [], $channel_arr = [],$currencyInfo = [], $exchangeCode = '1'){
        if (empty($lists)){
            return $lists;
        }

        if (empty($channel_arr)){
            foreach ($lists as $val){
                if ($val['channel_id']){
                    $channel_arr[] = $val['channel_id'];
                }
            }
        }

        $userInfo = getUserInfo();
        $templateType = $userInfo['template_type']; //0旧版 1新版
        $goalList = [];
        if(!empty($channel_arr)){
            $nowYear = date("Y");
            $beforeYear = $nowYear - 1;
            $afterYear = $nowYear + 1;

            $channelTargetsModel = new ChannelTargetsMySQLModel([], $this->dbhost, $this->codeno);;

            if ($templateType){
                $where = "channel_id IN(".implode(',', $channel_arr).") AND user_id = {$datas['user_id']} AND year IN({$nowYear},{$beforeYear},{$afterYear}) AND type = 2" ;
            }else{
                $where = "channel_id IN(".implode(',', $channel_arr).") AND user_id = {$datas['user_id']} AND year IN({$nowYear},{$beforeYear},{$afterYear}) AND type = 1" ;
            }
            $goalList = $channelTargetsModel->select($where);
        }

        $mapGoal = [];
        $mapRate = [];
        if ($goalList){
            $siteId = [];

            if ($templateType){
                foreach ($goalList as $val){
                    $siteId[] = $val['site_id'];
                    if (!empty($val['new_target'])){
                        $tempNewTarget = json_decode($val['new_target'], true);
                        foreach ($tempNewTarget['target'] as $v){
                            $mapGoal[$val['channel_id']][$val['year']]["month{$v['date']}"] = $v['sale_money'];
                        }
                    }
                }
            }else{
                foreach ($goalList as $val){
                    $mapGoal[$val['channel_id']][$val['year']] = $val;
                    $siteId[] = $val['site_id'];
                }
            }

            if ($datas['currency_code'] != 'ORIGIN') {
                $rateModel = new SiteRateMySQLModel([], $this->dbhost, $this->codeno);;
                if ($currencyInfo['currency_type'] == '1' || empty($currencyInfo)) {
                    $rateList = $rateModel->select("site_id IN(".implode(',', array_unique($siteId)).") AND user_id = 0");
                } else {
                    $rateList = $rateModel->select("site_id IN(".implode(',', array_unique($siteId)).") AND user_id = {$datas['user_id']}");
                }

                foreach ($rateList as $val){
                    $mapRate[$val['site_id']] = $val['rate'];
                }
            }
        }

        if ($datas['count_periods'] == 3)
        {
            //按月
            foreach ($lists as $key => $val){
                $temTime = strtotime($val['time']);
                $tempYear = date("Y", $temTime);
                $tempMonth = date("n", $temTime);
                $tempRate = isset($mapRate[$val['site_id']]) ? $mapRate[$val['site_id']] : 1;

                if ($datas['currency_code'] != 'ORIGIN') {
                    $val['sale_channel_month_goal'] = isset($mapGoal[$val['channel_id']][$tempYear]) ? round($mapGoal[$val['channel_id']][$tempYear]["month{$tempMonth}"] / $tempRate * $exchangeCode, 2) : 0;
                }else{
                    $val['sale_channel_month_goal'] = isset($mapGoal[$val['channel_id']][$tempYear]) ? $mapGoal[$val['channel_id']][$tempYear]["month{$tempMonth}"] : 0;
                }
                $lists[$key] = $val;
            }
        }
        elseif ($datas['count_periods'] == 4)
        {
            //按季
            foreach ($lists as $key => $val){
                $explodeTime = explode('-', $val['time']) ;
                $tempYear = $explodeTime[0];
                $tempQua = $explodeTime[1];
                $tempRate = isset($mapRate[$val['site_id']]) ? $mapRate[$val['site_id']] : 1;

                if (isset($mapGoal[$val['channel_id']][$tempYear])){
                    $tempGaol = $mapGoal[$val['channel_id']][$tempYear];
                    if ($tempQua == 1){
                        if ($datas['currency_code'] != 'ORIGIN') {
                            $val['sale_channel_month_goal'] = round(($tempGaol['month1'] + $tempGaol['month2'] + $tempGaol['month3']) / $tempRate * $exchangeCode, 2);
                        }else{
                            $val['sale_channel_month_goal'] = $tempGaol['month1'] + $tempGaol['month2'] + $tempGaol['month3'];
                        }
                    }elseif ($tempQua == 2){
                        if ($datas['currency_code'] != 'ORIGIN') {
                            $val['sale_channel_month_goal'] = round(($tempGaol['month4'] + $tempGaol['month5'] + $tempGaol['month6']) / $tempRate * $exchangeCode, 2);
                        }else{
                            $val['sale_channel_month_goal'] = $tempGaol['month4'] + $tempGaol['month5'] + $tempGaol['month6'];
                        }
                    }elseif ($tempQua == 3){
                        if ($datas['currency_code'] != 'ORIGIN') {
                            $val['sale_channel_month_goal'] = round(($tempGaol['month7'] + $tempGaol['month8'] + $tempGaol['month9']) / $tempRate * $exchangeCode, 2);
                        }else{
                            $val['sale_channel_month_goal'] = $tempGaol['month7'] + $tempGaol['month8'] + $tempGaol['month9'];
                        }
                    }else{
                        if ($datas['currency_code'] != 'ORIGIN') {
                            $val['sale_channel_month_goal'] = round(($tempGaol['month10'] + $tempGaol['month11'] + $tempGaol['month12']) / $tempRate * $exchangeCode, 2);
                        }else{
                            $val['sale_channel_month_goal'] = $tempGaol['month10'] + $tempGaol['month11'] + $tempGaol['month12'];
                        }
                    }
                }else{
                    $val['sale_channel_month_goal'] = 0;
                }
                $lists[$key] = $val;
            }
        }
        elseif ($datas['count_periods'] == 5)
        {
            //按年
            foreach ($lists as $key => $val)
            {
                if (isset($mapGoal[$val['channel_id']][$val['time']])){
                    $tempGaol = $mapGoal[$val['channel_id']][$val['time']];
                    $tempSum = $tempGaol['month1'] + $tempGaol['month2'] + $tempGaol['month3'] + $tempGaol['month4'] + $tempGaol['month5'] + $tempGaol['month6'] + $tempGaol['month7'] + $tempGaol['month8'] + $tempGaol['month9'] + $tempGaol['month10'] + $tempGaol['month11'] + $tempGaol['month12'];
                    $tempRate = isset($mapRate[$val['site_id']]) ? $mapRate[$val['site_id']] : 1;

                    if ($datas['currency_code'] != 'ORIGIN') {
                        $val['sale_channel_month_goal'] = round($tempSum / $tempRate * $exchangeCode, 2);
                    }else{
                        $val['sale_channel_month_goal'] = $tempSum;
                    }
                }else{
                    $val['sale_channel_month_goal'] = 0;
                }
                $lists[$key] = $val;
            }
        }
        return $lists;
    }


    /**
     * 获取运营人员table
     * @author json.qiu 2021/05/31
     *
     * @param $datas
     * @param $ym_where  //ym条件
     * @param string $table_type //店铺类型，day,week,month
     * @return string  返回table
     */
    public function operationTable($datas,$ym_where,$table_type = "day"){

        $create_time_tmp = str_replace("site_id",'dw_report.byorder_site_id',$datas['origin_time']);
        $where_dw_report_amazon_goods = "dw_report.byorder_user_id = {$datas['user_id']} AND dw_report.user_id_mod = ".($datas['user_id'] % 20)
            ." AND dw_report.byorder_channel_id IN (".$datas['operation_channel_ids'].") "
            ." AND amazon_goods.goods_user_id = {$datas['user_id']} AND amazon_goods.goods_user_id_mod = ".($datas['user_id'] % 20)." AND  amazon_goods.goods_operation_user_admin_id > 0  "." AND amazon_goods.goods_channel_id IN (".$datas['operation_channel_ids'].") ";
        $where_ym = " AND ".str_replace("report.",'dw_report.',$ym_where);
        $where_dw_report_month_amazon_goods = $where_dw_report_amazon_goods;
        $where_dw_report_amazon_goods .= " ".str_replace("create_time",'dw_report.byorder_create_time',$create_time_tmp);


        $where_channel = "user_id = {$datas['user_id']} AND user_id_mod = ".($datas['user_id'] % 20)." AND channel_id IN (".$datas['operation_channel_ids'].") ".$datas['origin_time']." AND ".str_replace("report.",'',$ym_where);
        $goods_month_table = '';
        if ($table_type == 'week'){
//sum(bychannel_first_purchasing_cost) as bychannel_first_purchasing_cost,
//                        sum(bychannel_first_logistics_head_course) as bychannel_first_logistics_head_course,
//                        sum(bychannel_fba_first_logistics_head_course) as bychannel_fba_first_logistics_head_course,
//                        sum(bychannel_fbm_first_logistics_head_course) as bychannel_fbm_first_logistics_head_course,
//            sum(bychannel_monthly_sku_reserved_field37) as bychannel_monthly_sku_reserved_field37,
//                        sum(bychannel_monthly_sku_reserved_field38) as bychannel_monthly_sku_reserved_field38,
//                        sum(bychannel_monthly_sku_reserved_field39) as bychannel_monthly_sku_reserved_field39,
//                        sum(bychannel_monthly_sku_reserved_field40) as bychannel_monthly_sku_reserved_field40,
//                        sum(bychannel_monthly_sku_reserved_field41) as bychannel_monthly_sku_reserved_field41,
//                        sum(bychannel_monthly_sku_reserved_field42) as bychannel_monthly_sku_reserved_field42,
//                        sum(bychannel_monthly_sku_reserved_field43) as bychannel_monthly_sku_reserved_field43,
//                        sum(bychannel_monthly_sku_reserved_field44) as bychannel_monthly_sku_reserved_field44,
//                        sum(bychannel_monthly_sku_reserved_field45) as bychannel_monthly_sku_reserved_field45,
//                        sum(bychannel_monthly_sku_reserved_field46) as bychannel_monthly_sku_reserved_field46,
//                        sum(bychannel_monthly_sku_reserved_field47) as bychannel_monthly_sku_reserved_field47,
//                        sum(bychannel_monthly_sku_reserved_field48) as bychannel_monthly_sku_reserved_field48,
//                        sum(bychannel_monthly_sku_reserved_field49) as bychannel_monthly_sku_reserved_field49,
            $goods_table = "{$this->table_dwd_goods_report} AS dw_report
			Right JOIN {$this->table_goods_dim_report} AS amazon_goods ON dw_report.byorder_amazon_goods_id = amazon_goods.es_id";
            $channel_field = "channel_id,myear,mmonth,mweek,max(mweekyear) as mweekyear,
                        max(mquarter) as mquarter,
                        max(site_id) as site_id,
                        max(user_id) as user_id,
                        SUM( bychannel_sales_quota ) as bychannel_sales_quota ,
                       
SUM( bychannel_fba_sales_quota ) as bychannel_fba_sales_quota ,
SUM( bychannel_sales_volume ) as bychannel_sales_volume ,
SUM( bychannel_fba_sales_volume ) as bychannel_fba_sales_volume ,
SUM( bychannel_operating_fee ) as bychannel_operating_fee ,
SUM( bychannel_fba_per_unit_fulfillment_fee ) as bychannel_fba_per_unit_fulfillment_fee ,
SUM( bychannel_return_postage_billing_fuel_surcharge ) as bychannel_return_postage_billing_fuel_surcharge ,
SUM( bychannel_postage_billing_tracking ) as bychannel_postage_billing_tracking ,
SUM( bychannel_postage_billing_delivery_area_surcharge ) as bychannel_postage_billing_delivery_area_surcharge ,
SUM( bychannel_postage_billing_vat ) as bychannel_postage_billing_vat ,
SUM( bychannel_postage_billing_signature_confirmation ) as bychannel_postage_billing_signature_confirmation ,
SUM( bychannel_return_postage_billing_oversize_surcharge ) as bychannel_return_postage_billing_oversize_surcharge ,
SUM( bychannel_postage_billing_postage_adjustment ) as bychannel_postage_billing_postage_adjustment ,
SUM( bychannel_postage_billing_insurance ) as bychannel_postage_billing_insurance ,
SUM( bychannel_postage_billing_import_duty ) as bychannel_postage_billing_import_duty ,
SUM( bychannel_postage_billing_fuel_surcharge ) as bychannel_postage_billing_fuel_surcharge ,
SUM( bychannel_product_ads_payment_eventlist_charge ) as bychannel_product_ads_payment_eventlist_charge ,
SUM( bychannel_product_ads_payment_eventlist_refund ) as bychannel_product_ads_payment_eventlist_refund ,
SUM( bychannel_fba_storage_fee ) as bychannel_fba_storage_fee ,
SUM( bychannel_review_enrollment_fee ) as bychannel_review_enrollment_fee ,
SUM( bychannel_loan_payment ) as bychannel_loan_payment ,
SUM( bychannel_debt_payment ) as bychannel_debt_payment ,
SUM( bychannel_coupon_redemption_fee ) as bychannel_coupon_redemption_fee ,
SUM( bychannel_run_lightning_deal_fee ) as bychannel_run_lightning_deal_fee ,
SUM( bychannel_fba_inbound_convenience_fee ) as bychannel_fba_inbound_convenience_fee ,
SUM( bychannel_labeling_fee ) as bychannel_labeling_fee ,
SUM( bychannel_polybagging_fee ) as bychannel_polybagging_fee ,
SUM( bychannel_fba_inbound_shipment_carton_level_info_fee ) as bychannel_fba_inbound_shipment_carton_level_info_fee ,
SUM( bychannel_fba_inbound_transportation_program_fee ) as bychannel_fba_inbound_transportation_program_fee ,
SUM( bychannel_fba_overage_fee ) as bychannel_fba_overage_fee ,
SUM( bychannel_fba_inbound_transportation_fee ) as bychannel_fba_inbound_transportation_fee ,
SUM( bychannel_fba_inbound_defect_fee ) as bychannel_fba_inbound_defect_fee ,
SUM( bychannel_subscription ) as bychannel_subscription ,
SUM( bychannel_charge_back_recovery ) as bychannel_charge_back_recovery ,
SUM( bychannel_cs_error_non_itemized ) as bychannel_cs_error_non_itemized ,
SUM( bychannel_return_postage_billing_postage ) as bychannel_return_postage_billing_postage ,
SUM( bychannel_re_evaluation ) as bychannel_re_evaluation ,
SUM( bychannel_subscription_fee_correction ) as bychannel_subscription_fee_correction ,
SUM( bychannel_incorrect_fees_non_itemized ) as bychannel_incorrect_fees_non_itemized ,
SUM( bychannel_buyer_recharge ) as bychannel_buyer_recharge ,
SUM( bychannel_multichannel_order_late ) as bychannel_multichannel_order_late ,
SUM( bychannel_non_subscription_fee_adj ) as bychannel_non_subscription_fee_adj ,
SUM( bychannel_fba_disposal_fee ) as bychannel_fba_disposal_fee ,
SUM( bychannel_fba_removal_fee ) as bychannel_fba_removal_fee ,
SUM( bychannel_fba_long_term_storage_fee ) as bychannel_fba_long_term_storage_fee ,
SUM( bychannel_fba_international_inbound_freight_fee ) as bychannel_fba_international_inbound_freight_fee ,
SUM( bychannel_fba_international_inbound_freight_tax_and_duty ) as bychannel_fba_international_inbound_freight_tax_and_duty ,
SUM( bychannel_postage_billing_transaction ) as bychannel_postage_billing_transaction ,
SUM( bychannel_postage_billing_delivery_confirmation ) as bychannel_postage_billing_delivery_confirmation ,
SUM( bychannel_postage_billing_postage ) as bychannel_postage_billing_postage ,
SUM( bychannel_reserved_field1 ) as bychannel_reserved_field1 ,
SUM( bychannel_reserved_field2 ) as bychannel_reserved_field2 ,
SUM( bychannel_reserved_field3 ) as bychannel_reserved_field3 ,
SUM( bychannel_reserved_field4 ) as bychannel_reserved_field4 ,
SUM( bychannel_reserved_field5 ) as bychannel_reserved_field5 ,
SUM( bychannel_reserved_field6 ) as bychannel_reserved_field6 ,
SUM( bychannel_reserved_field7 ) as bychannel_reserved_field7 ,
SUM( bychannel_reserved_field8 ) as bychannel_reserved_field8 ,
SUM( bychannel_reserved_field9 ) as bychannel_reserved_field9 ,
SUM( bychannel_reserved_field10 ) as bychannel_reserved_field10 ,
SUM( bychannel_reserved_field11 ) as bychannel_reserved_field11 ,
SUM( bychannel_reserved_field12 ) as bychannel_reserved_field12 ,
SUM( bychannel_reserved_field13 ) as bychannel_reserved_field13 ,
SUM( bychannel_reserved_field14 ) as bychannel_reserved_field14 ,
SUM( bychannel_reserved_field15 ) as bychannel_reserved_field15 ,
SUM( bychannel_reserved_field16 ) as bychannel_reserved_field16 ,
SUM( bychannel_reserved_field17 ) as bychannel_reserved_field17 ,
SUM( bychannel_reserved_field18 ) as bychannel_reserved_field18 ,
SUM( bychannel_reserved_field19 ) as bychannel_reserved_field19 ,
SUM( bychannel_reserved_field20 ) as bychannel_reserved_field20 ,
SUM( bychannel_misc_adjustment ) as bychannel_misc_adjustment ,
SUM( bychannel_cpc_sb_sales_volume ) as bychannel_cpc_sb_sales_volume ,
SUM( bychannel_cpc_sb_sales_quota ) as bychannel_cpc_sb_sales_quota ,
SUM( bychannel_cpc_sb_cost ) as bychannel_cpc_sb_cost ,
SUM( bychannel_modified_time ) as bychannel_modified_time ,
SUM( bychannel_platform_type ) as bychannel_platform_type ,
SUM( bychannel_mfnpostageFee ) as bychannel_mfnpostageFee ,
SUM( bychannel_coupon_payment_eventList_tax ) as bychannel_coupon_payment_eventList_tax ,
SUM( bychannel_channel_amazon_order_fee ) as bychannel_channel_amazon_order_fee ,
SUM( bychannel_channel_amazon_refund_fee ) as bychannel_channel_amazon_refund_fee ,
SUM( bychannel_channel_amazon_storage_fee ) as bychannel_channel_amazon_storage_fee ,
SUM( bychannel_channel_amazon_other_fee ) as bychannel_channel_amazon_other_fee ,
SUM( bychannel_channel_profit ) as bychannel_channel_profit ,
SUM( bychannel_channel_goods_adjustment_fee ) as bychannel_channel_goods_adjustment_fee ,
max( bychannel_create_time ) as bychannel_create_time 
            ";
            $channel_table = "(select {$channel_field} from {$this->table_channel_day_report} WHERE {$where_channel} group by  channel_id,myear,mmonth,mweek) AS bychannel ON goods.channel_id = bychannel.channel_id AND goods.myear = bychannel.myear AND goods.mmonth = bychannel.mmonth AND goods.mweek = bychannel.mweek
	    AND goods.goods_operation_pattern = 2";
            $goods_group = "amazon_goods.goods_operation_user_admin_id,amazon_goods.goods_channel_id,dw_report.byorder_myear,dw_report.byorder_mmonth,dw_report.byorder_mweek";
            $goods_other_field = "dw_report.byorder_mweek as mweek,max(dw_report.byorder_mweekyear) as mweekyear,";
            $report_other_field = "COALESCE(goods.mweek ,bychannel.mweek) AS mweek,COALESCE(goods.mweekyear ,bychannel.mweekyear) AS mweekyear,concat(cast(goods.goods_operation_user_admin_id as varchar),'_',cast(COALESCE(goods.mweekyear ,bychannel.mweekyear) as varchar),'_',lpad(cast(COALESCE(goods.mweek ,bychannel.mweek) as varchar),2,'0')) as goods_operation_user_admin_id_group,";

        }elseif ($table_type == 'month'){
            $goods_table = "{$this->table_dwd_goods_report} AS dw_report
			Right JOIN {$this->table_goods_dim_report} AS amazon_goods ON dw_report.byorder_amazon_goods_id = amazon_goods.es_id";
            $channel_table = "(select * from {$this->table_channel_month_report} WHERE {$where_channel} ) AS bychannel ON goods.channel_id = bychannel.channel_id AND goods.myear = bychannel.myear AND goods.mmonth = bychannel.mmonth 
	    AND goods.goods_operation_pattern = 2";
            $goods_group = "amazon_goods.goods_operation_user_admin_id,amazon_goods.goods_channel_id,dw_report.byorder_myear,dw_report.byorder_mmonth";
            $goods_other_field = "max(dw_report.byorder_mquarter) as mquarter,";
            $report_other_field = "
            COALESCE(goods.mquarter ,bychannel.mquarter) as mquarter,
            COALESCE(goods_month.first_purchasing_cost ,bychannel.first_purchasing_cost) as first_purchasing_cost,
            COALESCE(goods_month.first_logistics_head_course ,bychannel.first_logistics_head_course) as first_logistics_head_course,
            COALESCE(goods_month.fba_first_logistics_head_course ,bychannel.fba_first_logistics_head_course) as fba_first_logistics_head_course,
            COALESCE(goods_month.fbm_first_logistics_head_course ,bychannel.fbm_first_logistics_head_course) as fbm_first_logistics_head_course,
            COALESCE(goods_month.monthly_sku_reserved_field37 ,bychannel.monthly_sku_reserved_field37) as monthly_sku_reserved_field37,
            COALESCE(goods_month.monthly_sku_reserved_field38 ,bychannel.monthly_sku_reserved_field38) as monthly_sku_reserved_field38,
            COALESCE(goods_month.monthly_sku_reserved_field39 ,bychannel.monthly_sku_reserved_field39) as monthly_sku_reserved_field39,
            COALESCE(goods_month.monthly_sku_reserved_field40 ,bychannel.monthly_sku_reserved_field40) as monthly_sku_reserved_field40,
            COALESCE(goods_month.monthly_sku_reserved_field41 ,bychannel.monthly_sku_reserved_field41) as monthly_sku_reserved_field41,
            COALESCE(goods_month.monthly_sku_reserved_field42 ,bychannel.monthly_sku_reserved_field42) as monthly_sku_reserved_field42,
            COALESCE(goods_month.monthly_sku_reserved_field43 ,bychannel.monthly_sku_reserved_field43) as monthly_sku_reserved_field43,
            COALESCE(goods_month.monthly_sku_reserved_field44 ,bychannel.monthly_sku_reserved_field44) as monthly_sku_reserved_field44,
            COALESCE(goods_month.monthly_sku_reserved_field44 ,bychannel.monthly_sku_reserved_field44) as monthly_sku_reserved_field44,
            COALESCE(goods_month.monthly_sku_reserved_field45 ,bychannel.monthly_sku_reserved_field45) as monthly_sku_reserved_field45,
            COALESCE(goods_month.monthly_sku_reserved_field46 ,bychannel.monthly_sku_reserved_field46) as monthly_sku_reserved_field46,
            COALESCE(goods_month.monthly_sku_reserved_field47 ,bychannel.monthly_sku_reserved_field47) as monthly_sku_reserved_field47,
            COALESCE(goods_month.monthly_sku_reserved_field48 ,bychannel.monthly_sku_reserved_field48) as monthly_sku_reserved_field48,
            COALESCE(goods_month.monthly_sku_reserved_field49 ,bychannel.monthly_sku_reserved_field49) as monthly_sku_reserved_field49,
            COALESCE(goods_month.monthly_sku_estimated_monthly_storage_fee ,0) as monthly_sku_estimated_monthly_storage_fee,
            concat(cast(goods.goods_operation_user_admin_id as varchar),'_',cast(COALESCE(goods.myear ,bychannel.myear) as  varchar),'_',lpad(cast(COALESCE(goods.mmonth ,bychannel.mmonth) as varchar),2,'0')) as goods_operation_user_admin_id_group,";

            $goods_month_table = "LEFT JOIN ( SELECT
			amazon_goods.goods_operation_user_admin_id,
			amazon_goods.goods_channel_id as channel_id,
			dw_report.myear,
			sum(first_purchasing_cost ) as first_purchasing_cost,
			sum(first_logistics_head_course ) as first_logistics_head_course,
			sum(first_logistics_head_course- reserved_field24) as fba_first_logistics_head_course,
			sum(reserved_field24 ) as fbm_first_logistics_head_course,
			sum(reserved_field37 ) as monthly_sku_reserved_field37,
			sum(reserved_field38 ) as monthly_sku_reserved_field38,
			sum(reserved_field39 ) as monthly_sku_reserved_field39,
			sum(reserved_field40 ) as monthly_sku_reserved_field40,
			sum(reserved_field41 ) as monthly_sku_reserved_field41,
			sum(reserved_field42 ) as monthly_sku_reserved_field42,
			sum(reserved_field43 ) as monthly_sku_reserved_field43,
			sum(reserved_field44 ) as monthly_sku_reserved_field44,
			sum(reserved_field45 ) as monthly_sku_reserved_field45,
			sum(reserved_field46 ) as monthly_sku_reserved_field46,
			sum(reserved_field47 ) as monthly_sku_reserved_field47,
			sum(reserved_field48 ) as monthly_sku_reserved_field48,
			sum(reserved_field49 ) as monthly_sku_reserved_field49,
			sum(estimated_monthly_storage_fee ) as monthly_sku_estimated_monthly_storage_fee,
			dw_report.mmonth
	FROM
		{$this->table_monthly_profit_report_by_sku} AS dw_report 
		left JOIN {$this->table_goods_dim_report} AS amazon_goods ON dw_report.amazon_goods_id = amazon_goods.es_id and dw_report.db_num = '".$this->dbhost."'  WHERE ".str_replace("dw_report.create_time","dw_report.start_time",str_replace("dw_report.byorder_","dw_report.",$where_dw_report_month_amazon_goods))." GROUP BY amazon_goods.goods_operation_user_admin_id,amazon_goods.goods_channel_id,dw_report.myear,dw_report.mmonth) as goods_month ON goods.channel_id = goods_month.channel_id 
		AND goods.myear = goods_month.myear 
		AND goods.mmonth = goods_month.mmonth AND goods.goods_operation_user_admin_id = goods_month.goods_operation_user_admin_id ";
        }else{
//            $goods_table = "dws.dws_dataark_f_dw_goods_day_report_{$this->dbhost} AS dw_report
//			Right JOIN dim.dim_dataark_f_dw_goods_dim_report_{$this->dbhost} AS amazon_goods ON dw_report.amazon_goods_id = amazon_goods.es_id";
            $goods_table = "{$this->table_dwd_goods_report} AS dw_report
			Right JOIN {$this->table_goods_dim_report} AS amazon_goods ON dw_report.byorder_amazon_goods_id = amazon_goods.es_id";
            $channel_table = "(select * from {$this->table_channel_day_report} WHERE {$where_channel} ) AS bychannel ON goods.channel_id = bychannel.channel_id AND goods.myear = bychannel.myear AND goods.mmonth = bychannel.mmonth AND goods.mday = bychannel.mday
	    AND goods.goods_operation_pattern = 2";
            $goods_group = "amazon_goods.goods_operation_user_admin_id,amazon_goods.goods_channel_id,dw_report.byorder_myear,dw_report.byorder_mmonth,dw_report.byorder_mday";
            $goods_other_field = "dw_report.byorder_mday as mday,";
            $report_other_field = "COALESCE(goods.mday ,bychannel.mday) AS mday,concat(cast(goods.goods_operation_user_admin_id as  varchar),'_',cast(COALESCE(goods.myear ,bychannel.myear) as varchar),'_',lpad(cast(COALESCE(goods.mmonth ,bychannel.mmonth) as varchar),2,'0'),'_',lpad(cast(COALESCE(goods.mday ,bychannel.mday) as varchar),2,'0')) as goods_operation_user_admin_id_group,";

        }

        $where_dw_report_amazon_goods .= $where_ym." AND dw_report.available = 1 ";

        $table = " (
select
COALESCE(goods.byorder_user_sessions,0) AS byorder_user_sessions ,
COALESCE(goods.byorder_quantity_of_goods_ordered,0) AS byorder_quantity_of_goods_ordered ,
COALESCE(goods.byorder_number_of_visits,0) AS byorder_number_of_visits ,
COALESCE(goods.byorder_buy_button_winning_rate,0) AS byorder_buy_button_winning_rate ,
COALESCE(goods.byorder_sales_volume,0) AS byorder_sales_volume ,
COALESCE(goods.byorder_group_id,0) AS byorder_group_id ,
COALESCE(goods.byorder_sales_quota,0) AS byorder_sales_quota ,
COALESCE(goods.byorder_refund_num,0) AS byorder_refund_num ,
COALESCE(goods.byorder_refund,0) AS byorder_refund ,
COALESCE(goods.byorder_promote_discount,0) AS byorder_promote_discount ,
COALESCE(goods.byorder_refund_promote_discount,0) AS byorder_refund_promote_discount ,
COALESCE(goods.byorder_purchasing_cost,0) AS byorder_purchasing_cost ,
COALESCE(goods.byorder_logistics_head_course,0) AS byorder_logistics_head_course ,
COALESCE(goods.byorder_goods_profit,0) AS byorder_goods_profit ,
COALESCE(goods.byorder_goods_amazon_fee,0) AS byorder_goods_amazon_fee ,
COALESCE(goods.byorder_platform_sales_commission,0) AS byorder_platform_sales_commission ,
COALESCE(goods.byorder_fba_generation_delivery_cost,0) AS byorder_fba_generation_delivery_cost ,
COALESCE(goods.byorder_fbaperorderfulfillmentfee,0) AS byorder_fbaperorderfulfillmentfee ,
COALESCE(goods.byorder_fbaweightbasedfee,0) AS byorder_fbaweightbasedfee ,
COALESCE(goods.byorder_profit,0) AS byorder_profit ,
COALESCE(goods.byorder_order_variableclosingfee,0) AS byorder_order_variableclosingfee ,
COALESCE(goods.byorder_fixedclosingfee,0) AS byorder_fixedclosingfee ,
COALESCE(goods.byorder_refund_variableclosingfee,0) AS byorder_refund_variableclosingfee ,
COALESCE(goods.byorder_goods_amazon_other_fee,0) AS byorder_goods_amazon_other_fee ,
COALESCE(goods.byorder_returnshipping,0) AS byorder_returnshipping ,
COALESCE(goods.byorder_return_and_return_sales_commission,0) AS byorder_return_and_return_sales_commission ,
COALESCE(goods.byorder_fba_refund_treatment_fee,0) AS byorder_fba_refund_treatment_fee ,
COALESCE(goods.byorder_fbacustomerreturnperorderfee,0) AS byorder_fbacustomerreturnperorderfee ,
COALESCE(goods.byorder_fbacustomerreturnweightbasedfee,0) AS byorder_fbacustomerreturnweightbasedfee ,
COALESCE(goods.byorder_estimated_monthly_storage_fee,0) AS byorder_estimated_monthly_storage_fee ,
COALESCE(goods.byorder_long_term_storage_fee,0) AS byorder_long_term_storage_fee ,
COALESCE(goods.byorder_reserved_field16,0) AS byorder_reserved_field16 ,
COALESCE(goods.byorder_reserved_field10,0) AS byorder_reserved_field10 ,
COALESCE(goods.byorder_cpc_cost,0) AS byorder_cpc_cost ,
COALESCE(goods.byorder_cpc_sd_cost,0) AS byorder_cpc_sd_cost ,
COALESCE(goods.byorder_reserved_field1,0) AS byorder_reserved_field1 ,
COALESCE(goods.byorder_reserved_field2,0) AS byorder_reserved_field2 ,
COALESCE(goods.byorder_cpc_sd_clicks,0) AS byorder_cpc_sd_clicks ,
COALESCE(goods.byorder_cpc_sp_clicks,0) AS byorder_cpc_sp_clicks ,
COALESCE(goods.byorder_sp_attributedconversions7d,0) AS byorder_sp_attributedconversions7d ,
COALESCE(goods.byorder_sd_attributedconversions7d,0) AS byorder_sd_attributedconversions7d ,
COALESCE(goods.byorder_sp_attributedsales7d,0) AS byorder_sp_attributedsales7d ,
COALESCE(goods.byorder_sd_attributedsales7d,0) AS byorder_sd_attributedsales7d ,
COALESCE(goods.byorder_sd_attributedconversions7dsamesku,0) AS byorder_sd_attributedconversions7dsamesku ,
COALESCE(goods.byorder_sp_attributedconversions7dsamesku,0) AS byorder_sp_attributedconversions7dsamesku ,
COALESCE(goods.byorder_sd_attributedsales7dsamesku,0) AS byorder_sd_attributedsales7dsamesku ,
COALESCE(goods.byorder_sp_attributedsales7dsamesku,0) AS byorder_sp_attributedsales7dsamesku ,
COALESCE(goods.byorder_buy_button_winning_num,0) AS byorder_buy_button_winning_num ,
COALESCE(goods.byorder_return_and_return_commission,0) AS byorder_return_and_return_commission ,
COALESCE(goods.byorder_reserved_field17,0) AS byorder_reserved_field17 ,
COALESCE(goods.byorder_order_quantity,0) AS byorder_order_quantity ,
COALESCE(goods.byorder_reserved_field21,0) AS byorder_reserved_field21 ,
COALESCE(goods.byorder_fba_sales_volume,0) AS byorder_fba_sales_volume ,
COALESCE(goods.byorder_fbm_sales_volume,0) AS byorder_fbm_sales_volume ,
COALESCE(goods.byorder_fba_refund_num,0) AS byorder_fba_refund_num ,
COALESCE(goods.byorder_fbm_refund_num,0) AS byorder_fbm_refund_num ,
COALESCE(goods.byorder_fba_logistics_head_course,0) AS byorder_fba_logistics_head_course ,
COALESCE(goods.byorder_fba_sales_quota,0) AS byorder_fba_sales_quota ,
COALESCE(goods.byorder_fbm_sales_quota,0) AS byorder_fbm_sales_quota ,
COALESCE(goods.byorder_fba_refund,0) AS byorder_fba_refund ,
COALESCE(goods.byorder_fbm_refund,0) AS byorder_fbm_refund ,
COALESCE(goods.byorder_tax,0) AS byorder_tax ,
COALESCE(goods.byorder_ware_house_lost,0) AS byorder_ware_house_lost ,
COALESCE(goods.byorder_ware_house_damage,0) AS byorder_ware_house_damage ,
COALESCE(goods.byorder_shipping_charge,0) AS byorder_shipping_charge ,
COALESCE(goods.byorder_customer_damage,0) AS byorder_customer_damage ,
COALESCE(goods.byorder_removal_order_lost,0) AS byorder_removal_order_lost ,
COALESCE(goods.byorder_incorrect_fees_items,0) AS byorder_incorrect_fees_items ,
COALESCE(goods.byorder_missing_from_inbound,0) AS byorder_missing_from_inbound ,
COALESCE(goods.byorder_multichannel_order_lost,0) AS byorder_multichannel_order_lost ,
COALESCE(goods.byorder_removal_fee,0) AS byorder_removal_fee ,
COALESCE(goods.byorder_gift_wrap,0) AS byorder_gift_wrap ,
COALESCE(goods.report_sales_volume,0) AS report_sales_volume ,
COALESCE(goods.report_group_id,0) AS report_group_id ,
COALESCE(goods.report_sales_quota,0) AS report_sales_quota ,
COALESCE(goods.report_refund_num,0) AS report_refund_num ,
COALESCE(goods.report_refund,0) AS report_refund ,
COALESCE(goods.report_promote_discount,0) AS report_promote_discount ,
COALESCE(goods.report_refund_promote_discount,0) AS report_refund_promote_discount ,
COALESCE(goods.report_purchasing_cost,0) AS report_purchasing_cost ,
COALESCE(goods.report_logistics_head_course,0) AS report_logistics_head_course ,
COALESCE(goods.report_goods_profit,0) AS report_goods_profit ,
COALESCE(goods.report_goods_amazon_fee,0) AS report_goods_amazon_fee ,
COALESCE(goods.report_platform_sales_commission,0) AS report_platform_sales_commission ,
COALESCE(goods.report_fba_generation_delivery_cost,0) AS report_fba_generation_delivery_cost ,
COALESCE(goods.report_fbaperorderfulfillmentfee,0) AS report_fbaperorderfulfillmentfee ,
COALESCE(goods.report_fbaweightbasedfee,0) AS report_fbaweightbasedfee ,
COALESCE(goods.report_profit,0) AS report_profit ,
COALESCE(goods.report_order_variableclosingfee,0) AS report_order_variableclosingfee ,
COALESCE(goods.report_fixedclosingfee,0) AS report_fixedclosingfee ,
COALESCE(goods.report_refund_variableclosingfee,0) AS report_refund_variableclosingfee ,
COALESCE(goods.report_goods_amazon_other_fee,0) AS report_goods_amazon_other_fee ,
COALESCE(goods.report_returnshipping,0) AS report_returnshipping ,
COALESCE(goods.report_return_and_return_sales_commission,0) AS report_return_and_return_sales_commission ,
COALESCE(goods.report_fba_refund_treatment_fee,0) AS report_fba_refund_treatment_fee ,
COALESCE(goods.report_fbacustomerreturnperorderfee,0) AS report_fbacustomerreturnperorderfee ,
COALESCE(goods.report_fbacustomerreturnweightbasedfee,0) AS report_fbacustomerreturnweightbasedfee ,
COALESCE(goods.report_estimated_monthly_storage_fee,0) AS report_estimated_monthly_storage_fee ,
COALESCE(goods.report_long_term_storage_fee,0) AS report_long_term_storage_fee ,
COALESCE(goods.report_reserved_field16,0) AS report_reserved_field16 ,
COALESCE(goods.report_reserved_field10,0) AS report_reserved_field10 ,
COALESCE(goods.report_cpc_cost,0) AS report_cpc_cost ,
COALESCE(goods.report_cpc_sd_cost,0) AS report_cpc_sd_cost ,
COALESCE(goods.report_reserved_field1,0) AS report_reserved_field1 ,
COALESCE(goods.report_reserved_field2,0) AS report_reserved_field2 ,
COALESCE(goods.report_cpc_sd_clicks,0) AS report_cpc_sd_clicks ,
COALESCE(goods.report_cpc_sp_clicks,0) AS report_cpc_sp_clicks ,
COALESCE(goods.report_sp_attributedconversions7d,0) AS report_sp_attributedconversions7d ,
COALESCE(goods.report_sd_attributedconversions7d,0) AS report_sd_attributedconversions7d ,
COALESCE(goods.report_sp_attributedsales7d,0) AS report_sp_attributedsales7d ,
COALESCE(goods.report_sd_attributedsales7d,0) AS report_sd_attributedsales7d ,
COALESCE(goods.report_sd_attributedconversions7dsamesku,0) AS report_sd_attributedconversions7dsamesku ,
COALESCE(goods.report_sp_attributedconversions7dsamesku,0) AS report_sp_attributedconversions7dsamesku ,
COALESCE(goods.report_sd_attributedsales7dsamesku,0) AS report_sd_attributedsales7dsamesku ,
COALESCE(goods.report_sp_attributedsales7dsamesku,0) AS report_sp_attributedsales7dsamesku ,
COALESCE(goods.report_return_and_return_commission,0) AS report_return_and_return_commission ,
COALESCE(goods.report_reserved_field17,0) AS report_reserved_field17 ,
COALESCE(goods.report_order_quantity,0) AS report_order_quantity ,
COALESCE(goods.report_reserved_field21,0) AS report_reserved_field21 ,
COALESCE(goods.report_fba_sales_volume,0) AS report_fba_sales_volume ,
COALESCE(goods.report_fbm_sales_volume,0) AS report_fbm_sales_volume ,
COALESCE(goods.report_fba_refund_num,0) AS report_fba_refund_num ,
COALESCE(goods.report_fbm_refund_num,0) AS report_fbm_refund_num ,
COALESCE(goods.report_fba_logistics_head_course,0) AS report_fba_logistics_head_course ,
COALESCE(goods.report_fba_sales_quota,0) AS report_fba_sales_quota ,
COALESCE(goods.report_fbm_sales_quota,0) AS report_fbm_sales_quota ,
COALESCE(goods.report_fba_refund,0) AS report_fba_refund ,
COALESCE(goods.report_fbm_refund,0) AS report_fbm_refund ,
COALESCE(goods.report_tax,0) AS report_tax ,
COALESCE(goods.report_ware_house_lost,0) AS report_ware_house_lost ,
COALESCE(goods.report_ware_house_damage,0) AS report_ware_house_damage ,
COALESCE(goods.report_shipping_charge,0) AS report_shipping_charge ,
COALESCE(goods.report_customer_damage,0) AS report_customer_damage ,
COALESCE(goods.report_removal_order_lost,0) AS report_removal_order_lost ,
COALESCE(goods.report_incorrect_fees_items,0) AS report_incorrect_fees_items ,
COALESCE(goods.report_missing_from_inbound,0) AS report_missing_from_inbound ,
COALESCE(goods.report_multichannel_order_lost,0) AS report_multichannel_order_lost ,
COALESCE(goods.report_removal_fee,0) AS report_removal_fee ,
COALESCE(goods.byorder_fbm_logistics_head_course,0) AS byorder_fbm_logistics_head_course ,
COALESCE(goods.report_fbm_logistics_head_course,0) AS report_fbm_logistics_head_course ,
COALESCE(goods.report_gift_wrap,0) AS report_gift_wrap ,".
//--bychannnel
//"COALESCE(goods.report_channel_profit,0) as report_channel_profit,
//COALESCE(goods.byorder_channel_profit,0) as byorder_channel_profit,
            "COALESCE(goods.byorder_channel_amazon_order_fee,0) as byorder_channel_amazon_order_fee,
COALESCE(goods.report_channel_amazon_order_fee,0) as report_channel_amazon_order_fee,
COALESCE(goods.byorder_channel_amazon_refund_fee,0) as byorder_channel_amazon_refund_fee,
COALESCE(goods.report_channel_amazon_refund_fee,0) as report_channel_amazon_refund_fee,
COALESCE(goods.byorder_channel_amazon_storage_fee,0) as byorder_channel_amazon_storage_fee,
COALESCE(goods.report_channel_amazon_storage_fee,0) as report_channel_amazon_storage_fee,
COALESCE(goods.byorder_channel_amazon_other_fee,0) as byorder_channel_amazon_other_fee,
COALESCE(goods.report_channel_amazon_other_fee,0) as report_channel_amazon_other_fee,
COALESCE(goods.byorder_channel_goods_adjustment_fee,0) as byorder_channel_goods_adjustment_fee,
COALESCE(goods.report_channel_goods_adjustment_fee,0) as report_channel_goods_adjustment_fee,
COALESCE(goods.report_channel_profit,0) as report_channel_profit,
COALESCE(goods.byorder_channel_profit,0) as byorder_channel_profit,

COALESCE(bychannel.bychannel_sales_quota,0) as bychannel_sales_quota,
COALESCE(bychannel.bychannel_fba_sales_quota,0) as bychannel_fba_sales_quota,
COALESCE(bychannel.bychannel_sales_volume,0) as bychannel_sales_volume,
COALESCE(bychannel.bychannel_fba_sales_volume,0) as bychannel_fba_sales_volume,
COALESCE(bychannel.bychannel_operating_fee,0) as bychannel_operating_fee,
COALESCE(bychannel.bychannel_fba_per_unit_fulfillment_fee,0) as bychannel_fba_per_unit_fulfillment_fee,
COALESCE(bychannel.bychannel_return_postage_billing_fuel_surcharge,0) as bychannel_return_postage_billing_fuel_surcharge,
COALESCE(bychannel.bychannel_postage_billing_tracking,0) as bychannel_postage_billing_tracking,
COALESCE(bychannel.bychannel_postage_billing_delivery_area_surcharge,0) as bychannel_postage_billing_delivery_area_surcharge,
COALESCE(bychannel.bychannel_postage_billing_vat,0) as bychannel_postage_billing_vat,
COALESCE(bychannel.bychannel_postage_billing_signature_confirmation,0) as bychannel_postage_billing_signature_confirmation,
COALESCE(bychannel.bychannel_return_postage_billing_oversize_surcharge,0) as bychannel_return_postage_billing_oversize_surcharge,
COALESCE(bychannel.bychannel_postage_billing_postage_adjustment,0) as bychannel_postage_billing_postage_adjustment,
COALESCE(bychannel.bychannel_postage_billing_insurance,0) as bychannel_postage_billing_insurance,
COALESCE(bychannel.bychannel_postage_billing_import_duty,0) as bychannel_postage_billing_import_duty,
COALESCE(bychannel.bychannel_postage_billing_fuel_surcharge,0) as bychannel_postage_billing_fuel_surcharge,
COALESCE(bychannel.bychannel_product_ads_payment_eventlist_charge,0) as bychannel_product_ads_payment_eventlist_charge,
COALESCE(bychannel.bychannel_product_ads_payment_eventlist_refund,0) as bychannel_product_ads_payment_eventlist_refund,
COALESCE(bychannel.bychannel_fba_storage_fee,0) as bychannel_fba_storage_fee,
COALESCE(bychannel.bychannel_review_enrollment_fee,0) as bychannel_review_enrollment_fee,
COALESCE(bychannel.bychannel_loan_payment,0) as bychannel_loan_payment,
COALESCE(bychannel.bychannel_debt_payment,0) as bychannel_debt_payment,
COALESCE(bychannel.bychannel_coupon_redemption_fee,0) as bychannel_coupon_redemption_fee,
COALESCE(bychannel.bychannel_run_lightning_deal_fee,0) as bychannel_run_lightning_deal_fee,
COALESCE(bychannel.bychannel_fba_inbound_convenience_fee,0) as bychannel_fba_inbound_convenience_fee,
COALESCE(bychannel.bychannel_labeling_fee,0) as bychannel_labeling_fee,
COALESCE(bychannel.bychannel_polybagging_fee,0) as bychannel_polybagging_fee,
COALESCE(bychannel.bychannel_fba_inbound_shipment_carton_level_info_fee,0) as bychannel_fba_inbound_shipment_carton_level_info_fee,
COALESCE(bychannel.bychannel_fba_inbound_transportation_program_fee,0) as bychannel_fba_inbound_transportation_program_fee,
COALESCE(bychannel.bychannel_fba_overage_fee,0) as bychannel_fba_overage_fee,
COALESCE(bychannel.bychannel_fba_inbound_transportation_fee,0) as bychannel_fba_inbound_transportation_fee,
COALESCE(bychannel.bychannel_fba_inbound_defect_fee,0) as bychannel_fba_inbound_defect_fee,
COALESCE(bychannel.bychannel_subscription,0) as bychannel_subscription,
COALESCE(bychannel.bychannel_charge_back_recovery,0) as bychannel_charge_back_recovery,
COALESCE(bychannel.bychannel_cs_error_non_itemized,0) as bychannel_cs_error_non_itemized,
COALESCE(bychannel.bychannel_return_postage_billing_postage,0) as bychannel_return_postage_billing_postage,
COALESCE(bychannel.bychannel_re_evaluation,0) as bychannel_re_evaluation,
COALESCE(bychannel.bychannel_subscription_fee_correction,0) as bychannel_subscription_fee_correction,
COALESCE(bychannel.bychannel_incorrect_fees_non_itemized,0) as bychannel_incorrect_fees_non_itemized,
COALESCE(bychannel.bychannel_buyer_recharge,0) as bychannel_buyer_recharge,
COALESCE(bychannel.bychannel_multichannel_order_late,0) as bychannel_multichannel_order_late,
COALESCE(bychannel.bychannel_non_subscription_fee_adj,0) as bychannel_non_subscription_fee_adj,
COALESCE(bychannel.bychannel_fba_disposal_fee,0) as bychannel_fba_disposal_fee,
COALESCE(bychannel.bychannel_fba_removal_fee,0) as bychannel_fba_removal_fee,
COALESCE(bychannel.bychannel_fba_long_term_storage_fee,0) as bychannel_fba_long_term_storage_fee,
COALESCE(bychannel.bychannel_fba_international_inbound_freight_fee,0) as bychannel_fba_international_inbound_freight_fee,
COALESCE(bychannel.bychannel_fba_international_inbound_freight_tax_and_duty,0) as bychannel_fba_international_inbound_freight_tax_and_duty,
COALESCE(bychannel.bychannel_postage_billing_transaction,0) as bychannel_postage_billing_transaction,
COALESCE(bychannel.bychannel_postage_billing_delivery_confirmation,0) as bychannel_postage_billing_delivery_confirmation,
COALESCE(bychannel.bychannel_postage_billing_postage,0) as bychannel_postage_billing_postage,
COALESCE(bychannel.bychannel_reserved_field1,0) as bychannel_reserved_field1,
COALESCE(bychannel.bychannel_reserved_field2,0) as bychannel_reserved_field2,
COALESCE(bychannel.bychannel_reserved_field3,0) as bychannel_reserved_field3,
COALESCE(bychannel.bychannel_reserved_field4,0) as bychannel_reserved_field4,
COALESCE(bychannel.bychannel_reserved_field5,0) as bychannel_reserved_field5,
COALESCE(bychannel.bychannel_reserved_field6,0) as bychannel_reserved_field6,
COALESCE(bychannel.bychannel_reserved_field7,0) as bychannel_reserved_field7,
COALESCE(bychannel.bychannel_reserved_field8,0) as bychannel_reserved_field8,
COALESCE(bychannel.bychannel_reserved_field9,0) as bychannel_reserved_field9,
COALESCE(bychannel.bychannel_reserved_field10,0) as bychannel_reserved_field10,
COALESCE(bychannel.bychannel_reserved_field11,0) as bychannel_reserved_field11,
COALESCE(bychannel.bychannel_reserved_field12,0) as bychannel_reserved_field12,
COALESCE(bychannel.bychannel_reserved_field13,0) as bychannel_reserved_field13,
COALESCE(bychannel.bychannel_reserved_field14,0) as bychannel_reserved_field14,
COALESCE(bychannel.bychannel_reserved_field15,0) as bychannel_reserved_field15,
COALESCE(bychannel.bychannel_reserved_field16,0) as bychannel_reserved_field16,
COALESCE(bychannel.bychannel_reserved_field17,0) as bychannel_reserved_field17,
COALESCE(bychannel.bychannel_reserved_field18,0) as bychannel_reserved_field18,
COALESCE(bychannel.bychannel_reserved_field19,0) as bychannel_reserved_field19,
COALESCE(bychannel.bychannel_reserved_field20,0) as bychannel_reserved_field20,
COALESCE(bychannel.bychannel_misc_adjustment,0) as bychannel_misc_adjustment,
COALESCE(bychannel.bychannel_cpc_sb_sales_volume,0) as bychannel_cpc_sb_sales_volume,
COALESCE(bychannel.bychannel_cpc_sb_sales_quota,0) as bychannel_cpc_sb_sales_quota,
COALESCE(bychannel.bychannel_cpc_sb_cost,0) as bychannel_cpc_sb_cost,
COALESCE(bychannel.bychannel_create_time,0) as bychannel_create_time,
COALESCE(bychannel.bychannel_modified_time,0) as bychannel_modified_time,
COALESCE(bychannel.bychannel_platform_type,0) as bychannel_platform_type,
COALESCE(bychannel.bychannel_mfnpostageFee,0) as bychannel_mfnpostageFee,
COALESCE(bychannel.bychannel_coupon_payment_eventList_tax,0) as bychannel_coupon_payment_eventList_tax,
COALESCE(bychannel.bychannel_channel_amazon_order_fee,0) as bychannel_channel_amazon_order_fee,
COALESCE(bychannel.bychannel_channel_amazon_refund_fee,0) as bychannel_channel_amazon_refund_fee,
COALESCE(bychannel.bychannel_channel_amazon_storage_fee,0) as bychannel_channel_amazon_storage_fee,
COALESCE(bychannel.bychannel_channel_amazon_other_fee,0) as bychannel_channel_amazon_other_fee,
COALESCE(bychannel.bychannel_channel_profit,0) as bychannel_channel_profit,
COALESCE(bychannel.bychannel_channel_goods_adjustment_fee,0) as bychannel_channel_goods_adjustment_fee,
COALESCE(bychannel.bychannel_reserved_field1,0) as channel_fbm_safe_t_claim_demage,
COALESCE(goods.channel_id ,bychannel.channel_id) AS channel_id ,
COALESCE(goods.site_id ,bychannel.site_id) AS site_id ,
COALESCE(goods.user_id ,bychannel.user_id) AS user_id ,
COALESCE(goods.myear ,bychannel.channel_id) AS myear ,
COALESCE(goods.mmonth ,bychannel.channel_id) AS mmonth ,
COALESCE(goods.goods_operation_user_admin_id ,0) AS goods_operation_user_admin_id ,
COALESCE(goods.create_time ,bychannel.bychannel_create_time) AS create_time ,
{$report_other_field}
COALESCE(goods.goods_operation_pattern ,2) AS goods_operation_pattern 
 from
        (SELECT
			SUM( dw_report.byorder_user_sessions ) AS byorder_user_sessions,
			SUM( dw_report.byorder_channel_amazon_order_fee ) AS byorder_channel_amazon_order_fee,
			SUM( dw_report.report_channel_amazon_order_fee ) AS report_channel_amazon_order_fee,
			SUM( dw_report.byorder_channel_amazon_refund_fee ) AS byorder_channel_amazon_refund_fee,
			SUM( dw_report.report_channel_amazon_refund_fee ) AS report_channel_amazon_refund_fee,
			SUM( dw_report.byorder_channel_amazon_storage_fee ) AS byorder_channel_amazon_storage_fee,
			SUM( dw_report.report_channel_amazon_storage_fee ) AS report_channel_amazon_storage_fee,
			SUM( dw_report.byorder_channel_amazon_other_fee ) AS byorder_channel_amazon_other_fee,
			SUM( dw_report.report_channel_amazon_other_fee ) AS report_channel_amazon_other_fee,
			SUM( dw_report.byorder_channel_goods_adjustment_fee ) AS byorder_channel_goods_adjustment_fee,
			SUM( dw_report.report_channel_goods_adjustment_fee ) AS report_channel_goods_adjustment_fee,
			SUM( dw_report.byorder_channel_profit ) AS byorder_channel_profit,
			SUM( dw_report.report_channel_profit ) AS report_channel_profit,
			SUM( dw_report.byorder_quantity_of_goods_ordered ) AS byorder_quantity_of_goods_ordered,
			SUM( dw_report.byorder_number_of_visits ) AS byorder_number_of_visits,
			SUM( dw_report.byorder_buy_button_winning_rate ) AS byorder_buy_button_winning_rate,
			SUM( dw_report.byorder_sales_volume ) AS byorder_sales_volume,
			SUM( dw_report.byorder_group_id ) AS byorder_group_id,
			SUM( dw_report.byorder_sales_quota ) AS byorder_sales_quota,
			SUM( dw_report.byorder_refund_num ) AS byorder_refund_num,
			SUM( dw_report.byorder_refund ) AS byorder_refund,
			SUM( dw_report.byorder_promote_discount ) AS byorder_promote_discount,
			SUM( dw_report.byorder_refund_promote_discount ) AS byorder_refund_promote_discount,
			SUM( dw_report.byorder_purchasing_cost ) AS byorder_purchasing_cost,
			SUM( dw_report.byorder_logistics_head_course ) AS byorder_logistics_head_course,
			SUM( dw_report.byorder_goods_profit ) AS byorder_goods_profit,
			SUM( dw_report.byorder_goods_amazon_fee ) AS byorder_goods_amazon_fee,
			SUM( dw_report.byorder_platform_sales_commission ) AS byorder_platform_sales_commission,
			SUM( dw_report.byorder_fba_generation_delivery_cost ) AS byorder_fba_generation_delivery_cost,
			SUM( dw_report.byorder_fbaperorderfulfillmentfee ) AS byorder_fbaperorderfulfillmentfee,
			SUM( dw_report.byorder_fbaweightbasedfee ) AS byorder_fbaweightbasedfee,
			SUM( dw_report.byorder_profit ) AS byorder_profit,
			SUM( dw_report.byorder_order_variableclosingfee ) AS byorder_order_variableclosingfee,
			SUM( dw_report.byorder_fixedclosingfee ) AS byorder_fixedclosingfee,
			SUM( dw_report.byorder_refund_variableclosingfee ) AS byorder_refund_variableclosingfee,
			SUM( dw_report.byorder_goods_amazon_other_fee ) AS byorder_goods_amazon_other_fee,
			SUM( dw_report.byorder_returnshipping ) AS byorder_returnshipping,
			SUM( dw_report.byorder_return_and_return_sales_commission ) AS byorder_return_and_return_sales_commission,
			SUM( dw_report.byorder_fba_refund_treatment_fee ) AS byorder_fba_refund_treatment_fee,
			SUM( dw_report.byorder_fbacustomerreturnperorderfee ) AS byorder_fbacustomerreturnperorderfee,
			SUM( dw_report.byorder_fbacustomerreturnweightbasedfee ) AS byorder_fbacustomerreturnweightbasedfee,
			SUM( dw_report.byorder_estimated_monthly_storage_fee ) AS byorder_estimated_monthly_storage_fee,
			SUM( dw_report.byorder_long_term_storage_fee ) AS byorder_long_term_storage_fee,
			SUM( dw_report.byorder_reserved_field16 ) AS byorder_reserved_field16,
			SUM( dw_report.byorder_reserved_field10 ) AS byorder_reserved_field10,
			SUM( dw_report.byorder_cpc_cost ) AS byorder_cpc_cost,
			SUM( dw_report.byorder_cpc_sd_cost ) AS byorder_cpc_sd_cost,
			SUM( dw_report.byorder_reserved_field1 ) AS byorder_reserved_field1,
			SUM( dw_report.byorder_reserved_field2 ) AS byorder_reserved_field2,
			SUM( dw_report.byorder_cpc_sd_clicks ) AS byorder_cpc_sd_clicks,
			SUM( dw_report.byorder_cpc_sp_clicks ) AS byorder_cpc_sp_clicks,
			SUM( dw_report.byorder_sp_attributedconversions7d ) AS byorder_sp_attributedconversions7d,
			SUM( dw_report.byorder_sd_attributedconversions7d ) AS byorder_sd_attributedconversions7d,
			SUM( dw_report.byorder_sp_attributedsales7d ) AS byorder_sp_attributedsales7d,
			SUM( dw_report.byorder_sd_attributedsales7d ) AS byorder_sd_attributedsales7d,
			SUM( dw_report.byorder_sd_attributedconversions7dsamesku ) AS byorder_sd_attributedconversions7dsamesku,
			SUM( dw_report.byorder_sp_attributedconversions7dsamesku ) AS byorder_sp_attributedconversions7dsamesku,
			SUM( dw_report.byorder_sd_attributedsales7dsamesku ) AS byorder_sd_attributedsales7dsamesku,
			SUM( dw_report.byorder_sp_attributedsales7dsamesku ) AS byorder_sp_attributedsales7dsamesku,
			SUM( dw_report.byorder_number_of_visits * dw_report.byorder_buy_button_winning_rate/100 ) AS byorder_buy_button_winning_num,
			SUM( dw_report.byorder_return_and_return_commission ) AS byorder_return_and_return_commission,
			SUM( dw_report.byorder_reserved_field17 ) AS byorder_reserved_field17,
			SUM( dw_report.byorder_order_quantity ) AS byorder_order_quantity,
			SUM( dw_report.byorder_reserved_field21 ) AS byorder_reserved_field21,
			SUM( dw_report.byorder_reserved_field11 ) AS byorder_fba_sales_volume,
			SUM( dw_report.byorder_sales_volume - dw_report.byorder_reserved_field11 ) AS byorder_fbm_sales_volume,
			SUM( dw_report.byorder_reserved_field12 ) AS byorder_fba_refund_num,
			SUM( dw_report.byorder_refund_num - dw_report.byorder_reserved_field12 ) AS byorder_fbm_refund_num,
			SUM( dw_report.byorder_reserved_field13 ) AS byorder_fba_logistics_head_course,
			SUM( case when dw_report.byorder_sales_volume=0 then 0 else dw_report.byorder_sales_quota*(dw_report.byorder_reserved_field11/(dw_report.byorder_sales_volume)) end  ) AS byorder_fba_sales_quota,
			SUM( case when dw_report.byorder_sales_volume=0 then 0 else dw_report.byorder_sales_quota-dw_report.byorder_sales_quota*(dw_report.byorder_reserved_field11/(dw_report.byorder_sales_volume)) end) AS byorder_fbm_sales_quota,
			SUM( case when dw_report.byorder_refund_num=0 then 0 else dw_report.byorder_refund*(dw_report.byorder_reserved_field12/dw_report.byorder_refund_num) end ) AS byorder_fba_refund,
			SUM( case when dw_report.byorder_refund_num=0 then 0 else dw_report.byorder_refund-byorder_refund*(dw_report.byorder_reserved_field12/dw_report.byorder_refund_num) end ) AS byorder_fbm_refund,
			SUM( dw_report.byorder_tax ) AS byorder_tax,
			SUM( dw_report.byorder_ware_house_lost ) AS byorder_ware_house_lost,
			SUM( dw_report.byorder_ware_house_damage ) AS byorder_ware_house_damage,
			SUM( dw_report.byorder_shipping_charge ) AS byorder_shipping_charge,
			SUM( dw_report.byorder_customer_damage ) AS byorder_customer_damage,
			SUM( dw_report.byorder_removal_order_lost ) AS byorder_removal_order_lost,
			SUM( dw_report.byorder_incorrect_fees_items ) AS byorder_incorrect_fees_items,
			SUM( dw_report.byorder_missing_from_inbound ) AS byorder_missing_from_inbound,
			SUM( dw_report.byorder_multichannel_order_lost ) AS byorder_multichannel_order_lost,
			SUM( dw_report.byorder_removal_fee ) AS byorder_removal_fee,
			SUM( dw_report.byorder_gift_wrap ) AS byorder_gift_wrap,
			SUM( dw_report.report_sales_volume ) AS report_sales_volume,
			SUM( dw_report.report_group_id ) AS report_group_id,
			SUM( dw_report.report_sales_quota ) AS report_sales_quota,
			SUM( dw_report.report_refund_num ) AS report_refund_num,
			SUM( dw_report.report_refund ) AS report_refund,
			SUM( dw_report.report_promote_discount ) AS report_promote_discount,
			SUM( dw_report.report_refund_promote_discount ) AS report_refund_promote_discount,
			SUM( dw_report.report_purchasing_cost ) AS report_purchasing_cost,
			SUM( dw_report.report_logistics_head_course ) AS report_logistics_head_course,
			SUM( dw_report.report_goods_profit ) AS report_goods_profit,
			SUM( dw_report.report_goods_amazon_fee ) AS report_goods_amazon_fee,
			SUM( dw_report.report_platform_sales_commission ) AS report_platform_sales_commission,
			SUM( dw_report.report_fba_generation_delivery_cost ) AS report_fba_generation_delivery_cost,
			SUM( dw_report.report_fbaperorderfulfillmentfee ) AS report_fbaperorderfulfillmentfee,
			SUM( dw_report.report_fbaweightbasedfee ) AS report_fbaweightbasedfee,
			SUM( dw_report.report_profit ) AS report_profit,
			SUM( dw_report.report_order_variableclosingfee ) AS report_order_variableclosingfee,
			SUM( dw_report.report_fixedclosingfee ) AS report_fixedclosingfee,
			SUM( dw_report.report_refund_variableclosingfee ) AS report_refund_variableclosingfee,
			SUM( dw_report.report_goods_amazon_other_fee ) AS report_goods_amazon_other_fee,
			SUM( dw_report.report_returnshipping ) AS report_returnshipping,
			SUM( dw_report.report_return_and_return_sales_commission ) AS report_return_and_return_sales_commission,
			SUM( dw_report.report_fba_refund_treatment_fee ) AS report_fba_refund_treatment_fee,
			SUM( dw_report.report_fbacustomerreturnperorderfee ) AS report_fbacustomerreturnperorderfee,
			SUM( dw_report.report_fbacustomerreturnweightbasedfee ) AS report_fbacustomerreturnweightbasedfee,
			SUM( dw_report.report_estimated_monthly_storage_fee ) AS report_estimated_monthly_storage_fee,
			SUM( dw_report.report_long_term_storage_fee ) AS report_long_term_storage_fee,
			SUM( dw_report.report_reserved_field16 ) AS report_reserved_field16,
			SUM( dw_report.report_reserved_field10 ) AS report_reserved_field10,
			SUM( dw_report.report_cpc_cost ) AS report_cpc_cost,
			SUM( dw_report.report_cpc_sd_cost ) AS report_cpc_sd_cost,
			SUM( dw_report.report_reserved_field1 ) AS report_reserved_field1,
			SUM( dw_report.report_reserved_field2 ) AS report_reserved_field2,
			SUM( dw_report.report_cpc_sd_clicks ) AS report_cpc_sd_clicks,
			SUM( dw_report.report_cpc_sp_clicks ) AS report_cpc_sp_clicks,
			SUM( dw_report.report_sp_attributedconversions7d ) AS report_sp_attributedconversions7d,
			SUM( dw_report.report_sd_attributedconversions7d ) AS report_sd_attributedconversions7d,
			SUM( dw_report.report_sp_attributedsales7d ) AS report_sp_attributedsales7d,
			SUM( dw_report.report_sd_attributedsales7d ) AS report_sd_attributedsales7d,
			SUM( dw_report.report_sd_attributedconversions7dsamesku ) AS report_sd_attributedconversions7dsamesku,
			SUM( dw_report.report_sp_attributedconversions7dsamesku ) AS report_sp_attributedconversions7dsamesku,
			SUM( dw_report.report_sd_attributedsales7dsamesku ) AS report_sd_attributedsales7dsamesku,
			SUM( dw_report.report_sp_attributedsales7dsamesku ) AS report_sp_attributedsales7dsamesku,
			SUM( dw_report.report_return_and_return_commission ) AS report_return_and_return_commission,
			SUM( dw_report.report_reserved_field17 ) AS report_reserved_field17,
			SUM( dw_report.report_order_quantity ) AS report_order_quantity,
			SUM( dw_report.report_reserved_field21 ) AS report_reserved_field21,
			SUM( dw_report.report_reserved_field11 ) AS report_fba_sales_volume,
			SUM( dw_report.report_sales_volume  - dw_report.report_reserved_field11) AS report_fbm_sales_volume,
			SUM( dw_report.report_reserved_field12 ) AS report_fba_refund_num,
			SUM( dw_report.report_refund_num - report_reserved_field12 ) AS report_fbm_refund_num,
			SUM( dw_report.report_reserved_field13 ) AS report_fba_logistics_head_course,
			SUM( case when dw_report.report_sales_volume=0 then 0 else dw_report.report_sales_quota*(dw_report.report_reserved_field11/(dw_report.report_sales_volume)) end) AS report_fba_sales_quota,
			SUM( case when dw_report.report_sales_volume=0 then 0 else dw_report.report_sales_quota-dw_report.report_sales_quota*(dw_report.report_reserved_field11/(dw_report.report_sales_volume)) end ) AS report_fbm_sales_quota,
			SUM( case when dw_report.report_refund_num=0 then 0 else dw_report.report_refund*(dw_report.report_reserved_field12/dw_report.report_refund_num) end ) AS report_fba_refund,
			SUM( case when dw_report.report_refund_num=0 then 0 else dw_report.report_refund-dw_report.report_refund*(dw_report.report_reserved_field12/dw_report.report_refund_num) end ) AS report_fbm_refund,
			SUM( dw_report.report_tax ) AS report_tax,
			SUM( dw_report.report_ware_house_lost ) AS report_ware_house_lost,
			SUM( dw_report.report_ware_house_damage ) AS report_ware_house_damage,
			SUM( dw_report.report_shipping_charge ) AS report_shipping_charge,
			SUM( dw_report.report_customer_damage ) AS report_customer_damage,
			SUM( dw_report.report_removal_order_lost ) AS report_removal_order_lost,
			SUM( dw_report.report_incorrect_fees_items ) AS report_incorrect_fees_items,
			SUM( dw_report.report_missing_from_inbound ) AS report_missing_from_inbound,
			SUM( dw_report.report_multichannel_order_lost ) AS report_multichannel_order_lost,
			SUM( dw_report.report_removal_fee ) AS report_removal_fee,
			SUM( dw_report.report_gift_wrap ) AS report_gift_wrap,
			SUM( dw_report.byorder_logistics_head_course - dw_report.byorder_logistics_head_course) AS byorder_fbm_logistics_head_course,
			SUM( dw_report.report_logistics_head_course  - dw_report.report_logistics_head_course) AS report_fbm_logistics_head_course,
			amazon_goods.goods_channel_id AS channel_id ,
			max(dw_report.byorder_myear) as myear ,
			max(dw_report.byorder_mmonth) as mmonth,
			{$goods_other_field}
			amazon_goods.goods_operation_user_admin_id,
			max(dw_report.byorder_create_time) as create_time,
			max(dw_report.byorder_user_id) as user_id,
			max(dw_report.byorder_site_id) as site_id,
			max(amazon_goods.channel_goods_operation_pattern) as goods_operation_pattern
		FROM
			{$goods_table}
			WHERE
		{$where_dw_report_amazon_goods}
		GROUP BY
			{$goods_group}
			) AS goods {$goods_month_table}
			full JOIN
		    {$channel_table}

) AS report ";

        $table = str_replace("dw_report.byorder_goods_profit","byorder_order_variableclosingfee+byorder_fixedclosingfee+byorder_refund_variableclosingfee+byorder_platform_sales_commission+byorder_fba_generation_delivery_cost
	+byorder_fbaperorderfulfillmentfee+byorder_fbaweightbasedfee-byorder_profit+byorder_profit+byorder_returnshipping+byorder_return_and_return_sales_commission+byorder_return_and_return_commission
	+byorder_fba_refund_treatment_fee+byorder_fbacustomerreturnperorderfee+byorder_fbacustomerreturnweightbasedfee+byorder_estimated_monthly_storage_fee+byorder_gift_wrap+byorder_restocking_fee
	+byorder_shipping_charge+byorder_shipping_charge_charge_back+byorder_shipping_tax+byorder_tax+byorder_gift_wrap_tax+byorder_refund_shipping_charge+byorder_refund_shipping_charge_charge_back
	+byorder_refund_shipping_tax+byorder_refund_tax+byorder_marketplace_facilitator_tax_shipping+byorder_marketplace_facilitator_tax_principal+byorder_order_lowvaluegoods_other
	+byorder_order_giftwrapchargeback+byorder_order_shippinghb+byorder_salestaxcollectionfee+byorder_costofpointsgranted+byorder_order_codchargeback+byorder_amazonexclusivesfee+byorder_giftwrapcommission
	+byorder_order_paymentmethodfee+byorder_cod_tax+byorder_refund_lowvaluegoods_other+byorder_marketplacefacilitatortax_restockingfee+byorder_goodwill+byorder_refund_paymentmethodfee
	+byorder_refund_codchargeback+byorder_refund_shippinghb+byorder_refund_giftwrapchargeback+byorder_costofpointsreturned+byorder_pointsadjusted+byorder_reserved_field3+byorder_reserved_field4
	+byorder_reserved_field6+byorder_reserved_field7+byorder_reserved_field8+byorder_reserved_field14+byorder_reserved_field15+byorder_reserved_field18+byorder_reserved_field19+byorder_reserved_field20
	+byorder_reserved_field21+byorder_ware_house_lost+byorder_ware_house_damage+byorder_ware_house_lost_manual+byorder_ware_house_damage_exception+byorder_reversal_reimbursement
	+byorder_compensated_clawback+byorder_customer_damage+byorder_sales_quota-byorder_refund+byorder_long_term_storage_fee+byorder_promote_discount
	+byorder_refund_promote_discount+byorder_cpc_cost+byorder_cpc_sd_cost+byorder_reserved_field10-byorder_reserved_field16
	+byorder_free_replacement_refund_items
	+byorder_removal_order_lost
	+byorder_incorrect_fees_items
	+byorder_missing_from_inbound_clawback
	+byorder_missing_from_inbound
	+byorder_inbound_carrier_damage
	+byorder_multichannel_order_lost
	+byorder_payment_retraction_items-byorder_reserved_field17+byorder_cpc_sb_sales_quota+byorder_cpc_sb_cost+byorder_refund_rate
	+byorder_profit_margin
	+byorder_disposal_fee
	+byorder_removal_fee",$table);
        $table = str_replace("dw_report.report_goods_profit","report_order_variableclosingfee+report_fixedclosingfee+report_refund_variableclosingfee+report_platform_sales_commission+report_fba_generation_delivery_cost
	+report_fbaperorderfulfillmentfee+report_fbaweightbasedfee-report_profit+report_profit+report_returnshipping+report_return_and_return_sales_commission+report_return_and_return_commission
	+report_fba_refund_treatment_fee+report_fbacustomerreturnperorderfee+report_fbacustomerreturnweightbasedfee+report_estimated_monthly_storage_fee+report_gift_wrap+report_restocking_fee
	+report_shipping_charge+report_shipping_charge_charge_back+report_shipping_tax+report_tax+report_gift_wrap_tax+report_refund_shipping_charge+report_refund_shipping_charge_charge_back
	+report_refund_shipping_tax+report_refund_tax+report_marketplace_facilitator_tax_shipping+report_marketplace_facilitator_tax_principal+report_order_lowvaluegoods_other
	+report_order_giftwrapchargeback+report_order_shippinghb+report_salestaxcollectionfee+report_costofpointsgranted+report_order_codchargeback+report_amazonexclusivesfee+report_giftwrapcommission
	+report_order_paymentmethodfee+report_cod_tax+report_refund_lowvaluegoods_other+report_marketplacefacilitatortax_restockingfee+report_goodwill+report_refund_paymentmethodfee
	+report_refund_codchargeback+report_refund_shippinghb+report_refund_giftwrapchargeback+report_costofpointsreturned+report_pointsadjusted+report_reserved_field3+report_reserved_field4
	+report_reserved_field6+report_reserved_field7+report_reserved_field8+report_reserved_field14+report_reserved_field15+report_reserved_field18+report_reserved_field19+report_reserved_field20
	+report_reserved_field21+report_ware_house_lost+report_ware_house_damage+report_ware_house_lost_manual+report_ware_house_damage_exception+report_reversal_reimbursement
	+report_compensated_clawback+report_customer_damage+report_sales_quota-report_refund+report_long_term_storage_fee+report_promote_discount
	+report_refund_promote_discount+report_cpc_cost+report_cpc_sd_cost+report_reserved_field10-report_reserved_field16
	+report_free_replacement_refund_items
	+report_removal_order_lost
	+report_incorrect_fees_items
	+report_missing_from_inbound_clawback
	+report_missing_from_inbound
	+report_inbound_carrier_damage
	+report_multichannel_order_lost
	+report_payment_retraction_items-report_reserved_field17+report_cpc_sb_sales_quota+report_cpc_sb_cost+report_refund_rate
	+report_profit_margin
	+report_disposal_fee
	+report_removal_fee",$table);
        $table = str_replace("dw_report.byorder_goods_amazon_fee","byorder_order_variableclosingfee+byorder_fixedclosingfee+byorder_refund_variableclosingfee+byorder_platform_sales_commission+byorder_fba_generation_delivery_cost
	+byorder_fbaperorderfulfillmentfee+byorder_fbaweightbasedfee-byorder_profit+byorder_profit+byorder_returnshipping+byorder_return_and_return_sales_commission+byorder_return_and_return_commission
	+byorder_fba_refund_treatment_fee+byorder_fbacustomerreturnperorderfee+byorder_fbacustomerreturnweightbasedfee+byorder_estimated_monthly_storage_fee+byorder_gift_wrap+byorder_restocking_fee
	+byorder_shipping_charge+byorder_shipping_charge_charge_back+byorder_shipping_tax+byorder_tax+byorder_gift_wrap_tax+byorder_refund_shipping_charge+byorder_refund_shipping_charge_charge_back
	+byorder_refund_shipping_tax+byorder_refund_tax+byorder_marketplace_facilitator_tax_shipping+byorder_marketplace_facilitator_tax_principal+byorder_order_lowvaluegoods_other
	+byorder_order_giftwrapchargeback+byorder_order_shippinghb+byorder_salestaxcollectionfee+byorder_costofpointsgranted+byorder_order_codchargeback+byorder_amazonexclusivesfee+byorder_giftwrapcommission
	+byorder_order_paymentmethodfee+byorder_cod_tax+byorder_refund_lowvaluegoods_other+byorder_marketplacefacilitatortax_restockingfee+byorder_goodwill+byorder_refund_paymentmethodfee
	+byorder_refund_codchargeback+byorder_refund_shippinghb+byorder_refund_giftwrapchargeback+byorder_costofpointsreturned+byorder_pointsadjusted+byorder_reserved_field3+byorder_reserved_field4
	+byorder_reserved_field6+byorder_reserved_field7+byorder_reserved_field8+byorder_reserved_field14+byorder_reserved_field15+byorder_reserved_field18+byorder_reserved_field19+byorder_reserved_field20
	+byorder_reserved_field21+byorder_ware_house_lost+byorder_ware_house_damage+byorder_ware_house_lost_manual+byorder_ware_house_damage_exception+byorder_reversal_reimbursement
	+byorder_compensated_clawback+byorder_customer_damage+byorder_long_term_storage_fee
	+byorder_free_replacement_refund_items
	+byorder_removal_order_lost
	+byorder_incorrect_fees_items
	+byorder_missing_from_inbound_clawback
	+byorder_missing_from_inbound
	+byorder_inbound_carrier_damage
	+byorder_multichannel_order_lost
	+byorder_payment_retraction_items
	+byorder_cpc_sb_sales_quota
	+byorder_cpc_sb_cost
	+byorder_refund_rate
	+byorder_profit_margin
	+byorder_disposal_fee
	+byorder_removal_fee",$table);
        $table = str_replace("dw_report.report_goods_amazon_fee","report_order_variableclosingfee+report_fixedclosingfee+report_refund_variableclosingfee+report_platform_sales_commission+report_fba_generation_delivery_cost
	+report_fbaperorderfulfillmentfee+report_fbaweightbasedfee-report_profit+report_profit+report_returnshipping+report_return_and_return_sales_commission+report_return_and_return_commission
	+report_fba_refund_treatment_fee+report_fbacustomerreturnperorderfee+report_fbacustomerreturnweightbasedfee+report_estimated_monthly_storage_fee+report_gift_wrap+report_restocking_fee
	+report_shipping_charge+report_shipping_charge_charge_back+report_shipping_tax+report_tax+report_gift_wrap_tax+report_refund_shipping_charge+report_refund_shipping_charge_charge_back
	+report_refund_shipping_tax+report_refund_tax+report_marketplace_facilitator_tax_shipping+report_marketplace_facilitator_tax_principal+report_order_lowvaluegoods_other
	+report_order_giftwrapchargeback+report_order_shippinghb+report_salestaxcollectionfee+report_costofpointsgranted+report_order_codchargeback+report_amazonexclusivesfee+report_giftwrapcommission
	+report_order_paymentmethodfee+report_cod_tax+report_refund_lowvaluegoods_other+report_marketplacefacilitatortax_restockingfee+report_goodwill+report_refund_paymentmethodfee
	+report_refund_codchargeback+report_refund_shippinghb+report_refund_giftwrapchargeback+report_costofpointsreturned+report_pointsadjusted+report_reserved_field3+report_reserved_field4
	+report_reserved_field6+report_reserved_field7+report_reserved_field8+report_reserved_field14+report_reserved_field15+report_reserved_field18+report_reserved_field19+report_reserved_field20
	+report_reserved_field21+report_ware_house_lost+report_ware_house_damage+report_ware_house_lost_manual+report_ware_house_damage_exception+report_reversal_reimbursement
	+report_compensated_clawback+report_customer_damage+report_long_term_storage_fee
	+report_free_replacement_refund_items
	+report_removal_order_lost
	+report_incorrect_fees_items
	+report_missing_from_inbound_clawback
	+report_missing_from_inbound
	+report_inbound_carrier_damage
	+report_multichannel_order_lost
	+report_payment_retraction_items
	+report_cpc_sb_sales_quota
	+report_cpc_sb_cost
	+report_refund_rate
	+report_profit_margin
	+report_disposal_fee
	+report_removal_fee",$table);
        $table = str_replace("dw_report.byorder_goods_amazon_other_fee","byorder_gift_wrap+byorder_restocking_fee+byorder_shipping_charge+byorder_shipping_charge_charge_back+byorder_shipping_tax+byorder_tax+byorder_gift_wrap_tax
	+byorder_refund_shipping_charge+byorder_refund_shipping_charge_charge_back+byorder_refund_shipping_tax+byorder_refund_tax+byorder_marketplace_facilitator_tax_shipping
	+byorder_marketplace_facilitator_tax_principal+byorder_order_lowvaluegoods_other+byorder_order_giftwrapchargeback+byorder_order_shippinghb+byorder_salestaxcollectionfee
	+byorder_costofpointsgranted+byorder_order_codchargeback+byorder_amazonexclusivesfee+byorder_giftwrapcommission+byorder_order_paymentmethodfee+byorder_cod_tax+byorder_refund_lowvaluegoods_other
	+byorder_marketplacefacilitatortax_restockingfee+byorder_goodwill+byorder_refund_paymentmethodfee+byorder_refund_codchargeback+byorder_refund_shippinghb+byorder_refund_giftwrapchargeback
	+byorder_costofpointsreturned+byorder_pointsadjusted+byorder_reserved_field3+byorder_reserved_field4+byorder_reserved_field6+byorder_reserved_field7+byorder_reserved_field8+byorder_reserved_field14
	+byorder_reserved_field15+byorder_reserved_field18+byorder_reserved_field19+byorder_reserved_field20+byorder_ware_house_lost+byorder_ware_house_damage
	+byorder_ware_house_lost_manual+byorder_ware_house_damage_exception+byorder_reversal_reimbursement+byorder_compensated_clawback+byorder_customer_damage
	+byorder_free_replacement_refund_items
	+byorder_removal_order_lost
	+byorder_incorrect_fees_items
	+byorder_missing_from_inbound_clawback
	+byorder_missing_from_inbound
	+byorder_inbound_carrier_damage
	+byorder_multichannel_order_lost
	+byorder_payment_retraction_items
	+byorder_cpc_sb_sales_quota
	+byorder_cpc_sb_cost
	+byorder_refund_rate
	+byorder_profit_margin
	+byorder_disposal_fee+byorder_cpc_sb_cost",$table);
        $table = str_replace("dw_report.report_goods_amazon_other_fee","report_gift_wrap+report_restocking_fee+report_shipping_charge+report_shipping_charge_charge_back+report_shipping_tax+report_tax+report_gift_wrap_tax
	+report_refund_shipping_charge+report_refund_shipping_charge_charge_back+report_refund_shipping_tax+report_refund_tax+report_marketplace_facilitator_tax_shipping
	+report_marketplace_facilitator_tax_principal+report_order_lowvaluegoods_other+report_order_giftwrapchargeback+report_order_shippinghb+report_salestaxcollectionfee
	+report_costofpointsgranted+report_order_codchargeback+report_amazonexclusivesfee+report_giftwrapcommission+report_order_paymentmethodfee+report_cod_tax+report_refund_lowvaluegoods_other
	+report_marketplacefacilitatortax_restockingfee+report_goodwill+report_refund_paymentmethodfee+report_refund_codchargeback+report_refund_shippinghb+report_refund_giftwrapchargeback
	+report_costofpointsreturned+report_pointsadjusted+report_reserved_field3+report_reserved_field4+report_reserved_field6+report_reserved_field7+report_reserved_field8+report_reserved_field14
	+report_reserved_field15+report_reserved_field18+report_reserved_field19+report_reserved_field20+report_ware_house_lost+report_ware_house_damage
	+report_ware_house_lost_manual+report_ware_house_damage_exception+report_reversal_reimbursement+report_compensated_clawback+report_customer_damage
	+report_free_replacement_refund_items
	+report_removal_order_lost
	+report_incorrect_fees_items
	+report_missing_from_inbound_clawback
	+report_missing_from_inbound
	+report_inbound_carrier_damage
	+report_multichannel_order_lost
	+report_payment_retraction_items
	+report_cpc_sb_sales_quota
	+report_cpc_sb_cost
	+report_refund_rate
	+report_profit_margin
	+report_disposal_fee
	+report_removal_fee+report_cpc_sb_cost",$table);

        $table = str_replace("dw_report.byorder_channel_amazon_order_fee","byorder_platform_sales_commission+byorder_fba_generation_delivery_cost
	+byorder_fbaperorderfulfillmentfee+byorder_fbaweightbasedfee+byorder_order_variableclosingfee+byorder_fixedclosingfee
	+byorder_reserved_field20+byorder_reserved_field21+byorder_disposal_fee",$table);
        $table = str_replace("dw_report.report_channel_amazon_order_fee","report_platform_sales_commission+report_fba_generation_delivery_cost
	+report_fbaperorderfulfillmentfee+report_fbaweightbasedfee+report_order_variableclosingfee+report_fixedclosingfee
	+report_reserved_field20+report_reserved_field21+report_disposal_fee",$table);

        $table = str_replace("dw_report.byorder_channel_amazon_refund_fee","byorder_return_and_return_commission+byorder_fba_refund_treatment_fee
	+byorder_return_and_return_sales_commission+byorder_returnshipping+byorder_refund_variableclosingfee
	+byorder_fbacustomerreturnperorderfee+byorder_fbacustomerreturnweightbasedfee",$table);
        $table = str_replace("dw_report.report_channel_amazon_refund_fee","report_return_and_return_commission+report_fba_refund_treatment_fee
	+report_return_and_return_sales_commission+report_returnshipping+report_refund_variableclosingfee
	+report_fbacustomerreturnperorderfee+report_fbacustomerreturnweightbasedfee",$table);

        $table = str_replace("dw_report.byorder_channel_amazon_storage_fee","byorder_restocking_fee+byorder_removal_fee",$table);
        $table = str_replace("dw_report.report_channel_amazon_storage_fee","report_restocking_fee+report_removal_fee",$table);
        $table = str_replace("dw_report.byorder_channel_amazon_other_fee","byorder_gift_wrap+byorder_shipping_charge+byorder_shipping_charge_charge_back+byorder_shipping_tax+byorder_tax+byorder_gift_wrap_tax
	+byorder_refund_shipping_charge+byorder_refund_shipping_charge_charge_back+byorder_refund_shipping_tax+byorder_refund_tax
	+byorder_marketplace_facilitator_tax_shipping+byorder_marketplace_facilitator_tax_principal+byorder_order_lowvaluegoods_other
	+byorder_order_giftwrapchargeback+byorder_order_shippinghb+byorder_salestaxcollectionfee+byorder_costofpointsgranted+byorder_order_codchargeback
	+byorder_amazonexclusivesfee+byorder_giftwrapcommission+byorder_order_paymentmethodfee+byorder_cod_tax+byorder_refund_lowvaluegoods_other
	+byorder_marketplacefacilitatortax_restockingfee+byorder_goodwill+byorder_refund_paymentmethodfee+byorder_refund_codchargeback
	+byorder_refund_shippinghb+byorder_refund_giftwrapchargeback+byorder_costofpointsreturned+byorder_pointsadjusted+byorder_reserved_field3
	+byorder_reserved_field4+byorder_reserved_field6+byorder_reserved_field7+byorder_reserved_field8+byorder_reserved_field14+byorder_reserved_field15+byorder_reserved_field18+byorder_cpc_sb_sales_quota+byorder_cpc_sb_cost
	+byorder_refund_rate
	+byorder_profit_margin",$table);
        $table = str_replace("dw_report.report_channel_amazon_other_fee","report_gift_wrap+report_shipping_charge+report_shipping_charge_charge_back+report_shipping_tax+report_tax+report_gift_wrap_tax
	+report_refund_shipping_charge+report_refund_shipping_charge_charge_back+report_refund_shipping_tax+report_refund_tax
	+report_marketplace_facilitator_tax_shipping+report_marketplace_facilitator_tax_principal+report_order_lowvaluegoods_other
	+report_order_giftwrapchargeback+report_order_shippinghb+report_salestaxcollectionfee+report_costofpointsgranted+report_order_codchargeback
	+report_amazonexclusivesfee+report_giftwrapcommission+report_order_paymentmethodfee+report_cod_tax+report_refund_lowvaluegoods_other
	+report_marketplacefacilitatortax_restockingfee+report_goodwill+report_refund_paymentmethodfee+report_refund_codchargeback
	+report_refund_shippinghb+report_refund_giftwrapchargeback+report_costofpointsreturned+report_pointsadjusted+report_reserved_field3
	+report_reserved_field4+report_reserved_field6+report_reserved_field7+report_reserved_field8+report_reserved_field14+report_reserved_field15+report_reserved_field18+report_cpc_sb_sales_quota+report_cpc_sb_cost
	+report_refund_rate
	+report_profit_margin",$table);
        $table = str_replace("dw_report.byorder_channel_goods_adjustment_fee","byorder_ware_house_lost+byorder_ware_house_damage+byorder_ware_house_lost_manual+byorder_ware_house_damage_exception
	+byorder_reversal_reimbursement+byorder_compensated_clawback+byorder_free_replacement_refund_items+byorder_removal_order_lost+byorder_incorrect_fees_items
	+byorder_missing_from_inbound_clawback+byorder_missing_from_inbound+byorder_inbound_carrier_damage+byorder_multichannel_order_lost+byorder_payment_retraction_items+byorder_reserved_field19",$table);
        $table = str_replace("dw_report.report_channel_goods_adjustment_fee","report_ware_house_lost+report_ware_house_damage+report_ware_house_lost_manual+report_ware_house_damage_exception
	+report_reversal_reimbursement+report_compensated_clawback+report_free_replacement_refund_items+report_removal_order_lost+report_incorrect_fees_items
	+report_missing_from_inbound_clawback+report_missing_from_inbound+report_inbound_carrier_damage+report_multichannel_order_lost+report_payment_retraction_items+report_reserved_field19",$table);
        $table = str_replace("dw_report.byorder_channel_profit","byorder_platform_sales_commission+byorder_fba_generation_delivery_cost
	+byorder_fbaperorderfulfillmentfee+byorder_fbaweightbasedfee+byorder_order_variableclosingfee+byorder_fixedclosingfee
	+byorder_reserved_field20+byorder_reserved_field21+byorder_return_and_return_commission+byorder_fba_refund_treatment_fee
	+byorder_return_and_return_sales_commission+byorder_returnshipping+byorder_refund_variableclosingfee
	+byorder_fbacustomerreturnperorderfee+byorder_fbacustomerreturnweightbasedfee+byorder_restocking_fee
	+byorder_gift_wrap+byorder_shipping_charge+byorder_shipping_charge_charge_back+byorder_shipping_tax+byorder_tax+byorder_gift_wrap_tax
	+byorder_refund_shipping_charge+byorder_refund_shipping_charge_charge_back+byorder_refund_shipping_tax+byorder_refund_tax
	+byorder_marketplace_facilitator_tax_shipping+byorder_marketplace_facilitator_tax_principal+byorder_order_lowvaluegoods_other
	+byorder_order_giftwrapchargeback+byorder_order_shippinghb+byorder_salestaxcollectionfee+byorder_costofpointsgranted+byorder_order_codchargeback
	+byorder_amazonexclusivesfee+byorder_giftwrapcommission+byorder_order_paymentmethodfee+byorder_cod_tax+byorder_refund_lowvaluegoods_other
	+byorder_marketplacefacilitatortax_restockingfee+byorder_goodwill+byorder_refund_paymentmethodfee+byorder_refund_codchargeback
	+byorder_refund_shippinghb+byorder_refund_giftwrapchargeback+byorder_costofpointsreturned+byorder_pointsadjusted+byorder_reserved_field3
	+byorder_reserved_field4+byorder_reserved_field6+byorder_reserved_field7+byorder_reserved_field8+byorder_reserved_field14+byorder_reserved_field15+byorder_reserved_field18+byorder_cpc_sb_sales_quota+byorder_cpc_sb_cost
	+byorder_sales_quota-byorder_refund+byorder_promote_discount+byorder_refund_promote_discount+byorder_ware_house_lost+byorder_ware_house_damage+byorder_ware_house_lost_manual
	+byorder_ware_house_damage_exception+byorder_reversal_reimbursement+byorder_compensated_clawback+byorder_free_replacement_refund_items+byorder_removal_order_lost
	+byorder_incorrect_fees_items+byorder_missing_from_inbound_clawback+byorder_missing_from_inbound+byorder_inbound_carrier_damage+byorder_multichannel_order_lost
	+byorder_payment_retraction_items+byorder_reserved_field19+byorder_reserved_field10-byorder_reserved_field17
	+byorder_refund_rate
	+byorder_profit_margin
	+byorder_disposal_fee+byorder_removal_fee",$table);
        $table = str_replace("dw_report.report_channel_profit","report_platform_sales_commission+report_fba_generation_delivery_cost
	+report_fbaperorderfulfillmentfee+report_fbaweightbasedfee+report_order_variableclosingfee+report_fixedclosingfee
	+report_reserved_field20+report_reserved_field21+report_return_and_return_commission+report_fba_refund_treatment_fee
	+report_return_and_return_sales_commission+report_returnshipping+report_refund_variableclosingfee
	+report_fbacustomerreturnperorderfee+report_fbacustomerreturnweightbasedfee+report_restocking_fee
	+report_gift_wrap+report_shipping_charge+report_shipping_charge_charge_back+report_shipping_tax+report_tax+report_gift_wrap_tax
	+report_refund_shipping_charge+report_refund_shipping_charge_charge_back+report_refund_shipping_tax+report_refund_tax
	+report_marketplace_facilitator_tax_shipping+report_marketplace_facilitator_tax_principal+report_order_lowvaluegoods_other
	+report_order_giftwrapchargeback+report_order_shippinghb+report_salestaxcollectionfee+report_costofpointsgranted+report_order_codchargeback
	+report_amazonexclusivesfee+report_giftwrapcommission+report_order_paymentmethodfee+report_cod_tax+report_refund_lowvaluegoods_other
	+report_marketplacefacilitatortax_restockingfee+report_goodwill+report_refund_paymentmethodfee+report_refund_codchargeback
	+report_refund_shippinghb+report_refund_giftwrapchargeback+report_costofpointsreturned+report_pointsadjusted+report_reserved_field3
	+report_reserved_field4+report_reserved_field6+report_reserved_field7+report_reserved_field8+report_reserved_field14+report_reserved_field15+report_reserved_field18+report_cpc_sb_sales_quota+report_cpc_sb_cost
	+report_sales_quota-report_refund+report_promote_discount+report_refund_promote_discount+report_ware_house_lost+report_ware_house_damage+report_ware_house_lost_manual
	+report_ware_house_damage_exception+report_reversal_reimbursement+report_compensated_clawback+report_free_replacement_refund_items+report_removal_order_lost
	+report_incorrect_fees_items+report_missing_from_inbound_clawback+report_missing_from_inbound+report_inbound_carrier_damage+report_multichannel_order_lost
	+report_payment_retraction_items+report_reserved_field19+report_reserved_field10-report_reserved_field17
	+report_refund_rate
	+report_profit_margin
	+report_disposal_fee+report_removal_fee",$table);
//        $table = str_replace("dw_report.byorder_goods_profit","",$table);
//        $table = str_replace("dw_report.byorder_goods_profit","",$table);
//        $table = str_replace("dw_report.byorder_goods_profit","",$table);


        return $table;

    }

    /**
     * 是否读月报的数据
     * @param $datas
     * @return bool
     */
    public function is_month_table($datas){
        if($datas['count_periods'] == 3 || $datas['count_periods'] == 4 || $datas['count_periods'] == 5 ){
            return true;
        }else if($datas['cost_count_type'] == 2){//先进先出只能读取月报
            return true;
        }

        return false;
    }

}
