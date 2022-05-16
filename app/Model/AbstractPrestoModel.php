<?php

namespace App\Model;

use App\Lib\Athena;
use Captainbi\Hyperf\Util\Redis;
use RuntimeException;

use App\Lib\Presto;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use GuzzleHttp\ClientInterface;

use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Logger\LoggerFactory;
use Hyperf\DbConnection\Db;

abstract class AbstractPrestoModel implements BIModelInterface
{
    use BIModelDefaultCacheTrait;

    protected static $detectSchemaName = '';

    protected static $tableMaps = [
        'table_channel' => 'ods.ods_dataark_b_channel',
        'table_area' => 'ods.ods_dataark_b_site_area',
        'table_site_rate' => 'ods.ods_dataark_b_site_rate',
        'table_user_department' => 'ods.ods_dataark_b_user_department',
        'table_amazon_goods_isku' => 'ods.ods_dataark_f_amazon_goods_isku_001',
        'table_amazon_goods_finance' => 'ods.ods_dataark_f_amazon_goods_finance_001',
        'table_amazon_goods_tags' => 'ods.ods_dataark_g_amazon_goods_tags_001',
        'table_amazon_goods_tags_rel' => 'ods.ods_dataark_g_amazon_goods_tags_rel_001',
        'table_amazon_fba_inventory_by_channel' => 'ods.ods_dataark_f_amazon_fba_inventory_by_channel_001',
        'table_amazon_goods_finance_report_by_order' => 'ods.ods_dataark_f_amazon_goods_finance_report_by_order_001',
        'table_channel_monthly_profit_report' => 'ods.ods_dataark_f_monthly_profit_report_001',
        'table_monthly_profit_report_by_sku' => 'ods.ods_dataark_f_monthly_profit_report_by_sku_001',

        'table_amazon_fba_inventory_v3' => 'ods.ods_g_amazon_fba_inventory_v3_001',
        'table_amazon_fba_inventory_v3_rel' => 'ods.ods_g_amazon_fba_inventory_v3_rel_001',
        'table_amazon_fba_inventory_tend_v3' => 'ods.ods_g_amazon_fba_inventory_v3_tend_001',

        'table_user_channel' => 'dim.dim_dataark_b_user_channel',
        'table_department_channel' => 'dim.dim_dataark_b_department_channel',
        'table_goods_dim_report' => 'dim.dim_dataark_f_dw_goods_dim_report_{DBHOST}',

        'table_goods_day_report' => 'dws.dws_dataark_f_dw_goods_day_report_{DWSDBHOST} AS report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_channel_day_report' => 'dws.dws_dataark_f_dw_channel_day_report_{DWSDBHOST}',
        'table_goods_week_report' => 'dws.dws_dataark_f_dw_goods_day_report_{DWSDBHOST} AS week_report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_goods_month_report' => 'dws.dws_dataark_f_dw_goods_month_report_slave_bigusers_{DWSDBHOST} AS report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_channel_week_report' => 'dws.dws_dataark_f_dw_channel_week_report_{DWSDBHOST}' ,
        'table_channel_month_report' => 'dws.dws_dataark_f_dw_channel_month_report_slave_bigusers_{DWSDBHOST}' ,
        'table_operation_day_report' => 'dws.dws_dataark_f_dw_operation_day_report_{DWSDBHOST}' ,
        'table_operation_week_report' => 'dws.dws_dataark_f_dw_operation_week_report_{DWSDBHOST}',
        'table_operation_month_report' => 'dws.dws_dataark_f_dw_operation_month_report_slave_{DWSDBHOST}',

        'table_dwd_goods_report' => 'dwd.dwd_dataark_f_dw_goods_report_{DBHOST}',
        'table_dws_goods_day_report' => 'dws.dws_dataark_f_dw_goods_day_report_{DWSDBHOST}',
        'table_dws_goods_month_report' => 'dws.dws_dataark_f_dw_goods_month_report_slave_bigusers_{DWSDBHOST}',

        'table_dws_idm_category01_topn_kpi' => 'dwsslave.dws_dataark_idm_category01_topn_kpi',
        'table_dws_idm_category02_topn_kpi' => 'dwsslave.dws_dataark_idm_category02_topn_kpi',
        'table_dws_idm_category03_topn_kpi' => 'dwsslave.dws_dataark_idm_category03_topn_kpi',

        'table_dws_arkdata_category01_month' => 'dwsslave.dws_arkdata_category01_month',
        'table_dws_arkdata_category02_month' => 'dwsslave.dws_arkdata_category02_month',
        'table_dws_arkdata_category03_month' => 'dwsslave.dws_arkdata_category03_month',

        'table_erp_storage_warehouse_isku' => 'ods.ods_e_erp_storage_warehouse_isku_001',
        'table_erp_storage_inventory_warehouse_report' => 'ods.ods_e_erp_storage_inventory_warehouse_report_001',
    ];


    protected $goodsCols = array(
        "goods_g_amazon_goods_id"=>"goods_g_amazon_goods_id",
        "goods_title"=>"goods_title",
        "goods_site_id"=>"goods_site_id",
        "goods_user_id"=>"goods_user_id",
        "goods_channel_id"=>"goods_channel_id",
        "goods_logistics_head_course"=>"goods_logistics_head_course",
        "goods_from_logistics_head_course"=>"goods_from_logistics_head_course",
        "goods_Transport_mode"=>"goods_Transport_mode",
        "goods_asin"=>"goods_asin",
        "goods_parent_asin"=>"goods_parent_asin",
        "goods_is_parent"=>"goods_is_parent",
        "goods_image"=>"goods_image",
        "goods_sku"=>"goods_sku",
        "goods_fnsku"=>"goods_fnsku",
        "goods_create_time"=>"goods_create_time",
        "goods_modified_time"=>"goods_modified_time",
        "goods_price"=>"goods_price",
        "goods_purchasing_cost"=>"goods_purchasing_cost",
        "goods_from_purchasing_cost"=>"goods_from_purchasing_cost",
        "goods_exchang_rate"=>"goods_exchang_rate",
        "goods_fbm_logistics_head_course"=>"goods_fbm_logistics_head_course",
        "goods_fbm_from_logistics_head_course"=>"goods_fbm_from_logistics_head_course",
        "goods_fbm_purchasing_cost"=>"goods_fbm_purchasing_cost",
        "goods_fbm_from_purchasing_cost"=>"goods_fbm_from_purchasing_cost",
        "goods_up_status"=>"goods_up_status",
        "goods_is_new"=>"goods_is_new",
        "goods_is_care"=>"goods_is_care",
        "goods_is_remarks"=>"goods_is_remarks",
        "goods_is_keyword"=>"goods_is_keyword",
        "goods_set_time"=>"goods_set_time",
        "goods_is_set_business"=>"goods_is_set_business",
        "goods_tag_id"=>"goods_tag_id",
        "goods_remark"=>"goods_remark",
        "goods_group_id"=>"goods_group_id",
        "goods_group_name"=>"group_group_name",
        "goods_group"=>"goods_group",
        "goods_get_image"=>"goods_get_image",
        "goods_is_set"=>"goods_is_set",
        "goods_is_sync"=>"goods_is_sync",
        "goods_sale_nums_y"=>"goods_sale_nums_y",
        "goods_sale_nums_7"=>"goods_sale_nums_7",
        "goods_sale_nums_14"=>"goods_sale_nums_14",
        "goods_sale_nums_30"=>"goods_sale_nums_30",
        "goods_sale_nums"=>"goods_sale_nums",
        "goods_sale_nums_before"=>"goods_sale_nums_before",
        "goods_sale_amount_y"=>"goods_sale_amount_y",
        "goods_sale_amount_7"=>"goods_sale_amount_7",
        "goods_sale_amount_14"=>"goods_sale_amount_14",
        "goods_sale_amount_30"=>"goods_sale_amount_30",
        "goods_sale_amount"=>"goods_sale_amount",
        "goods_sale_amount_before"=>"goods_sale_amount_before",
        "goods_care_remark_new_time"=>"goods_care_remark_new_time",
        "goods_is_get_category"=>"goods_is_get_category",
        "goods_product_category_name_1"=>"goods_product_category_name_1",
        "goods_product_category_id_1"=>"goods_product_category_id_1",
        "goods_product_category_name_2"=>"goods_product_category_name_2",
        "goods_product_category_id_2"=>"goods_product_category_id_2",
        "goods_product_category_name_3"=>"goods_product_category_name_3",
        "goods_product_category_id_3"=>"goods_product_category_id_3",
        "goods_month_time"=>"goods_month_time",
        "goods_isku_id"=>"goods_isku_id",
        "goods_operation_user_admin_id"=>"goods_operation_user_admin_id",
        "goods_rank_group"=>"goods_rank_group",
        "goods_rank"=>"goods_rank",
        "goods_rank_increase"=>"goods_rank_increase",
        "goods_min_rank"=>"goods_min_rank",
        "goods_min_rank_group"=>"goods_min_rank_group",
        "goods_min_rank_increase"=>"goods_min_rank_increase",
        "goods_loaddata_time"=>"goods_loaddata_time",
        "goods_fulfillable_quantity"=>"goods_fulfillable_quantity",
        "goods_available_days"=>"goods_available_days",
        "goods_reserved_quantity"=>"goods_reserved_quantity",
        "goods_replenishment_quantity"=>"goods_replenishment_quantity",
        "goods_available_stock"=>"goods_available_stock",
        "goods_sync_time"=>"goods_sync_time",
        //"amazon_goods_id"=>"amazon_goods_id",
        //"site_id"=>"goods_site_id",
        //"user_id"=>"user_id",
        //"channel_id"=>"goods_channel_id",
        //"user_id_mod"=>"user_id_mod",
        "area_id"=>"area_area_id",
        "isku_title"=>"isku_isku_title",
        "isku_image"=>"isku_image",
        //"goods_operation_user_admin_name" => "goods_operation_user_admin_name",
        "parent_asin_group" => array(
            'day' => "goods_parent_asin,amazon_goods.goods_channel_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_parent_asin,amazon_goods.goods_channel_id,report.mweekyear,mweek",
            'month' => "goods_parent_asin,amazon_goods.goods_channel_id,report.myear,report.mmonth"
        ),
        "asin_group" => array(
            'day' => "goods_asin,amazon_goods.goods_channel_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_asin,amazon_goods.goods_channel_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_asin,amazon_goods.goods_channel_id,report.myear,report.mmonth"
        ),
        "sku_group" => array(
            'day' => "goods_sku,amazon_goods.goods_channel_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_sku,amazon_goods.goods_channel_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_sku,amazon_goods.goods_channel_id,report.myear,report.mmonth"
        ),
        "isku_group" => array(
            'day' => "goods_isku_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_isku_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_isku_id,report.myear,report.mmonth"
        ),
        "site_id_group" => array(
            'day' => "goods_site_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_site_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_site_id,report.myear,report.mmonth"
        ),
        "channel_id_group" => array(
            'day' => "goods_channel_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_channel_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_channel_id,report.myear,report.mmonth"
        ),
        "product_category_id_1_group" => array(
            'day' => "goods_product_category_id_1,report.myear,report.mmonth,report.mday",
            'week' => "goods_product_category_id_1,report.myear,report.mweekyear,mweek",
            'month' => "goods_product_category_id_1,report.myear,report.mmonth"
        ),
        "group_id_group" => array(
            'day' => "goods_group_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_group_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_group_id,report.myear,report.mmonth"
        ),
        "operation_user_admin_id_group" => array(
            'day' => "goods_operation_user_admin_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_operation_user_admin_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_operation_user_admin_id,report.myear,report.mmonth"
        ),
        "isku_head_id_group" => array(
            'day' => "isku_head_id,report.myear,report.mmonth,report.mday",
            'week' => "isku_head_id,report.myear,report.mweekyear,mweek",
            'month' => "isku_head_id,report.myear,report.mmonth"
        ),
        "isku_developer_id_group" => array(
            'day' => "isku_developer_id,report.myear,report.mmonth,report.mday",
            'week' => "isku_developer_id,report.myear,report.mweekyear,mweek",
            'month' => "isku_developer_id,report.myear,report.mmonth"
        ),
        "isku_head_id" => "isku_head_id",
        "isku_developer_id" => "isku_developer_id",
        "isku"=>"isku_isku"
    );

    protected $goodsGroupByCols = array(
        "parent_asin_group" => array(
            'day' => "goods_parent_asin,report.myear,report.mmonth,report.mday",
            'week' => "goods_parent_asin,report.mweekyear,mweek",
            'month' => "goods_parent_asin,report.myear,report.mmonth"
        ),
        "asin_group" => array(
            'day' => "goods_asin,report.myear,report.mmonth,report.mday",
            'week' => "goods_asin,report.myear,report.mweekyear,mweek",
            'month' => "goods_asin,report.myear,report.mmonth"
        ),
        "sku_group" => array(
            'day' => "goods_sku,report.myear,report.mmonth,report.mday",
            'week' => "goods_sku,report.myear,report.mweekyear,mweek",
            'month' => "goods_sku,report.myear,report.mmonth"
        ),
        "isku_group" => array(
            'day' => "isku_isku,report.myear,report.mmonth,report.mday",
            'week' => "isku_isku,report.myear,report.mweekyear,mweek",
            'month' => "isku_isku,report.myear,report.mmonth"
        ),
        "site_id_group" => array(
            'day' => "goods_site_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_site_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_site_id,report.myear,report.mmonth"
        ),
        "channel_id_group" => array(
            'day' => "goods_channel_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_channel_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_channel_id,report.myear,report.mmonth"
        ),
        "product_category_id_1_group" => array(
            'day' => "goods_product_category_id_1,report.myear,report.mmonth,report.mday",
            'week' => "goods_product_category_id_1,report.myear,report.mweekyear,mweek",
            'month' => "goods_product_category_id_1,report.myear,report.mmonth"
        ),
        "group_id_group" => array(
            'day' => "goods_group_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_group_id,report.myear,report.mmonth,report.mweekyear,mweek",
            'month' => "goods_group_id,report.myear,report.mmonth"
        ),
        "operation_user_admin_id_group" => array(
            'day' => "goods_operation_user_admin_id,report.myear,report.mmonth,report.mday",
            'week' => "goods_operation_user_admin_id,report.myear,report.mweekyear,mweek",
            'month' => "goods_operation_user_admin_id,report.myear,report.mmonth"
        ),
        "isku_head_id_group" => array(
            'day' => "isku_head_id,report.myear,report.mmonth,report.mday",
            'week' => "isku_head_id,report.myear,report.mmonth,report.mweekyear,mweek",
            'month' => "isku_head_id,report.myear,report.mmonth"
        ),
        "isku_developer_id_group" => array(
            'day' => "isku_developer_id,report.myear,report.mmonth,report.mday",
            'week' => "isku_developer_id,report.myear,report.mmonth,report.mweekyear,mweek",
            'month' => "isku_developer_id,report.myear,report.mmonth"
        )
    );

    protected $dbhost = '001';

    protected $codeno = '001';

    protected $presto = null;

    protected $logger = null;

    protected $lastSql = '';

    protected $table = '';

    protected $cache = null;

    protected $dryRun = false;

    protected $logSql = false;

    protected $isReadAthena = false;

    protected $isUseTmpTable = false;

    protected $isReadTmpTable = false;

    protected $tmpTable = '';

    protected $exportTmp = 'tmp.';//切库记得创建

    protected $compatible_dws = "dws_finance_slave";

    protected $user_id = 0;

    public function __construct(
        string $dbhost = '',
        string $codeno = '',
        bool $isReadAthena = false,
        bool $isUseTmpTable = false,
        bool $isReadTmpTable = false,
        string $tmpTable = '',
        ?LoggerInterface $logger = null,
        ?ClientInterface $httpClient = null
    ) {
        $ods = config('misc.presto_schema_ods', 'ods');
        $dws = config('misc.presto_schema_dws', 'dws');
        $dim = config('misc.presto_schema_dim', 'dim');
        $dwd = config('misc.presto_schema_dwd', 'dwdslave');
        $schemas = "{$ods}{$dws}{$dim}{$dwd}";

        $this->isReadAthena = $isReadAthena;

        if ($schemas !== static::$detectSchemaName) {
            static::$detectSchemaName = $schemas;

            foreach (static::$tableMaps as &$v) {
                $schema = substr($v, 0, 4);
                $v = ([
                            'ods.' => $ods,
                            'dws.' => $dws,
                            'dim.' => $dim,
                            'dwd.' => $dwd,
                        ][$schema] ?? substr($schema, 0, 3)) . substr($v, 3);
                $v = str_replace('dim.dim_dataark_f_dw_goods_dim_report_', $dim.".dim_dataark_f_dw_goods_dim_report_", $v);
            }
        }

        $container = ApplicationContext::getContainer();
        if (null === $logger) {
            $logger = $container->get(LoggerFactory::class)->get('presto', 'default');
        }
        $this->logger = $logger;

        if ('' === $dbhost) {
            $userInfo = \app\getUserInfo();
            $dbhost = $userInfo['dbhost'] ?? '';
            $codeno = $userInfo['codeno'] ?? '';
        }
        $this->user_id = \app\getUserInfo()['user_id']??0;

        if (!is_numeric($dbhost) || !is_numeric($codeno)) {
            $this->logger->error('错误的 presto dbhost 或 codeno', [$dbhost, $codeno]);
            throw new RuntimeException('Invalid dbhost or codeno.');
        }
        $this->dbhost = trim($dbhost);
        $this->codeno = trim($codeno);

        $config = $container->get(ConfigInterface::class)->get('presto', []);
        if (empty($config)) {
            $this->logger->error('presto 配置信息不存在');
            throw new RuntimeException('Missing Presto config.');
        }

        //$config = $config[$this->dbhost] ?? [];
        $rand_key = array_rand($config);
        $config = $config[$rand_key] ?? [];
        if (empty($config)) {
            $this->logger->error('presto 数据库配置不存在', [$config]);
            throw new RuntimeException('Missing Presto connection config.');
        }

        //读取数据库presto对应得ip
        $rand_presto_ip = $this->randPrestoIp();
        $config['server'] = $rand_presto_ip;

        $this->logSql = $config['logSql'] ?? false;

        if ($this->table) {
            $tableName = $this->__get($this->table);
            if ($tableName) {
                $this->table = $tableName;
            } else {
                if (strlen($this->table) - 1 === strrpos($this->table, '_')) {
                    $this->table = $this->table . $this->dbhost;
                }
            }
        }

        if (null === $this->isDefaultCache) {
            $this->setDefaultCache(config('misc.presto_defautl_cache', false));
        }
        if ($isReadAthena){
            $config = $container->get(ConfigInterface::class)->get('athena', []);
            if (empty($config)) {
                $this->logger->error('Anthea 配置信息不存在');
                throw new RuntimeException('Missing Anthea config.');
            }
            $this->presto = Athena::getConnection($config, $this->logger, $httpClient);

        }else{
            $this->presto = Presto::getConnection($config, $this->logger, $httpClient);

        }

        $this->isUseTmpTable    = $isUseTmpTable;
        $this->isReadTmpTable   = $isReadTmpTable;
        $this->tmpTable         = $tmpTable;
        $this->exportTmp        = config('misc.presto_export_tmp', 'tmp').".";

    }

    protected function toMysqlTable($sql)
    {
        $ods = config('misc.presto_schema_ods', 'ods');
        $dws = config('misc.presto_schema_dws', 'dws');
        $dim = config('misc.presto_schema_dim', 'dim');
        $dwd = config('misc.presto_schema_dwd', 'dwdslave');

        $schema = array(
            $ods => 'ads',
            $dws => 'ads',
            $dim => 'ads',
            $dwd => 'ads',
            $this->compatible_dws => 'ads'
        );
        foreach ($schema as $key => $v) {
            if ($key == 'ods'){
                $sql = str_replace($key . '.ods', 'ods', $sql);
            }else{
                $sql = str_replace($key . '.', '', $sql);
            }
        }
        $sql = str_replace( '\"', 'toMysqlTable', $sql);
        $sql = str_replace( '"', '', $sql);
        $sql = str_replace( 'toMysqlTable', '\"', $sql);
        $sql = str_replace( 'as varchar)', 'as char)', $sql);
        $sql = str_replace( 'as varchar )', 'as char)', $sql);
        $sql = str_replace( 'as VARCHAR )', 'as char)', $sql);
        $sql = $this->ToMysqlTableChange($sql);
        $this->logger->error("read_mysql:$sql");
        return $sql;

    }

    protected function getCache()
    {
        if (null === $this->cache) {
            $this->cache = ApplicationContext::getContainer()->get(CacheInterface::class);
        }

        return $this->cache;
    }

    public function query(string $sql, array $bindings = [], ?bool $isCache = null, int $cacheTTL = 300, $isMysql = false,$isUseTmpTable = false,$isReadTmpTable = false): array
    {
        if ($bindings) {
            $this->lastSql = $psql = "PREPARE {$sql}; EXECUTE " . @join(',', $bindings);
        } else {
            $this->lastSql = $psql = $sql;
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($psql);
        if ($this->isCache($isCache)) {
            $result = $this->getCache()->get($cacheKey);
            if (!empty($result)) {
                return $result;
            }
        }

        if ($isMysql) {
            $sql = $this->toMysqlTable($sql);
            if (strpos($sql,'dw_goods_dim_report' ) or strpos($sql,'goods_day_report' )){
                $result = Db::connection("bigdata_goods_ads_{$this->dbhost}")->select($sql);

            }else{
                $result = Db::connection('bigdata_ads')->select($sql);

            }
            if (!empty($result)){
                foreach ($result as $key => $value){
                    $result[$key] = (array) $value;
                }
            }else{
                $result = array();
            }
        } else {
            if ($isUseTmpTable){
                $isCache = false;//使用临时表不设置缓存
//                $sql = "create Table {$this->exportTmp}{$this->tmpTable} as ({$sql})";
                $sql = "create Table {$this->exportTmp}{$this->tmpTable} as (select Row_Number() over () as auto_increment_id , * from ({$sql}) as t)";
            }else{
                if ($isReadTmpTable){//取临时表需要判断limit
                    $sql = "SELECT * from {$this->exportTmp}{$this->tmpTable} order  by auto_increment_id asc ";
                    if (1 === preg_match('/\s*offset\s+(\d+)\s+/i', strtolower($sql), $offset_arr) && isset($offset_arr[1])) {
                        $sql .= " OFFSET {$offset_arr[1]}";
                    }
                    if (1 === preg_match('/\s*limit\s+(\d+)/i', strtolower($sql), $limit_arr) && isset($limit_arr[1])) {
                        $sql .= " LIMIT {$limit_arr[1]}";
                    }
                }

            }
            $result = $this->presto->query($sql,...$bindings);
        }
        if (false === $result) {
            $this->logger->error("sql: {$psql} error:执行sql异常");
            return [];
        }
        if ($isUseTmpTable){//使用临时表直接返回空数组
            return [];
        }

        if ($this->isCache($isCache)) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }
        return $result;
    }

    public function fetch(string $sql, array $bindings = [], ?bool $isCache = null, int $cacheTTL = 300): array
    {
        $result = $this->query($sql, $bindings, $isCache, $cacheTTL);
        return $result[0] ?? [];
    }

    public function getLastSql(): string
    {
        return $this->lastSql;
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * 执行sql查询
     * @param $where 		查询条件[例`name`='$name']
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $table 		查询表[默认当前模型表]
     * @param $limit 		返回结果范围[例：10或10,10 默认为空]
     * @param $order 		排序方式	[默认按数据库默认方式排序]
     * @param $group 		分组方式	[默认为空]
     * @param $is_cache     是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @param $compare_data    对比数据信息
     * @return array		查询结果集数组
     */
    public function select(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        bool $isJoin = false ,
        ?bool $isCache = null,
        int $cacheTTL = 300,
        bool $isMysql = false,
        array $compare_data = [],
        array $fba_data = [],
        array $erp_data = [],
        bool $isUseTmpTable = false,
        bool $isReadTmpTable = false
    ): array {
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;

        $where = empty($where) ? '' : " WHERE {$where}";
        $order = empty($order) ? '' : " ORDER BY {$order}";
        $group = empty($group) ? '' : " GROUP BY {$group}";
        $limit = $mysql_limit = is_string($limit) || is_numeric($limit) ? trim((string)$limit) : '';
        $athena_limit = '';
        $is_only_limit = false;
        if (!empty($limit)) {
            // 兼容 $limit = '1', '1, 2', 'limit 1,2', 'limit 1 offset 2', 'offset 1 limit 2' 等形式
            if (false !== strpos($limit, ',')) {
                list($offset, $limit) = explode(',', $limit, 2);
                if (1 === preg_match('/\s*limit\s+(\d+)/i', $offset, $m)) {
                    $offset = $m[1];
                }

                // presto 语法必须 offset 在前，且不支持 limit 1,2 这种写法
                $limit = " OFFSET {$offset} LIMIT {$limit}";

                $mysql_limit = " LIMIT {$offset} , {$limit} ";
                //athena 分页写法
                $athena_offset = ($offset+1);
                $athena_limit = " WHERE rn BETWEEN {$athena_offset} AND ".($athena_offset+$limit);
            } else {
                if (is_numeric($limit)) {
                    $limit = $mysql_limit = " LIMIT {$limit}";
                    $athena_limit = " LIMIT {$limit}";
                    $is_only_limit = true;
                } elseif (1 === preg_match('/\s*offset\s+(\d+)\s+limit\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[1]} LIMIT {$m[2]}";
                    $mysql_limit = " LIMIT {$m[1]} , {$m[2]} ";
                    //athena 分页写法
                    $athena_offset = ($m[1]+1);
                    $athena_limit = " WHERE rn BETWEEN ".$athena_offset." AND ".($athena_offset+$m[2]);
                } elseif (1 === preg_match('/\s*limit\s+(\d+)\s+offset\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[2]} LIMIT {$m[1]}";
                    $mysql_limit = " LIMIT {$m[2]} , {$m[1]} ";
                    //athena 分页写法
                    $athena_offset = ($m[2]+1);
                    $athena_limit = " WHERE rn BETWEEN ".$athena_offset." AND ".($athena_offset+$m[1]);
                }
            }
        }
        if(!empty($compare_data)){
            $newTables = array() ;
            $newTables[] = "origin_table AS (  SELECT {$data} FROM {$table} {$where} {$group} {$order} ) " ;
            $rt_field = 'origin_table.* ' ;
            foreach($compare_data as $c1=>$cdata1){
                if(!empty($cdata1['custom_target']) && is_array($cdata1['custom_target'])){ //自定义指标
                    foreach($cdata1['custom_target'] as $custom_target){
                        $rt_field.= " , {$custom_target} " ;
                    }
                }
                if($cdata1['target'] == 'industry'){
                    $compare_data_target_type = !empty($cdata1['target_type']) ? $cdata1['target_type'] : 1;
                    $rt_field.=",industry_table_{$compare_data_target_type}.category_result_data";
                }
            }
            $rt_sql = "SELECT {$rt_field} FROM origin_table " ;
            $rt_where = '' ;
            $rt_order = '' ;
            //不需要having 之后的SQL
            $new_group =  preg_replace("/ having.*/i","",$group);
            foreach($compare_data as $c=>$cdata){
                $k = $c+1 ;
                if(!empty($cdata['industry_table_1'])){
                    $newTables[] = " industry_table_1 AS ( {$cdata['industry_table_1']} ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " industry_table_1 ON {$cdata['on']} " ;
                }elseif(!empty($cdata['industry_table_2'])){
                    $newTables[] = " industry_table_2 AS ( {$cdata['industry_table_2']} ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " industry_table_2 ON {$cdata['on']} " ;
                }elseif (!empty($cdata['get_value_type'])){
                    $temp_target = explode(',',$cdata['target']);
                    $temp_field_arr = [];
                    $temp_field_char = "MAX(compare{$k}_user_id) AS user_id";
                    if($cdata['get_value_type'] == 'avg_value'){
                        //均值
                        foreach ($temp_target as $item){
                            $temp_field_arr[] = "AVG(compare{$k}_{$item}) AS {$item}_avg ";
                        }
                        $temp_char = implode(',',$temp_field_arr);
                        $temp_field_char = !empty($temp_char) ? $temp_field_char . ',' . $temp_char : $temp_field_char;
                        $newTables[] = " avg_table_{$cdata['time_type']} AS ( SELECT {$temp_field_char} FROM (SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group}) ) "  ;
                        $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " avg_table_{$cdata['time_type']} ON {$cdata['on']} " ;
                    }elseif($cdata['get_value_type'] == 'median_value'){

                        //中位数
                        foreach ($temp_target as $item){
                            $temp_field_arr[] = "approx_percentile(compare{$k}_{$item}, 0.5) AS {$item}_median ";
                        }
                        $temp_char = implode(',',$temp_field_arr);
                        $temp_field_char = !empty($temp_char) ? $temp_field_char . ',' . $temp_char : $temp_field_char;
                        $newTables[] = " median_table_{$cdata['time_type']} AS ( SELECT {$temp_field_char} FROM (SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group}) ) "  ;
                        $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " median_table_{$cdata['time_type']} ON {$cdata['on']} " ;
                    }
                }elseif(!empty($cdata['new_table'])){
                    $newTables[] = " compare_table{$k} AS ( SELECT {$cdata['field_data']}   FROM  {$cdata['new_table']} WHERE {$cdata['compare_where']} {$new_group} ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " compare_table{$k} ON {$cdata['on']} " ;
                }else{
                    $newTables[] = " compare_table{$k} AS ( SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group} ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " compare_table{$k} ON {$cdata['on']} " ;
                }

                if(!empty($cdata['where'])){
                    $rt_where .= empty($rt_where) ? $cdata['where'] : (' AND ' . $cdata['where'] ) ;
                }
                if(!empty($cdata['order'])){
                    $rt_order .= empty($rt_order) ? $cdata['order'] : " , " . $cdata['order'] ;
                }
            }
            if(!empty($rt_where)){
                $rt_sql .= " WHERE " . $rt_where ;
            }
            $sql = 'WITH 
            ' . implode(',
            ' , $newTables) . "
            " .$rt_sql ;
            if(!empty($rt_order)){
                $sql.= " ORDER BY " .$rt_order ;
            }
        }
        elseif (!empty($erp_data))
        {
            $erp_origin_fields = $erp_data['origin_fields'];
            $erp_query_fields = $erp_data['query_fields'];
            $erp_query_group = $erp_data['query_group'];
            $erp_query_order = $erp_data['query_order'];
            $erp_report_table = $erp_data['report_table'];
            $erp_origin_sql = "SELECT {$erp_origin_fields} FROM {$table} {$where} {$group}, report.myear, report.mmonth";
            $sql = "SELECT {$erp_query_fields} FROM ({$erp_origin_sql}) AS report_inner {$erp_report_table} GROUP BY {$erp_query_group} ORDER BY {$erp_query_order}";
        }else{
            $sql =  "SELECT {$data} FROM {$table} {$where} {$group} {$order} ";
        }

        //有查询FBA指标
        if(!empty($fba_data)){
            $newTables = array() ;
            $newTables[] = "new_origin_table AS ( {$sql} ) " ;
            if(empty($fba_data['fba_fields'])){
                $rt_field = 'new_origin_table.*,fba_table.*' ;
            }else{
                $rt_field = 'new_origin_table.*,'. $fba_data['fba_fields'];
            }
            if(!empty($fba_data['other_field'])){
                $rt_field.=" , " . $fba_data['other_field'] ;
            }

            $rt_sql = "SELECT {$rt_field} FROM new_origin_table" ;
            $rt_join = !empty($fba_data['join']) ? $fba_data['join'] : "" ;
            $rt_where = !empty($fba_data['where']) ? $fba_data['where'] : "" ;
            $rt_order = !empty($fba_data['order']) ? $fba_data['order'] : "" ;
            $count_table_group = empty($fba_data['group']) ? '' : " GROUP BY {$fba_data['group']}";
            if(!empty($fba_data['child_table'])){
                foreach($fba_data['child_table'] as $c=>$cdata){
                    if(!empty($fba_data['is_count']) && $c == '0'){
                        if($fba_data['dimension'] == 'channel'){
                            $newTables[] = "{$cdata['table_name']}  AS (select fabTmp.* from (SELECT report.channel_id  FROM {$table} {$where} group by report.channel_id) AS FBAOriginTabel LEFT JOIN ({$cdata['table_sql']} ) AS fabTmp ON fabTmp.channel_id = FBAOriginTabel.channel_id AND fabTmp.channel_id is NOT NULL )  " ;
                        }elseif($fba_data['dimension'] == 'sku'){
                            $newTables[] = " {$cdata['table_name']} AS ( {$cdata['table_sql']} ) "  ;
                            $newTables[] = "count_table AS (SELECT max(report.user_id) AS user_id,max(amazon_goods.goods_sku) AS sku,max(report.channel_id) AS channel_id,max(report.amazon_goods_id) AS goods_id FROM {$table} {$where} {$count_table_group})";
                        }else{
                            $newTables[] = " {$cdata['table_name']} AS ( {$cdata['table_sql']} ) "  ;
                        }
                    }else{
                        $newTables[] = " {$cdata['table_name']} AS ( {$cdata['table_sql']} ) "  ;
                    }
                }
            }
            if(!empty($rt_join)){
                $rt_sql .= " LEFT JOIN fba_table ON " . $rt_join ;
            }
            if(!empty($rt_where)){
                $rt_sql .= " WHERE " . $rt_where ;
            }
            $sql = 'WITH 
            ' . implode(',
            ' , $newTables) . "
            " .$rt_sql ;
            if(!empty($rt_order)){
                $sql.= " ORDER BY " .$rt_order ;
            }
        }
        //商品级
        //print_r($this->goodsCols);
        if($isJoin){
            foreach ($this->goodsCols as $key => $value){
                if (!is_array($value)) {
                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report."' . $key.'"', 'amazon_goods.' . $value, $sql);

                } else {
                    if (strpos($table, '_day_report_') && !strpos($table,'week_report' ) ) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['day'], $sql);
                    } elseif (strpos($table,'week_report' )) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['week'], $sql);
                    } elseif (strpos($table,'_month_report_')) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['month'], $sql);
                    }
                }
            }
            if (strpos($table,'week_report' )){
                $sql = str_replace('week_report' , 'report' , $sql);

            }

        }
        $athena_sql = $sql;

        if ($isMysql){
            $sql .= " {$mysql_limit}";

        }else{
            $sql .= " {$limit}";

        }


        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($sql);
        if ($this->isCache($isCache)) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }
        if ($this->isReadAthena){
            if (!$is_only_limit && !empty($athena_limit)) {
                $sql = "SELECT * FROM ( SELECT row_number() over() AS rn, * FROM ($athena_sql) as t)  {$athena_limit}";//athena特有的分页写法
            }
        }
        if ($isReadTmpTable){
            $sql = "SELECT * from {$this->exportTmp}{$this->tmpTable} order  by auto_increment_id asc {$limit}";
        }
        if ($isMysql) {
            $sql = $this->toMysqlTable($sql);
            $this->lastSql = $sql;
            $this->logSql();
            if ($this->logDryRun()) {
                return [];
            }

            if (strpos($table,'dw_goods_dim_report' ) or strpos($table,'goods_day_report' )){

                $result = Db::connection("bigdata_goods_ads_{$this->dbhost}")->select($sql);

            }else{
                $result = Db::connection('bigdata_ads')->select($sql);

            }
            if (!empty($result)){
                foreach ($result as $key => $value){
                    $result[$key] = (array) $value;
                }
            }else{
                $result = array();
            }
        } else {
            $this->lastSql = $sql;
            $this->logSql();
            if ($this->logDryRun()) {
                return [];
            }
            if ($isUseTmpTable){
                $isCache = false;//使用临时表不设置缓存
                $sql = "create Table {$this->exportTmp}{$this->tmpTable} as (select Row_Number() over () as auto_increment_id , * from ({$sql}) as t)";
            }
            $result = $this->presto->query($sql);

        }

        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('presto 查询失败');
        }

        if ($isUseTmpTable){//临时表直接返回空
            return [];
        }

        if ($this->isCache($isCache)) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }

        return $result;
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * 执行sql查询
     * @param $where 		查询条件[例`name`='$name']
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $table 		查询表[默认当前模型表]
     * @param $limit 		返回结果范围[例：10或10,10 默认为空]
     * @param $order 		排序方式	[默认按数据库默认方式排序]
     * @param $group 		分组方式	[默认为空]
     * @param $is_cache     是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return array		查询结果集数组
     */
    public function getSelectSql(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        bool $isJoin = false ,
        bool $isMysql = false
    ): string {
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;

        $where = empty($where) ? '' : " WHERE {$where}";
        $order = empty($order) ? '' : " ORDER BY {$order}";
        $group = empty($group) ? '' : " GROUP BY {$group}";
        $limit = $mysql_limit = is_string($limit) || is_numeric($limit) ? trim((string)$limit) : '';
        $athena_limit = '';
        $is_only_limit = false;
        if (!empty($limit)) {
            // 兼容 $limit = '1', '1, 2', 'limit 1,2', 'limit 1 offset 2', 'offset 1 limit 2' 等形式
            if (false !== strpos($limit, ',')) {
                list($offset, $limit) = explode(',', $limit, 2);
                if (1 === preg_match('/\s*limit\s+(\d+)/i', $offset, $m)) {
                    $offset = $m[1];
                }


                // presto 语法必须 offset 在前，且不支持 limit 1,2 这种写法
                $limit = " OFFSET {$offset} LIMIT {$limit}";

                $mysql_limit = " LIMIT {$offset} , {$limit} ";
                //athena 分页写法
                $athena_offset = ($offset+1);
                $athena_limit = " WHERE rn BETWEEN {$athena_offset} AND ".($athena_offset+$limit);
            } else {
                if (is_numeric($limit)) {
                    $limit = $mysql_limit = " LIMIT {$limit}";
                    $athena_limit = " LIMIT {$limit}";
                    $is_only_limit = true;
                } elseif (1 === preg_match('/\s*offset\s+(\d+)\s+limit\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[1]} LIMIT {$m[2]}";
                    $mysql_limit = " LIMIT {$m[1]} , {$m[2]} ";
                    //athena 分页写法
                    $athena_offset = ($m[1]+1);
                    $athena_limit = " WHERE rn BETWEEN ".$athena_offset." AND ".($athena_offset+$m[2]);
                } elseif (1 === preg_match('/\s*limit\s+(\d+)\s+offset\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[2]} LIMIT {$m[1]}";
                    $mysql_limit = " LIMIT {$m[2]} , {$m[1]} ";
                    //athena 分页写法
                    $athena_offset = ($m[2]+1);
                    $athena_limit = " WHERE rn BETWEEN ".$athena_offset." AND ".($athena_offset+$m[1]);
                }
            }
        }

        $sql =  "SELECT {$data} FROM {$table} {$where} {$group} {$order} ";

        //商品级
        //print_r($this->goodsCols);
        if($isJoin){
            foreach ($this->goodsCols as $key => $value){
                if (!is_array($value)) {
                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report."' . $key.'"', 'amazon_goods.' . $value, $sql);

                } else {
                    if (strpos($table, '_day_report_') && !strpos($table,'week_report' ) ) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['day'], $sql);
                    } elseif (strpos($table,'week_report' )) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['week'], $sql);
                    } elseif (strpos($table,'_month_report_')) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['month'], $sql);
                    }
                }
            }
            if (strpos($table,'week_report' )){
                $sql = str_replace('week_report' , 'report' , $sql);

            }

        }
        $athena_sql = $sql;

        if ($isMysql){
            $sql .= " {$mysql_limit}";

        }else{
            $sql .= " {$limit}";

        }

        if ($this->isReadAthena){
            if (!$is_only_limit && !empty($athena_limit)) {
                $sql = "SELECT * FROM ( SELECT row_number() over() AS rn, * FROM ($athena_sql) as t)  {$athena_limit}";//athena特有的分页写法
            }
        }
        if ($isMysql) {
            $sql = $this->toMysqlTable($sql);
        }

        return $sql;
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * @param $where 		查询条件[例`name`='$name']
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $table 		查询表[默认当前模型表]
     * @param $order 		排序方式	[默认按数据库默认方式排序]
     * @param $group 		分组方式	[默认为空]
     * @param $is_cache     是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return array		查询结果集数组
     */
    public function getOne(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isJoin = false ,
        ?bool $isCache = null,
        int $cacheTTL = 300,
        bool $isMysql = false
    ): array {
        $result = $this->select($where, $data, $table, 1, $order, $group ,$isJoin, $isCache, $cacheTTL, $isMysql,[],[],[]);

        return $result[0] ?? [];
    }

    /** @see $this->getOne */
    public function get_one(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isJoin = false ,
        ?bool $isCache = null,
        int $cacheTTL = 300,
        bool $isMysql = false
    ): array {
        return $this->getOne($where, $data, $table, $order, $group, $isJoin ,$isCache, $cacheTTL,$isMysql);
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * 计算记录数
     * @param $where 		查询条件[例`name`='$name']
     * @param $table 		查询表[默认当前模型表]
     * @param $group 		分组方式	[默认为空]
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $cols 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $isCache      是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return int		    记录数
     */
    public function count(
        $where = '',
        string $table = '',
        string $group = '',
        string $data = '',
        string $cols = '',
        bool $isJoin = false ,
        ?bool $isCache = null,
        int $cacheTTL = 300,
        bool $isMysql = false ,
        array $compare_data = [],
        array $fba_data = []

    ): int {
        $where = is_array($where) ? $this->sqls($where) : $where;

        if(empty($compare_data) && empty($fba_data)){
            if ($group) {
                $data = $data ?: '1';
                if (stripos($group, 'having') === false && stripos($group, ',') === false && !$isMysql) {
                    $result = $this->getOne(
                        $where,
                        "count(distinct($group)) AS num",
                        "$table",
                        '',
                        '',
                        $isJoin ,
                        $isCache,
                        $cacheTTL,
                        $isMysql

                    );
                } else {
                    $result = $this->getOne(
                        '',
                        "COUNT(*) AS num",
                        "(SELECT {$data} FROM {$table} WHERE {$where} GROUP BY {$group}) AS tmp",
                        '',
                        '',
                        $isJoin,
                        $isCache,
                        $cacheTTL,
                        $isMysql
                    );
                }
            } elseif (!empty($cols)) {
                $result = $this->getOne($where, "COUNT({$cols}) AS num", $table, '', '', $isJoin , $isCache, $cacheTTL,$isMysql);
            } else {
                $result = $this->getOne($where, "COUNT(*) AS num", $table, '', '' , $isJoin, $isCache, $cacheTTL,$isMysql);
            }
        }else{  //有环比数据获取总条数时
            $result = $this->getCompareDataCount($where, $data, $table,  $group , $isJoin, $isCache, $cacheTTL,$isMysql,$compare_data,$fba_data);
        }
        return intval($result['num'] ?? 0);
    }

    /**
     * function getCompareDataCount
     * desc: 获取有比较数据的总条数
     * author: LWZ
     * editTime: 2021-09-26 15:21
     */
    public function getCompareDataCount($where, $data, $table, $group , $isJoin, $isCache, $cacheTTL,$isMysql,$compare_data,$fba_data){
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;
        $where = empty($where) ? '' : " WHERE {$where}";
        $group = empty($group) ? '' : " GROUP BY {$group}";

        $newTables = array() ;
        $newTables[] = "origin_table AS (  SELECT {$data} FROM {$table} {$where} {$group} ) " ;
        if(empty($fba_data)){
            $rt_sql = 'SELECT count(*) as num FROM origin_table ' ;
        }else{
            $rt_sql = 'SELECT origin_table.* FROM origin_table ' ;
        }
        $rt_where = '' ;
        //不需要having 之后的SQL
        $new_group =  preg_replace("/ having.*/i","",$group);
        foreach($compare_data as $c=>$cdata){
            $k = $c+1 ;
            if(!empty($cdata['industry_table_1'])){
                //销量
                $newTables[] = " industry_table_1 AS ( {$cdata['industry_table_1']} ) "  ;
                $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " industry_table_1 ON {$cdata['on']} " ;
            }elseif(!empty($cdata['industry_table_2'])){
                //销售额
                $newTables[] = " industry_table_2 AS ( {$cdata['industry_table_2']} ) "  ;
                $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " industry_table_2 ON {$cdata['on']} " ;
            }elseif (!empty($cdata['get_value_type'])){
                $temp_target = explode(',',$cdata['target']);
                $temp_field_arr = [];
                $temp_field_char = "MAX(compare{$k}_user_id) AS user_id";
                if($cdata['get_value_type'] == 'avg_value'){
                    //均值
                    foreach ($temp_target as $item){
                        $temp_field_arr[] = "AVG(compare{$k}_{$item}) AS {$item}_avg ";
                    }
                    $temp_char = implode(',',$temp_field_arr);
                    $temp_field_char = !empty($temp_char) ? $temp_field_char . ',' . $temp_char : $temp_field_char;
                    $newTables[] = " avg_table_{$cdata['time_type']} AS ( SELECT {$temp_field_char} FROM (SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group}) ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " avg_table_{$cdata['time_type']} ON {$cdata['on']} " ;
                }elseif($cdata['get_value_type'] == 'median_value'){

                    //中位数
                    foreach ($temp_target as $item){
                        $temp_field_arr[] = "approx_percentile(compare{$k}_{$item}, 0.5) AS {$item}_median ";
                    }
                    $temp_char = implode(',',$temp_field_arr);
                    $temp_field_char = !empty($temp_char) ? $temp_field_char . ',' . $temp_char : $temp_field_char;
                    $newTables[] = " median_table_{$cdata['time_type']} AS ( SELECT {$temp_field_char} FROM (SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group}) ) "  ;
                    $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " median_table_{$cdata['time_type']} ON {$cdata['on']} " ;
                }
            }elseif(!empty($cdata['new_table'])){
                $newTables[] = " compare_table{$k} AS ( SELECT {$cdata['field_data']}   FROM  {$cdata['new_table']} WHERE {$cdata['compare_where']} {$new_group} ) "  ;
                $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " compare_table{$k} ON {$cdata['on']} " ;
            }else{
                $newTables[] = " compare_table{$k} AS ( SELECT {$cdata['field_data']}   FROM  {$table} WHERE {$cdata['compare_where']} {$new_group} ) "  ;
                $rt_sql.=  ( empty($cdata['join_type']) ? 'LEFT JOIN ' : $cdata['join_type']  ) . " compare_table{$k} ON {$cdata['on']} " ;
            }
            if(!empty($cdata['where'])){
                $rt_where .= empty($rt_where) ? $cdata['where'] : (' AND ' . $cdata['where'] ) ;
            }
        }
        if(!empty($rt_where)){
            $rt_sql .= " WHERE " . $rt_where ;
        }

        $sql = 'WITH 
        ' . implode(',
        ' , $newTables) . "
        " .$rt_sql ;

        //有查询FBA指标
        if(!empty($fba_data)){
            $newTables = array() ;
            $newTables[] = "new_origin_table AS ( {$sql} ) " ;
            $rt_sql = "SELECT count(*) as num FROM new_origin_table" ;
            $rt_join = !empty($fba_data['join']) ? $fba_data['join'] : "" ;
            $rt_where = !empty($fba_data['where']) ? $fba_data['where'] : "" ;
            if(!empty($fba_data['child_table'])){
                foreach($fba_data['child_table'] as $c=>$cdata){
                    $newTables[] = " {$cdata['table_name']} AS ( {$cdata['table_sql']} ) "  ;
                }
            }
            if(!empty($rt_join)){
                $rt_sql .= " LEFT JOIN fba_table ON " . $rt_join ;
            }
            if(!empty($rt_where)){
                $rt_sql .= " WHERE " . $rt_where ;
            }
            $sql = 'WITH 
            ' . implode(',
            ' , $newTables) . "
            " .$rt_sql ;
        }
        if($isJoin){
            foreach ($this->goodsCols as $key => $value){
                if (!is_array($value)) {
                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report."' . $key.'"', 'amazon_goods.' . $value, $sql);

                } else {
                    if (strpos($table, '_day_report_') && !strpos($table,'week_report' ) ) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['day'], $sql);
                    } elseif (strpos($table,'week_report' )) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['week'], $sql);
                    } elseif (strpos($table,'_month_report_')) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['month'], $sql);
                    }
                }
            }
            if (strpos($table,'week_report' )){
                $sql = str_replace('week_report' , 'report' , $sql);
            }
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($sql);
        if ($this->isCache($isCache)) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }

        $this->lastSql = $sql;
        $this->logSql();
        if ($this->logDryRun()) {
            return 0;
        }
        $result = $this->presto->query($sql);
        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('presto 查询失败');
        }else{
            $result = $result[0] ?? [];
            if ($this->isCache($isCache)) {
                $this->getCache()->set($cacheKey, $result, $cacheTTL);
            }
        }
        return $result ;
    }

    /**
     * 将数组转换为SQL语句
     *
     * @param array $where 要生成的数组
     * @param string $font 连接串。
     */
    protected function sqls($where, string $font = 'AND'): string
    {
        if (is_array($where)) {
            $sql = '';
            foreach ($where as $key => $val) {
                $sql .= sprintf(" %s `%s`=%s", $font, $key, Presto::bindValue($val));
            }

            return $sql ? substr($sql, sizeof($font) + 1) : '';
        } else {
            return is_string($where) ? $where : '';
        }
    }

    public static function escape(string $val): string
    {
        return Presto::escape((string)$val);
    }

    public function dryRun(?bool $dryRun): bool
    {
        if (null !== $dryRun) {
            $this->dryRun = $dryRun;
        }

        return $this->dryRun;
    }

    protected function logDryRun(): bool
    {
        if ($this->dryRun) {
            $this->logger->debug('Presto dry run: ' . $this->getLastSql());
            return true;
        }

        return false;
    }

    protected function logSql()
    {
        if ($this->logSql) {
            $this->logger->info('Presto Sql: ' . $this->getLastSql());
        }
    }

    public function __get(string $name)
    {
        if (array_key_exists($name, self::$tableMaps)) {
            $tableName = self::$tableMaps[$name];
            $fba_arr = array(
                "ods.ods_g_amazon_fba_inventory_v3_tend_001",
                "ods.ods_g_amazon_fba_inventory_v3_001",
                "ods.ods_g_amazon_fba_inventory_v3_rel_001",
                "ods.ods_e_erp_storage_inventory_warehouse_report_001",
                "ods.ods_e_erp_storage_warehouse_isku_001",
            );
            if (in_array($tableName,$fba_arr)){
                $tableName = str_replace("ods.","odsslave.",$tableName);
            }
            $tableName = $this->dwsTransition($tableName);
            if (false !== strpos($tableName, '{DBHOST}')) {
                $tableName = strtr($tableName, ['{DBHOST}' => \app\getUserInfo()['dbhost'] ?? '']);

            }
            //月报未拆表
            if (false !== strpos($tableName, 'bigusers_')) {
                $tableName = strtr($tableName, ['{DWSDBHOST}' => \app\getUserInfo()['dbhost'] ?? '']);
            }

            $big_selling_users = config("common.big_selling_users");
            $user_id_arr = array();
            if (!empty($big_selling_users)){
                $user_id_arr = explode(',',$big_selling_users);
            }
            $user_id = \app\getUserInfo()['user_id']??0;

            if (false !== strpos($tableName, '{DWSDBHOST}')) {

                if (in_array($user_id,$user_id_arr)){
                    if (false !== strpos($tableName,'dw_channel_day')){
                        $tableName = strtr($tableName, ['{DWSDBHOST}' =>  'bigusers']);
                    }else{
//                        $tableName = strtr($tableName, ['{DWSDBHOST}' =>  'bigusers']);
                        $dbhost_tmp = 'bigusers_'.(\app\getUserInfo()['dbhost'] ?? '');
                        $tableName = strtr($tableName, ['{DWSDBHOST}' =>  $dbhost_tmp]);
                    }
                }else{
                    $tableName = strtr($tableName, ['{DWSDBHOST}' => \app\getUserInfo()['dbhost'] ?? '']);
                }
            }else{

                if (in_array($user_id,$user_id_arr)){
                    if (false !== strpos($tableName, 'dw_goods_month_report_slave_bigusers_')) {
                        $tableName = str_replace("_bigusers","",$tableName);
                    }
                }

            }
            $dbhost = \app\getUserInfo()['dbhost'] ?? '';
            $tableName = $this->dwsTransitionEnd($tableName,$user_id,$user_id_arr,$dbhost);
            return $tableName;
        }

        return strpos($name, 'table_') === 0 ? '' : null;
    }

    protected function ToMysqlTableChange($sql){
        $dbhost_arr = array(
            "001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021","022","023"
        );

        $big_selling_users = config("common.big_selling_users");

        //大用户数组
        $user_id_arr = array();
        if (!empty($big_selling_users)){
            $user_id_arr = explode(',',$big_selling_users);
        }
        $user_id = $this->user_id;
        $dbhost = $this->dbhost;

//        //小卖商品月报
//        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($sql, 'dws_dataark_f_dw_goods_month_report_')){
//            $sql = str_replace("dws_dataark_f_dw_goods_month_report_","dws_dataark_f_dw_goods_month_report_ads_",$sql);
//        }
//
//        //小卖店铺月报
//        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($sql, '.dws_dataark_f_dw_channel_month_report_slave_bigusers_')){
//            $sql = str_replace("dws_dataark_f_dw_channel_month_report_slave_bigusers_","dws_dataark_f_dw_channel_month_report_ads_",$sql);
//        }

        //小卖店铺日报
        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($sql, 'dws_dataark_f_dw_channel_day_report_')){
            $sql = str_replace("dws_dataark_f_dw_channel_day_report_","dws_dataark_f_dw_channel_day_report_ads_",$sql);
        }
        $this->logger->info('111111Presto Sql: ' . $sql);
        return $sql;
    }

    private function dwsTransitionEnd($tableName,$user_id,$user_id_arr,$dbhost){
        $dbhost_arr = array(
            "001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021","022","023"
        );

        //小卖商品月报
        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($tableName, 'dws_dataark_f_dw_goods_month_report_slave_bigusers_')){
            $tableName = str_replace("dws_dataark_f_dw_goods_month_report_slave_bigusers_","dws_dataark_f_dw_goods_month_report_",$tableName);
        }

        //小卖店铺月报
        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($tableName, '.dws_dataark_f_dw_channel_month_report_slave_bigusers_')){
            $tableName = str_replace("dws_dataark_f_dw_channel_month_report_slave_bigusers_","dws_dataark_f_dw_channel_month_report_",$tableName);
        }

        return $tableName;
    }

    private function dwsTransition($tableName){
        $dbhost_arr = array(
            "001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021","022","023"
        );
        $dws = config('misc.presto_schema_dws', 'dws');
        $dbhost = \app\getUserInfo()['dbhost'] ?? '';
        $big_selling_users = config("common.big_selling_users");

        //大用户数组
        $user_id_arr = array();
        if (!empty($big_selling_users)){
            $user_id_arr = explode(',',$big_selling_users);
        }
        $user_id = \app\getUserInfo()['user_id']??0;

        //小卖用户全部使用dws_finance_slave.
        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($tableName, $dws.'.')){
            $tableName = str_replace("$dws.","{$this->compatible_dws}.",$tableName);
        }

//        //小卖商品日报
//        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($tableName, $dws.'.dws_dataark_f_dw_goods_day_report_')){
//            $tableName = str_replace("$dws.dws_dataark_f_dw_goods_day_report_","{$this->compatible_dws}.dws_dataark_f_dw_goods_day_report_",$tableName);
//        }
//
//        //小卖店铺日报
//        if (in_array($dbhost,$dbhost_arr) && !in_array($user_id,$user_id_arr) && false !== strpos($tableName, $dws.'.dws_dataark_f_dw_channel_day_report_')){
//            $tableName = str_replace("$dws.dws_dataark_f_dw_channel_day_report_","{$this->compatible_dws}.dws_dataark_f_dw_channel_day_report_",$tableName);
//        }

        return $tableName;
    }

    public function randPrestoIp(){
        $redis =new Redis();
        $redis = $redis->getClient('bi');
        $rand_presto_ip = $redis->get('jdx_rand_presto_ip');
        if ($rand_presto_ip !== false){
            $rand_presto_ip = unserialize($rand_presto_ip);
        }
        if (empty($rand_presto_ip)){
            $rand_presto_ip = RandPrestoServerModel::where("is_available","=",1)->get(['presto_ip','presto_port','is_available'])->toArray();
                if (empty($rand_presto_ip)) {
                $this->logger->error('presto mysql数据库查询无数据', [$rand_presto_ip]);
                throw new RuntimeException('Missing select presto mysql connection config.');
            }
            $redis->set("jdx_rand_presto_ip",serialize($rand_presto_ip));
        }
        $rand_presto_ip_key = array_rand($rand_presto_ip);
        $rand_presto_ip = $rand_presto_ip[$rand_presto_ip_key];
        $server = trim($rand_presto_ip['presto_ip']).":".trim($rand_presto_ip['presto_port']);
        return $server;

    }

}
