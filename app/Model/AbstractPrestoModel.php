<?php

namespace App\Model;

use RuntimeException;

use App\Lib\Presto;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use GuzzleHttp\ClientInterface;

use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Logger\LoggerFactory;

abstract class AbstractPrestoModel implements BIModelInterface
{
    protected static $detectSchemaName = false;
    
    protected static $tableMaps = [
        'table_channel' => 'ods.ods_dataark_b_channel',
        'table_site_rate' => 'ods.ods_dataark_b_site_rate',
        'table_user_department' => 'ods.ods_dataark_b_user_department',
        'table_amazon_goods_isku' => 'ods.ods_dataark_f_amazon_goods_isku_001',
        'table_amazon_goods_finance' => 'ods.ods_dataark_f_amazon_goods_finance_001',
        'table_amazon_goods_tags' => 'ods.ods_dataark_g_amazon_goods_tags_001',
        'table_amazon_goods_tags_rel' => 'ods.ods_dataark_g_amazon_goods_tags_rel_001',
        'table_amazon_fba_inventory_by_channel' => 'ods.ods_dataark_f_amazon_fba_inventory_by_channel_001',
        'table_amazon_goods_finance_report_by_order' => 'ods.ods_dataark_f_amazon_goods_finance_report_by_order_001',

        'table_user_channel' => 'dim.dim_dataark_b_user_channel',
        'table_department_channel' => 'dim.dim_dataark_b_department_channel',

        'table_goods_day_report' => 'dws.dws_dataark_f_dw_goods_day_report_{DBHOST} AS report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_channel_day_report' => 'dws.dws_dataark_f_dw_channel_day_report_{DBHOST}',
        'table_goods_week_report' => 'dws.dws_dataark_f_dw_goods_week_report_{DBHOST} AS report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_goods_month_report' => 'dws.dws_dataark_f_dw_goods_month_report_{DBHOST} AS report JOIN dim.dim_dataark_f_dw_goods_dim_report_{DBHOST} AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id' ,
        'table_channel_week_report' => 'dws.dws_dataark_f_dw_channel_week_report_{DBHOST}' ,
        'table_channel_month_report' => 'dws.dws_dataark_f_dw_channel_month_report_{DBHOST}' ,
        'table_operation_day_report' => 'dws.dws_dataark_f_dw_operation_day_report_{DBHOST}' ,
        'table_operation_week_report' => 'dws.dws_dataark_f_dw_operation_week_report_{DBHOST}',
        'table_operation_month_report' => 'dws.dws_dataark_f_dw_operation_month_report_{DBHOST}',
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
        "goods_group"=>"goods_group",
        "goods_get_image"=>"goods_get_image",
        "goods_is_set"=>"goods_is_set",
        "goods_group_id"=>"goods_group_id",
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
        "isku"=>"isku_isku",
        "isku_title"=>"isku_isku_title",
        "isku_image"=>"isku_image",
        "goods_group_name"=>"group_group_name",
        "isku_head_id" => "isku_head_id",
        "isku_developer_id" => "isku_developer_id",
        "goods_operation_user_admin_name" => "goods_operation_user_admin_name",
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

    public function __construct(
        string $dbhost = '',
        string $codeno = '',
        ?LoggerInterface $logger = null,
        ?ClientInterface $httpClient = null
    ) {
        if (!static::$detectSchemaName) {
            static::$detectSchemaName = true;
            $ods = env('PRESTO_SCHEMA_ODS', 'ods');
            $dws = env('PRESTO_SCHEMA_DWS', 'dws');
            $dim = env('PRESTO_SCHEMA_DIM', 'dim');
            
            foreach (static::$tableMaps as &$v) {
                $schema = substr($v, 0, 4);
                $v = ([
                    'ods.' => $ods,
                    'dws.' => $dws,
                    'dim.' => $dim,
                ][$schema] ?? substr($schema, 0, 3)) . substr($v, 3);
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

        $this->presto = Presto::getConnection($config, $this->logger, $httpClient);
    }

    protected function getCache()
    {
        if (null === $this->cache) {
            $this->cache = ApplicationContext::getContainer()->get(CacheInterface::class);
        }

        return $this->cache;
    }

    public function query(string $sql, array $bindings = [], bool $isCache = false, int $cacheTTL = 300): array
    {
        if ($bindings) {
            $this->lastSql = $psql = "PREPARE {$sql}; EXECUTE " . @join(',', $bindings);
        } else {
            $this->lastSql = $psql = $sql;
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($psql);
        if ($isCache) {
            $result = $this->getCache()->get($cacheKey);
            if (!empty($result)) {
                return $result;
            }
        }

        $result = $this->presto->query($sql, ...$bindings);
        if (false === $result) {
            $this->logger->error("sql: {$psql} error:执行sql异常");
            return [];
        }

        if ($isCache) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }
        return $result;
    }

    public function fetch(string $sql, array $bindings = [], bool $isCache = false, int $cacheTTL = 300): array
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
     * @return array		查询结果集数组
     */
    public function select(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300,
        $isJoin = 0
    ): array {
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;

        $where = empty($where) ? '' : " WHERE {$where}";
        $order = empty($order) ? '' : " ORDER BY {$order}";
        $group = empty($group) ? '' : " GROUP BY {$group}";
        $limit = is_string($limit) || is_numeric($limit) ? trim((string)$limit) : '';

        if (!empty($limit)) {
            // 兼容 $limit = '1', '1, 2', 'limit 1,2', 'limit 1 offset 2', 'offset 1 limit 2' 等形式
            if (false !== strpos($limit, ',')) {
                list($offset, $limit) = explode(',', $limit, 2);
                if (1 === preg_match('/\s*limit\s+(\d+)/i', $offset, $m)) {
                    $offset = $m[1];
                }

                // presto 语法必须 offset 在前，且不支持 limit 1,2 这种写法
                $limit = " OFFSET {$offset} LIMIT {$limit}";
            } else {
                if (is_numeric($limit)) {
                    $limit = " LIMIT {$limit}";
                } elseif (1 === preg_match('/\s*offset\s+(\d+)\s+limit\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[1]} LIMIT {$m[2]}";
                } elseif (1 === preg_match('/\s*limit\s+(\d+)\s+offset\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[2]} LIMIT {$m[1]}";
                }
            }
        }

        $sql = $this->lastSql = "SELECT {$data} FROM {$table} {$where} {$group} {$order} {$limit}";

        //商品级
        //print_r($this->goodsCols);
        //if($isJoin==1){
            foreach ($this->goodsCols as $key => $value){
                if (!is_array($value)) {
                    $sql = str_replace('report.' . $key, 'amazon_goods.' . $value, $sql);
                    $sql = str_replace('report."' . $key.'"', 'amazon_goods.' . $value, $sql);

                } else {
                    if (strpos('_day_report_', $table)) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['day'], $sql);
                    } elseif (strpos('_week_report_', $table)) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['week'], $sql);
                    } elseif (strpos('_month_report_', $table)) {
                        $sql = str_replace('report.' . $key, 'amazon_goods.' . $value['month'], $sql);
                    }
                }
            }
        //}

        $this->logSql();
        if ($this->logDryRun()) {
            return [];
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($sql);
        if ($isCache) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }
        $result = $this->presto->query($sql);
        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('presto 查询失败');
        }

        if ($isCache) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }

        return $result;
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
        bool $isCache = false,
        int $cacheTTL = 300,
        $isJson = 0
    ): array {
        $result = $this->select($where, $data, $table, 1, $order, $group, $isCache, $cacheTTL,$isJson);

        return $result[0] ?? [];
    }

    /** @see $this->getOne */
    public function get_one(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array {
        return $this->getOne($where, $data, $table, $order, $group, $isCache, $cacheTTL);
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
        bool $isCache = false,
        int $cacheTTL = 300,
        int $isJoin = 0
    ): int {
        $where = is_array($where) ? $this->sqls($where) : $where;

        if ($group) {
            $data = $data ?: '1';
            $result = $this->getOne(
                $where,
                "count(distinct($group)) AS num",
                "$table",
                '',
                '',
                $isCache,
                $cacheTTL,
                $isJoin
            );
        } elseif (!empty($cols)) {
            $result = $this->getOne($where, "COUNT({$cols}) AS num", $table, '', '', $isCache, $cacheTTL, $isJoin);
        } else {
            $result = $this->getOne($where, "COUNT(*) AS num", $table, '', '', $isCache, $cacheTTL, $isJoin);
        }

        return intval($result['num'] ?? 0);
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
            if (false !== strpos($tableName, '{DBHOST}')) {
                $tableName = strtr($tableName, ['{DBHOST}' => \app\getUserInfo()['dbhost'] ?? '']);
            }

            return $tableName;
        }

        return strpos($name, 'table_') === 0 ? '' : null;
    }
}
