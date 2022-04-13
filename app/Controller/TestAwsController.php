<?php

namespace App\Controller;

use App\Lib\Redis;
use Aws\Athena\AthenaClient;
use Aws\Credentials\Credentials;
use Aws\S3\S3Client;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Captainbi\Hyperf\Util\Result;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Annotation\Controller;

/**
 * @Controller()
 */
class TestAwsController extends AbstractController
{

    public function testAws(){

        $redis = new Redis();

        $mysql_finance_fields1 = $redis->get("mysql_finance_fields_new");
        if (!empty($mysql_finance_fields1)){
            $redis->set("mysql_finance_fields_new",$mysql_finance_fields1,1);

        }
        $mysql_finance_fields = $redis->get("mysql_finance_fields_new");
        return [$mysql_finance_fields1,$mysql_finance_fields];
        $credentials = new Credentials('', '');
//        $credentials = array(
//            "key" => "",
//            "secret" => "+"
//        );
        $options = [
           //"api_provider" => '',//一api_provider 类型callable,一个 PHP 可调用对象，它接受类型、服务和版本参数，并返回相应配置数据的数组。类型值可以是一个 api，waiter或paginator。//默认情况下，SDK 使用Aws\Api\FileSystemApiProvider 从src/dataSDK 文件夹加载 API 文件的实例。
//            'csm' => [//指定用于签署请求的凭据。提供 Aws\ClientSideMonitoring\ConfigurationInterface 对象、用于创建客户端监控配置的可调用配置提供程序、false禁用 csm 或具有以下键的关联数组
//                "enabled" => false,//(bool) 设置为 true 以启用客户端监控，默认为false
//                "host" => "172.16.13.122",//host：（字符串）发送监控事件的主机位置，默认为 127.0.0.1；port：（int）用于主机连接的端口，默认为31000；client_id：（字符串）此项目的标识符
//                "port" => 8889
//            ],
            "debug"         => false,//debug: (bool|array) 设置为 true 以在发送请求时显示调试信息。或者，您可以提供具有以下键的关联数组： logfn: (callable) 使用日志消息调用的函数；stream_size: (int) 当流的大小大于这个数字时，流数据不会被记录（设置为“0”不记录任何流数据）；Scrub_auth: (bool) 设置为 false 以禁用从记录的消息中清除 auth 数据；http: (bool) 设置为 false 以禁用较低级别 HTTP 适配器的“调试”功能（例如，详细的 curl 输出）。
//            "stats" => true,//stats: (bool|array) 设置为 true 以收集有关发送请求的传输统计信息。或者，您可以提供具有以下键的关联数组： retries: (bool) 设置为 false 以禁用重试尝试的报告；http: (bool) 设置为 true 以启用从较低级别的 HTTP 适配器收集统计信息（例如，在 GuzzleHttp\TransferStats 中返回的值）。HTTP 处理程序必须支持一个http_stats_receiver选项才能生效；timer: (bool) 设置为 true 以启用命令计时器，该计时器报告在操作上花费的总挂钟时间（以秒为单位）。
        //disable_host_prefix_injection: (bool) 设置为 true 以禁用使用它的服务的主机前缀注入逻辑。这将禁用整个前缀注入，包括由用户定义的参数提供的部分。设置此标志对不使用主机前缀注入的服务没有影响。
//            'endpoint'      => "172.16.13.122:8889",//Web 服务的完整 URI。这仅在连接到自定义端点（例如，S3 的本地版本）时才需要。
//            'endpoint'      => "https://athena.cn-northwest-1.amazonaws.com.cn	",
            'credentials'   => $credentials,
            'region'        => 'cn-northwest-1',
            'version'       => 'latest',
        ];
        $sql = "SELECT
	max( report.user_id ) AS \"user_id\",
	max( report.amazon_goods_id ) AS \"goods_id\",
	max( report.site_id ) AS \"site_country_id\",
	min( amazon_goods.goods_price ) AS \"goods_price_min\",
	max( amazon_goods.goods_price ) AS \"goods_price_max\",
	min( amazon_goods.goods_Transport_mode ) AS \"min_transport_mode\",
	max( amazon_goods.goods_Transport_mode ) AS \"max_transport_mode\",
	max( amazon_goods.goods_parent_asin ) AS \"parent_asin\",
	max( amazon_goods.goods_image ) AS \"image\",
	max( amazon_goods.goods_title ) AS \"title\",
	max( report.channel_id ) AS \"channel_id\",
	max( report.site_id ) AS \"site_id\",
	SUM( report.byorder_user_sessions ) AS \"goods_visitors\",
	SUM( report.byorder_quantity_of_goods_ordered ) * 1.0000 / nullif( SUM( report.byorder_user_sessions ), 0 ) AS \"goods_conversion_rate\",
	min(
	nullif( amazon_goods.goods_rank, 0 )) AS \"goods_rank\",
	min(
	nullif( amazon_goods.goods_min_rank, 0 )) AS \"goods_min_rank\",
	SUM( report.byorder_number_of_visits ) AS \"goods_views_number\",
CASE
		
		WHEN max( report.channel_id ) = 349259 THEN
		SUM( report.byorder_number_of_visits ) * 1.0000 / round( 35806, 2 ) 
		WHEN max( report.channel_id ) = 349255 THEN
		SUM( report.byorder_number_of_visits ) * 1.0000 / round( 175253, 2 ) ELSE 0 
	END AS \"goods_views_rate\",
CASE
		
		WHEN max( report.channel_id ) = 349259 THEN
		SUM( report.byorder_user_sessions ) * 1.0000 / round( 31336, 2 ) 
		WHEN max( report.channel_id ) = 349255 THEN
		SUM( report.byorder_user_sessions ) * 1.0000 / round( 154608, 2 ) ELSE 0 
	END AS \"goods_buyer_visit_rate\",
	( SUM( byorder_buy_button_winning_num ) * 1.0000 / nullif( SUM( report.byorder_number_of_visits ), 0 ) ) AS \"goods_buybox_rate\",
	SUM( report.byorder_sales_volume + report.byorder_group_id ) AS \"sale_sales_volume\",
	SUM( report.byorder_group_id ) AS \"sale_many_channel_sales_volume\",
	SUM( report.byorder_sales_quota ) AS \"sale_sales_quota\",
	SUM( report.byorder_refund_num ) AS \"sale_return_goods_number\",
	SUM( 0 - report.byorder_refund ) AS \"sale_refund\",
	SUM( report.byorder_refund_num ) * 1.0000 / nullif( SUM( report.byorder_sales_volume + report.byorder_group_id ), 0 ) AS \"sale_refund_rate\",
	SUM( report.byorder_promote_discount ) AS \"promote_discount\",
	SUM( report.byorder_refund_promote_discount ) AS \"promote_refund_discount\",
	SUM( report.byorder_purchasing_cost ) AS \"purchase_logistics_purchase_cost\",
	SUM( report.byorder_logistics_head_course ) AS \"purchase_logistics_logistics_cost\",(
		SUM( report.byorder_goods_profit )+ SUM( report.byorder_purchasing_cost ) + SUM( report.byorder_logistics_head_course ) 
		) AS \"cost_profit_profit\",(
		SUM( report.byorder_goods_profit )+ SUM( report.byorder_purchasing_cost ) + SUM( report.byorder_logistics_head_course ) 
	) / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"cost_profit_profit_rate\",
	SUM( report.byorder_goods_amazon_fee ) AS \"amazon_fee\",
	SUM( report.byorder_platform_sales_commission + report.byorder_reserved_field21 ) AS \"amazon_sales_commission\",
	SUM( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit ) AS \"amazon_fba_delivery_fee\",
	SUM( report.byorder_profit ) AS \"amazon_multi_channel_delivery_fee\",
	SUM( report.byorder_order_variableclosingfee + report.byorder_fixedclosingfee + report.byorder_refund_variableclosingfee ) AS \"amazon_settlement_fee\",
	SUM( report.byorder_goods_amazon_other_fee ) AS \"amazon_other_fee\",
	SUM( report.byorder_returnshipping ) AS \"amazon_return_shipping_fee\",
	SUM( report.byorder_return_and_return_sales_commission ) AS \"amazon_return_sale_commission\",
	SUM( report.byorder_return_and_return_commission ) AS \"amazon_refund_deducted_commission\",
	SUM( report.byorder_fba_refund_treatment_fee + report.byorder_fbacustomerreturnperorderfee + report.byorder_fbacustomerreturnweightbasedfee ) AS \"amazon_fba_return_processing_fee\",
	SUM( report.byorder_estimated_monthly_storage_fee ) AS \"amazon_fba_monthly_storage_fee\",(
		SUM( report.byorder_goods_amazon_fee )) * 1.0000 / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"amazon_fee_rate\",(
		SUM( report.byorder_purchasing_cost ) + SUM( report.byorder_logistics_head_course ) 
	) * 1.0000 / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"purchase_logistics_cost_rate\",
	SUM( 0- report.byorder_reserved_field16 ) AS \"operate_fee\",(
		SUM( 0- report.byorder_reserved_field16 ) 
	) * 1.0000 / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"operate_fee_rate\",
	SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) AS \"cpc_cost\",(
		SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) 
	) * 1.0000 / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"cpc_cost_rate\",
	SUM( report.byorder_reserved_field1 + report.byorder_reserved_field2 ) AS \"cpc_exposure\",
	SUM( report.byorder_cpc_sd_clicks + report.byorder_cpc_sp_clicks ) AS \"cpc_click_number\",(
	SUM( report.byorder_cpc_sd_clicks + report.byorder_cpc_sp_clicks )) * 1.0000 / nullif( SUM( report.byorder_reserved_field1 + report.byorder_reserved_field2 ), 0 ) AS \"cpc_click_rate\",
	SUM( report.\"byorder_sp_attributedConversions7d\" + report.\"byorder_sd_attributedConversions7d\" ) AS \"cpc_order_number\",(
		SUM( report.\"byorder_sp_attributedConversions7d\" + report.\"byorder_sd_attributedConversions7d\" ) 
		) * 1.0000 / nullif( SUM( report.byorder_sales_volume + report.byorder_group_id ), 0 ) AS \"cpc_order_rate\",(
		SUM( report.\"byorder_sp_attributedConversions7d\" + report.\"byorder_sd_attributedConversions7d\" ) 
	) * 1.0000 / nullif( SUM( report.byorder_cpc_sd_clicks + report.byorder_cpc_sp_clicks ), 0 ) AS \"cpc_click_conversion_rate\",
	SUM( report.\"byorder_sp_attributedSales7d\" + report.\"byorder_sd_attributedSales7d\" ) AS \"cpc_turnover\",(
		SUM( report.\"byorder_sp_attributedSales7d\" + report.\"byorder_sd_attributedSales7d\" )) * 1.0000 / nullif( SUM( report.byorder_sales_quota ), 0 ) AS \"cpc_turnover_rate\",(
		SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) 
		) * 1.0000 / nullif( SUM( report.byorder_cpc_sd_clicks + report.byorder_cpc_sp_clicks ), 0 ) AS \"cpc_avg_click_cost\",(
		SUM( report.byorder_cpc_cost + report.byorder_cpc_sd_cost ) 
	) * 1.0000 / nullif( SUM( report.\"byorder_sp_attributedSales7d\" + report.\"byorder_sd_attributedSales7d\" ), 0 ) AS \"cpc_acos\",
	SUM( report.\"byorder_sd_attributedConversions7dSameSKU\" + report.\"byorder_sp_attributedConversions7dSameSKU\" ) AS \"cpc_direct_sales_volume\",
	SUM( report.\"byorder_sd_attributedSales7dSameSKU\" + report.\"byorder_sp_attributedSales7dSameSKU\" ) AS \"cpc_direct_sales_quota\",(
	SUM( report.\"byorder_sd_attributedConversions7dSameSKU\" + report.\"byorder_sp_attributedConversions7dSameSKU\" )) * 1.0000 / nullif( SUM( report.byorder_sales_volume + report.byorder_group_id ), 0 ) AS \"cpc_direct_sales_volume_rate\",
	SUM( report.\"byorder_sp_attributedConversions7d\" + report.\"byorder_sd_attributedConversions7d\" - report.\"byorder_sd_attributedConversions7dSameSKU\" - report.\"byorder_sp_attributedConversions7dSameSKU\" ) AS \"cpc_indirect_sales_volume\",
	SUM( report.\"byorder_sd_attributedSales7d\" + report.\"byorder_sp_attributedSales7d\" - report.\"byorder_sd_attributedSales7dSameSKU\" - report.\"byorder_sp_attributedSales7dSameSKU\" ) AS \"cpc_indirect_sales_quota\",(
		SUM( report.\"byorder_sp_attributedConversions7d\" + report.\"byorder_sd_attributedConversions7d\" - report.\"byorder_sd_attributedConversions7dSameSKU\" - report.\"byorder_sp_attributedConversions7dSameSKU\" ) 
	) * 1.0000 / nullif( SUM( report.byorder_sales_volume + report.byorder_group_id ), 0 ) AS \"cpc_indirect_sales_volume_rate\",
	SUM( 0-report.byorder_reserved_field17 ) AS \"other_vat_fee\",
	1 AS \"fba_sales_stock\",
	1 AS \"fba_sales_day\",
	1 AS \"fba_reserve_stock\",
	1 AS \"fba_recommended_replenishment\",
	1 AS \"fba_special_purpose\",
	SUM( report.byorder_fba_sales_quota ) AS \"fba_sales_quota\",
	SUM( report.byorder_fbm_sales_quota ) AS \"fbm_sales_quota\",
	SUM( report.byorder_fba_refund_num ) AS \"fba_refund_num\",
	SUM( report.byorder_fbm_refund_num ) AS \"fbm_refund_num\",
	SUM( report.byorder_fba_logistics_head_course ) AS \"fba_logistics_head_course\",
	SUM( report.byorder_fbm_logistics_head_course ) AS \"fbm_logistics_head_course\",
	SUM( report.byorder_shipping_charge ) AS \"shipping_charge\",
	SUM( report.byorder_tax ) AS \"tax\",
	SUM( report.byorder_ware_house_lost ) AS \"ware_house_lost\",
	SUM( report.byorder_ware_house_damage ) AS \"ware_house_damage\",
	SUM( report.byorder_sales_quota ) * 1.0000 / NULLIF( SUM( report.byorder_sales_volume + report.byorder_group_id ), 0 ) AS \"sale_sales_quota_rate\",(
	SUM( report.byorder_promote_discount )+ SUM( report.byorder_refund_promote_discount )) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"promote_discount_rate\",
	SUM( report.byorder_purchasing_cost ) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"purchase_logistics_purchase_cost_rate\",
	SUM( report.byorder_logistics_head_course ) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"purchase_logistics_logistics_cost_rate\",
	SUM( report.byorder_platform_sales_commission + report.byorder_reserved_field21 ) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"sales_commission_rate\",
	SUM( report.byorder_fba_generation_delivery_cost + report.byorder_fbaperorderfulfillmentfee + report.byorder_fbaweightbasedfee - report.byorder_profit ) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"fba_delivery_fee_rate\",
	SUM( report.byorder_estimated_monthly_storage_fee ) * 1.0000 / NULLIF( SUM( report.byorder_sales_quota ), 0 ) AS \"fba_monthly_storage_fee_rate\",
	'{fba_special_purpose}/{sale_sales_volume}' AS \"fba_sale_rate\",
	'{fba_special_purpose}*{purchase_logistics_purchase_cost}' AS \"fba_stock_value\" 
FROM
	dws.dws_dataark_f_dw_goods_day_report_001 AS report
	JOIN dim.dim_dataark_f_dw_goods_dim_report_001 AS amazon_goods ON report.amazon_goods_id = amazon_goods.es_id 
WHERE
	report.ym IN ( '20206','20207','20208','20209','202010','202011','202012','20211', '20212', '20213', '20214', '20215', '20216' ) 
	AND report.user_id_mod = 1 
	AND amazon_goods.goods_user_id_mod = 1
	AND report.available = 1 
	AND report.user_id = 21
	AND amazon_goods.goods_user_id = 21 
	AND (
		(
			report.site_id IN (
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
				17,
				18 
			) 
			AND report.create_time >= 1590940800 
			AND report.create_time <= 1623081599 
		) 
	) 
	AND amazon_goods.goods_parent_asin != '' 
GROUP BY
	amazon_goods.goods_parent_asin,
	report.channel_id 
ORDER BY
	(( SUM( report.byorder_user_sessions )) IS NULL ),
	( SUM( report.byorder_user_sessions ) ) DESC,
	amazon_goods.goods_parent_asin,
	report.channel_id 
	
	";

//        $sql = 'SELECT * FROM (SELECT row_number() over() AS rn, * FROM ods.ods_dataark_b_user_admin WHERE rn BETWEEN 1 AND 2';
        $sql = "SELECT * FROM (
    SELECT row_number() over() AS rn, * FROM ($sql) as t)
WHERE rn BETWEEN 1 AND 100;";
        $param_Query = [
//            'ClientRequestToken' => md5($sql),
//            'QueryExecutionContext' => [
//                'Catalog' => 'hive',
//                'Database' => 'ods',
//            ],
            'QueryString' => $sql, // REQUIRED
            'ResultConfiguration' => [
                'EncryptionConfiguration' => [
                    'EncryptionOption' => 'SSE_S3', // REQUIRED
//                    'KmsKey' => '<string>',
                ],
                'OutputLocation' => 's3://captain-athena-query-result/Athena-Presto/ods_channel_test',
            ],
//            'WorkGroup' => '<string>',
        ];
        $start_time = microtime(true);
        try{
            $athenaClient = new AthenaClient($options);
            $result           = $athenaClient->startQueryExecution($param_Query);
            $QueryExecutionId = $result['QueryExecutionId'];
            $i = 1;
            do{
                usleep(50000);//休眠一小段时间等待执行成功
                $result1 = $athenaClient->getQueryExecution([
                    'QueryExecutionId' => $QueryExecutionId, // REQUIRED
                ]);
                $status = $result1['QueryExecution']['Status']['State'];
                $i++;
            }while($status == "QUEUED" or $status == "RUNNING");
            if ($status == 'SUCCEEDED'){
                $result = $athenaClient->getQueryResults([
//                        'MaxResults' => 50,
                    'QueryExecutionId' => $QueryExecutionId, // REQUIRED

                ]);
            }else{
                return ['error:'.$status];
            }
//return microtime(true) - $start_time;
            var_dump(microtime(true) - $start_time);
            return $result->toArray();
        }catch (\Exception $exception){
            var_dump($exception->getMessage());
            return ['error1111'=>$exception->getMessage()];
        }

    }

    /**
     * @RequestMapping(path="/test/hotGoods", methods="get")
     */
    public function hot_goods(){

        $create_time = 1625068800 + mt_rand(0,10000);
        $sql = "SELECT SQL_NO_CACHE max(report.user_id) AS user_id,max(report.amazon_goods_id) AS goods_id,max(report.site_id) AS site_country_id,min(amazon_goods.goods_price* (1 / (COALESCE(rates.rate ,1)*1.00000))) AS goods_price_min,max(amazon_goods.goods_price* (1 / (COALESCE(rates.rate ,1)*1.00000))) AS goods_price_max, min(amazon_goods.goods_Transport_mode)  AS min_transport_mode, max(amazon_goods.goods_Transport_mode)  AS max_transport_mode,max(amazon_goods.goods_sku) AS sku,max(amazon_goods.goods_image) AS image,max(amazon_goods.goods_title) AS title,max(amazon_goods.goods_asin) AS asin,max(amazon_goods.goods_parent_asin) AS parent_asin,max(amazon_goods.goods_product_category_name_1) AS goods_product_category_name_1,max(amazon_goods.goods_product_category_name_2) AS goods_product_category_name_2,max(amazon_goods.goods_product_category_name_3) AS goods_product_category_name_3,max(amazon_goods.goods_is_care) AS goods_is_care,max(amazon_goods.goods_is_keyword) AS is_keyword,max(amazon_goods.goods_is_new) AS goods_is_new,max(amazon_goods.goods_up_status) AS up_status,max(amazon_goods.goods_isku_id) AS isku_id,max(amazon_goods.goods_fnsku) AS goods_fnsku,max(report.channel_id) AS channel_id,max(report.site_id) AS site_id,max(amazon_goods.goods_product_category_name_1) AS class1,max(amazon_goods.group_group_name) AS group1,max(amazon_goods.goods_operation_user_admin_id) AS goods_operation_user_admin_id,max(amazon_goods.goods_g_amazon_goods_id) AS goods_g_amazon_goods_id,max(amazon_goods.goods_is_remarks) AS is_remarks,SUM(report.byorder_user_sessions) AS goods_visitors,min(nullif(amazon_goods.goods_rank,0)) ASgoods_rank, min(nullif(amazon_goods.goods_min_rank,0)) AS goods_min_rank, sum( report.byorder_number_of_visits )  AS goods_views_number, (sum( byorder_buy_button_winning_num ) * 1.0000 / nullif(sum( report.byorder_number_of_visits ) ,0) )  AS goods_buybox_rate, sum( report.byorder_sales_volume +  report.byorder_group_id )  AS sale_sales_volume,sum( report.byorder_group_id ) AS sale_many_channel_sales_volume,sum( report.byorder_sales_quota * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS sale_sales_quota,sum(report.report_refund_num ) AS sale_return_goods_number,sum( (0 - report.report_refund) * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS sale_refund,SUM(report.report_promote_discount * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS promote_discount,SUM(report.report_refund_promote_discount * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS promote_refund_discount, sum( report.report_purchasing_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS purchase_logistics_purchase_cost, sum( report.report_logistics_head_course * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS purchase_logistics_logistics_cost,sum((report.report_goods_amazon_fee- ( CASE WHEN report.site_id IN (4,5,6,7,8,9,11,14,15,16,17,18) THEN report.report_tax ELSE 0 END )) * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS amazon_fee,sum( (0 -  report.byorder_reserved_field16) * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS operate_fee,sum( report.report_reserved_field10 * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS evaluation_fee, sum( report.byorder_cpc_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)))  AS cpc_sp_cost, sum( report.byorder_cpc_sd_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)))  AS cpc_sd_cost, sum( report.byorder_cpc_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) + report.byorder_cpc_sd_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) )  AS cpc_cost,sum( report.byorder_reserved_field1 + report.byorder_reserved_field2 ) AS cpc_exposure,sum( report.byorder_cpc_sd_clicks +report.byorder_cpc_sp_clicks ) AS cpc_click_number,sum( report.byorder_sp_attributedConversions7d + report.byorder_sd_attributedConversions7d )  AS cpc_order_number,sum( report.byorder_sp_attributedSales7d * (1 / (COALESCE(rates.rate ,1)*1.00000)) + report.byorder_sd_attributedSales7d * (1/ (COALESCE(rates.rate ,1)*1.00000))  ) AS cpc_turnover,(sum( report.byorder_sp_attributedSales7d * (1 / (COALESCE(rates.rate ,1)*1.00000)) + report.byorder_sd_attributedSales7d * (1 / (COALESCE(rates.rate ,1)*1.00000))  )) * 1.0000 / nullif( sum( report.byorder_sales_quota * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) , 0 )  AS cpc_turnover_rate,( sum( report.byorder_cpc_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) + report.byorder_cpc_sd_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) ) * 1.0000 / nullif( sum( report.byorder_sp_attributedSales7d * (1 / (COALESCE(rates.rate ,1)*1.00000)) + report.byorder_sd_attributedSales7d * (1 / (COALESCE(rates.rate ,1)*1.00000))  ) , 0 )  AS cpc_acos,SUM((0-report.report_reserved_field17) * (1 / (COALESCE(rates.rate ,1)*1.00000))) AS other_vat_fee,1 AS fba_sales_stock,1 AS fba_sales_day,1 AS fba_reserve_stock,1 AS fba_recommended_replenishment,sum( report.byorder_sales_quota * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS cost_profit_total_income,sum((report.report_goods_amazon_fee- ( CASE WHEN report.site_id IN (4,5,6,7,8,9,11,14,15,16,17,18) THEN report.report_tax ELSE 0 END )) * (1 / (COALESCE(rates.rate ,1)*1.00000)) )+sum( (0 - report.report_refund) * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) +SUM(report.report_promote_discount * (1 / (COALESCE(rates.rate ,1)*1.00000)) )+ sum( report.byorder_cpc_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)))
                                                + sum( report.byorder_cpc_sd_cost * (1 / (COALESCE(rates.rate ,1)*1.00000))) + sum( report.report_purchasing_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) + sum( report.report_logistics_head_course * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) +sum( report.report_reserved_field10 * (1 / (COALESCE(rates.rate ,1)*1.00000)) )
                                                +sum( (0 -  report.byorder_reserved_field16) * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) +SUM((0-report.report_reserved_field17) * (1 / (COALESCE(rates.rate ,1)*1.00000)))+SUM(report.report_refund_promote_discount * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) AS cost_profit_total_pay,(SUM((report.report_goods_profit- ( CASE WHEN report.site_id IN (4,5,6,7,8,9,11,14,15,16,17,18) THEN report.report_tax ELSE 0 END ) + report.byorder_sales_quota - report.report_sales_quota  ) * (1 / (COALESCE(rates.rate ,1)*1.00000)))+ sum( report.report_purchasing_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) + sum( report.report_logistics_head_course * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) ) AS cost_profit_profit,(SUM((report.report_goods_profit- ( CASE WHEN report.site_id IN (4,5,6,7,8,9,11,14,15,16,17,18) THEN report.report_tax ELSE 0 END ) + report.byorder_sales_quota - report.report_sales_quota  ) * (1 / (COALESCE(rates.rate ,1)*1.00000)))+ sum( report.report_purchasing_cost * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) + sum( report.report_logistics_head_course * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) ) /  nullif( sum( report.byorder_sales_quota * (1 / (COALESCE(rates.rate ,1)*1.00000)) ) , 0 )  AS cost_profit_profit_rate FROM dwsslave.dws_dataark_f_dw_goods_day_report_001 AS report JOIN dimslave.dim_dataark_f_dw_goods_dim_report_001 AS amazon_goods on report.amazon_goods_id=amazon_goods.es_id LEFT JOIN odsslave.ods_dataark_b_site_rate as rates ON rates.site_id = report.site_id AND rates.user_id = 0   WHERE report.ym = '20217' AND report.user_id_mod = 17 and amazon_goods.goods_user_id_mod=17 AND report.available = 1  AND report.user_id=597 AND report.channel_id IN (6842,6844,6846,6847,6848,6849,6850,6853,6854,6855,6857,6858,6859,7328,7329,7330,7331,7332,7333,8907,10083,10084,10085,10086,10087,17963,23876,47751,47752,59468,59469,59470,59471,59472,161446,161447,161448,170163,170164,170165,170166,170167,205873,205874,205875,205876,205877,482969,482970,482977,482978) and amazon_goods.goods_user_id=597 AND amazon_goods.goods_channel_id IN (6842,6844,6846,6847,6848,6849,6850,6853,6854,6855,6857,6858,6859,7328,7329,7330,7331,7332,7333,8907,10083,10084,10085,10086,10087,17963,23876,47751,47752,59468,59469,59470,59471,59472,161446,161447,161448,170163,170164,170165,170166,170167,205873,205874,205875,205876,205877,482969,482970,482977,482978) AND report.create_time>={$create_time} and report.create_time<=1627747199 AND amazon_goods.goods_sku != ''   GROUP BY amazon_goods.goods_sku ,report.channel_id   ORDER BY (( sum( report.byorder_sales_volume +  report.byorder_group_id ) ) IS NULL) ,  ( sum( report.byorder_sales_volume +  report.byorder_group_id )  ) desc , amazon_goods.goods_sku ,report.channel_id   LIMIT 50";

        $dataChannel =  'Presto' ;
        $className = "\\App\\Model\\DataArk\\AmazonGoodsFinanceReportByOrder{$dataChannel}Model";
        $amazonGoodsFinanceReportByOrderMD = new $className('001', '001',false);
        $time = microtime(true);
        $data = $amazonGoodsFinanceReportByOrderMD->query($sql, [], false, 300, true);

        return [$data,microtime(true) - $time];
    }

}
