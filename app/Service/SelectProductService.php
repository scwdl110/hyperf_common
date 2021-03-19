<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace App\Service;

use Captainbi\Hyperf\Util\Elasticsearch;
use Captainbi\Hyperf\Util\Http;
use Elasticsearch\Client;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class SelectProductService extends BaseService {
    protected $delivery = [
        0 => "FBA",
        1 => "FBM",
        2 => "Amazon"
    ];

    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @param $request_data
     * @return array
     */
    public function category_list($request_data){
        //验证
        $rule = [
            'country_id' => 'required|integer|in:1,4,9,11',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
            'search_value' => 'string|filled',
            'level' => 'string|filled|filled'
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }
        //es初始索引
        $i = 0;

        //es配置
        $esConfig = $this->config->get("es");

        //获取
        $es = new Elasticsearch();
//        $client = $es->getClient(['192.168.1.90:9200']);
        $client = $es->getClient($esConfig);

        $params = [
            'body'    => [
                "_source" => [
                    "cate_name",
                    "cate_id",
                    "p_l1_id",
                    "p_l1_name",
                    "p_l2_id",
                    "p_l2_name",
                    "p_l3_id",
                    "p_l3_name",
                    "p_l4_id",
                    "p_l4_name",
                    "p_l5_id",
                    "p_l5_name",
                    "p_l6_id",
                    "p_l6_name",
                    "p_l7_id",
                    "p_l7_name",
                    "p_l8_id",
                    "p_l8_name",
                    "p_l9_id",
                    "p_l9_name",
                    "p_l10_id",
                    "p_l10_name"
                ],
            ]
        ];

        if(isset($request_data['limit']) && isset($request_data['offset'])){
            $params['body']['from'] = $request_data['offset'];
            $params['body']['size'] = $request_data['limit'];
        }else{
            $params['body']['from'] = 0;
            $params['body']['size'] = 10000;
        }

        if(isset($request_data['level'])){
            $params['body']['query']['bool']['must'][$i]['terms']['level'] = explode(",", $request_data['level']);
            $i++;
        }

        if(isset($request_data['search_value'])){
            $params['body']['query']['bool']['must'][$i]['bool']['should'][]['wildcard']['cate_name'] = "*".$request_data['search_value']."*";
            $params['body']['query']['bool']['must'][$i]['bool']['should'][]['wildcard']['cate_name_cn.keyword'] = "*".$request_data['search_value']."*";
            $i++;
        }
        //国家
        $suffix = $this->config->get("site.".$request_data['country_id'].".country_suffix");
        $params['index'] = "amazon_selection_".$suffix;

        $response = $client->search($params);

        $response_data = [];
        $fillArr = [
            "cate_name" => 1,
            "cate_id"   => 0,
            "p_l1_id"   => 1,
            "p_l1_name" => 1,
            "p_l2_id"   => 1,
            "p_l2_name" => 1,
            "p_l3_id"   => 1,
            "p_l3_name" => 1,
            "p_l4_id"   => 1,
            "p_l4_name" => 1,
            "p_l5_id"   => 1,
            "p_l5_name" => 1,
            "p_l6_id"   => 1,
            "p_l6_name" => 1,
            "p_l7_id"   => 1,
            "p_l7_name" => 1,
            "p_l8_id"   => 1,
            "p_l8_name" => 1,
            "p_l9_id"   => 1,
            "p_l9_name" => 1,
            "p_l10_id"  => 1,
            "p_l10_name"=> 1
        ];
        foreach ($response['hits']['hits'] as $k=>$v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            //获取伪树状结构
            $path = $this->path_tree($v['_source'], intval($v['_source']['cate_id']), $v['_source']['cate_name']);

            $response_data[] = [
                "category" => $v['_source']['cate_name'],
                "category_id" => $v['_source']['cate_id'],
                'path' => $path
            ];
        }

        $data =  [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'num' => $response['hits']['total']['value'],
                'list'=> $response_data
            ]
        ];

        return $data;
    }


    /**
     * @param $request_data
     * @return array
     */
    public function product_list($request_data){
        //验证
        $rule = [
            'country_id' => 'required|integer|in:1,4,9,11',
            'product_id' => 'string|filled',
            'category_id' => 'string|filled',
            'min_month_salesamount' => 'integer|filled',
            'max_month_salesamount' => 'integer|filled',
            'min_month_revenue' => 'numeric|filled',
            'max_month_revenue' => 'numeric|filled',
            'asin_price_min' => 'numeric|filled',
            'asin_price_max' => 'numeric|filled',
            'min_customer_reviews' => 'integer|filled',
            'max_customer_reviews' => 'integer|filled',
            'min_score' => 'numeric|filled',
            'max_score' => 'numeric|filled',
            'min_best_sellers_rank' => 'integer|filled',
            'min_best_sellers_rank' => 'integer|filled',
            'max_best_sellers_rank' => 'integer|filled',
            'max_best_sellers_rank' => 'integer|filled',
            'min_follow_sellers_num' => 'integer|filled',
            'max_follow_sellers_num' => 'integer|filled',
            'is_best_seller' => 'integer|in:0,1|filled',
            'is_brand' => 'integer|in:0,1|filled',
            'chinese_sellers' => 'integer|in:0,1|filled',
            'is_ama_choice' => 'integer|in:0,1|filled',
            'delivery' => 'integer|in:0,1,2|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }

        //es配置
        $esConfig = $this->config->get("es");

        //获取
        $es = new Elasticsearch();
//        $client = $es->getClient(['192.168.1.90:9200']);
        $client = $es->getClient($esConfig);

        $categoryIdArr = [];

        $params = [
            'body'    => [
                "_source" => [
                    "id",
                    "asin",
                    "category_id",
                    "title",
                    "category",
                    "asin_price_min",
                    "best_sellers_rank",
                    "category_estimate_month_sales",
                    "category_estimate_month_sales_volume",
                    "customer_reviews",
                    "score",
                    "delivery",
                    "is_best_seller",
                    "chinese_sellers",
                    "is_ama_choice",
                    "shipping_weight",
                    "follow_sellers_num",
                    "img_url",
                    "brand"
                ],
                "sort" => [
                    [
                        "category_estimate_month_sales" => [
                            "order" => "desc"
                        ]
                    ]
                ]
            ]
        ];
        if(isset($request_data['limit']) && isset($request_data['offset'])){
            $params['body']['from'] = $request_data['offset'];
            $params['body']['size'] = $request_data['limit'];
        }else{
            $params['body']['from'] = 0;
            $params['body']['size'] = 10000;
        }

        if(isset($request_data['category_id'])){
            $categoryIdArr = $this->get_son_cate_id(explode(",",$request_data['category_id']), $client);
            $params['body']['query']['bool']['must'][]['terms']['category_id'] = $categoryIdArr;
        }

        if(isset($request_data['product_id'])){
            $params['body']['query']['bool']['must'][]['terms']['id'] = explode(",", $request_data['product_id']);
        }

        if(isset($request_data['min_month_salesamount']) && isset($request_data['max_month_salesamount'])){
            $params['body']['query']['bool']['must'][]['range']['month_salesamount'] = [
                "gte" => $request_data['min_month_salesamount'],
                "lte" => $request_data['max_month_salesamount'],
            ];
        }

        if(isset($request_data['min_month_revenue']) && isset($request_data['max_month_revenue'])){
            $params['body']['query']['bool']['must'][]['range']['month_revenue'] = [
                "gte" => $request_data['min_month_revenue'],
                "lte" => $request_data['max_month_revenue'],
            ];
        }

        if(isset($request_data['asin_price_min']) && isset($request_data['asin_price_max'])){
            $params['body']['query']['bool']['must'][]['range']['asin_price_min'] = [
                "gte" => $request_data['asin_price_min'],
                "lte" => $request_data['asin_price_max'],
            ];
        }

        if(isset($request_data['min_customer_reviews']) && isset($request_data['max_customer_reviews'])){
            $params['body']['query']['bool']['must'][]['range']['customer_reviews'] = [
                "gte" => $request_data['min_customer_reviews'],
                "lte" => $request_data['max_customer_reviews'],
            ];
        }

        if(isset($request_data['min_score']) && isset($request_data['max_score'])){
            $params['body']['query']['bool']['must'][]['range']['score'] = [
                "gte" => $request_data['min_score'],
                "lte" => $request_data['max_score'],
            ];
        }

        if(isset($request_data['min_best_sellers_rank']) && isset($request_data['max_best_sellers_rank'])){
            $params['body']['query']['bool']['must'][]['range']['best_sellers_rank'] = [
                "gte" => $request_data['min_best_sellers_rank'],
                "lte" => $request_data['max_best_sellers_rank'],
            ];
        }

        if(isset($request_data['min_follow_sellers_num']) && isset($request_data['max_follow_sellers_num'])){
            $params['body']['query']['bool']['must'][]['range']['follow_sellers_num'] = [
                "gte" => $request_data['min_follow_sellers_num'],
                "lte" => $request_data['max_follow_sellers_num'],
            ];
        }

        if(isset($request_data['is_best_seller'])){
            $params['body']['query']['bool']['must'][]['term']['is_best_seller'] = $request_data['is_best_seller'];
        }

        if(isset($request_data['is_brand']) && $request_data['is_brand'] == 1){
            $params['body']['query']['bool']['must'][]['exists']['field'] = $request_data['brand'];
        }elseif(isset($request_data['is_brand']) && $request_data['is_brand'] == 0){
            $params['body']['query']['bool']['must_not'][]['exists']['field'] = $request_data['brand'];
        }

        if(isset($request_data['chinese_sellers'])){
            $params['body']['query']['bool']['must'][]['term']['chinese_sellers'] = $request_data['chinese_sellers'];
        }

        if(isset($request_data['is_ama_choice'])){
            $params['body']['query']['bool']['must'][]['term']['is_ama_choice'] = $request_data['is_ama_choice'];
        }

        if(isset($request_data['delivery']) && isset($this->delivery[$request_data['delivery']])){
            $params['body']['query']['bool']['must'][]['term']['delivery'] = $this->delivery[$request_data['delivery']];
        }

        //国家
        $suffix = $this->config->get("site.".$request_data['country_id'].".country_suffix");
        $params['index'] = "amazon_market_".$suffix;


        $response = $client->search($params);
        $allCate = $this->get_cate($client, $categoryIdArr);
        $response_data = [];
        //delivery互换key value
        $deliveryNum = array_flip($this->delivery);

        $fillArr = [
            "id"   => 0,
            "asin" => 1,
            "category_id" => 0,
            "title" => 1,
            "category" => 1,
            "asin_price_min" => 0,
            "best_sellers_rank" => 0,
            "category_estimate_month_sales" => 0,
            "category_estimate_month_sales_volume" => 0,
            "customer_reviews" => 1,
            "score" => 2,
            "delivery" => 1,
            "is_best_seller" => 0,
            "chinese_sellers" => 0,
            "is_ama_choice" => 0,
            "shipping_weight" => 0,
            "follow_sellers_num" => 0,
            "img_url" => 1,
            "brand" => 1
        ];
        foreach ($response['hits']['hits'] as $k=>$v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            $response_data[] = [
                "product_id" => $v['_source']['id'],
                "asin" => $v['_source']['asin'],
                "category_id" => $v['_source']['category_id'],
                "title" => $v['_source']['title'],
                "category" => $v['_source']['category'],
                "asin_price_min" => $v['_source']['asin_price_min'],
                "best_sellers_rank" => $v['_source']['best_sellers_rank'],
                "category_estimate_month_sales" => $v['_source']['category_estimate_month_sales'],
                "category_estimate_month_sales_volume" => $v['_source']['category_estimate_month_sales_volume'],
                "customer_reviews" => $v['_source']['customer_reviews'],
                "score" => $v['_source']['score'],
                "delivery" => isset($deliveryNum[$v['_source']['delivery']])?$deliveryNum[$v['_source']['delivery']]:'',
                "is_best_seller" => $v['_source']['is_best_seller'],
                "chinese_sellers" => $v['_source']['chinese_sellers'],
                "is_ama_choice" => $v['_source']['is_ama_choice'],
                'path' => isset($allCate[$v['_source']['category_id']])?$allCate[$v['_source']['category_id']]:[
                    [
                        'level'    => '1',
                        'cate_id'  =>$v['_source']['category_id'],
                        'cate_name'=>$v['_source']['category'],
                    ]
                ],
                'shipping_weight' => $v['_source']['shipping_weight'],
                'follow_sellers_num' => $v['_source']['follow_sellers_num'],
                'img_url' => $v['_source']['img_url'],
                'brand' => $v['_source']['brand'],
            ];
        }

        $data =  [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'num' => $response['hits']['total']['value'],
                'list'=> $response_data
            ]
        ];

        return $data;
    }

    /**
     * @param $request_data
     * @return array
     */
    public function market_list($request_data){
        //验证
        $rule = [
            'country_id' => 'required|integer|in:1,4,9,11',
            'category_id' => 'string|filled',
            'min_category_estimate_month_sales' => 'integer|filled',
            'max_category_estimate_month_sales' => 'integer|filled',
            'min_category_estimate_month_sales_volume' => 'numeric|filled',
            'max_category_estimate_month_sales_volume' => 'numeric|filled',
            'min_category_sum' => 'integer|filled',
            'max_category_sum' => 'integer|filled',
            'min_category_all_average_score' => 'numeric|filled',
            'max_category_all_average_score' => 'numeric|filled',
            'min_category_all_average_BSR_rank' => 'numeric|filled',
            'max_category_all_average_BSR_rank' => 'numeric|filled',
            'min_category_all_average_price' => 'numeric|filled',
            'max_category_all_average_price' => 'numeric|filled',
            'min_category_all_average_weight' => 'numeric|filled',
            'max_category_all_average_weight' => 'numeric|filled',
            'min_customer_reviews' => 'numeric|filled',
            'max_customer_reviews' => 'numeric|filled',
            'min_category_all_average_variants_num' => 'numeric|filled',
            'max_category_all_average_variants_num' => 'numeric|filled',
            'min_category_brands_sum' => 'integer|filled',
            'max_category_brands_sum' => 'integer|filled',
            'min_category_brands_sum_percent' => 'number|filled',
            'max_category_brands_sum_percent' => 'number|filled',
            'min_category_FBA_sum_percent' => 'number|filled',
            'max_category_FBA_sum_percent' => 'number|filled',
            'min_category_FBM_sum_percent' => 'number|filled',
            'max_category_FBM_sum_percent' => 'number|filled',
            'min_category_AMZautarky_sum_percent' => 'number|filled',
            'max_category_AMZautarky_sum_percent' => 'number|filled',
            'min_category_chinese_saler_count_percent' => 'number|filled',
            'max_category_chinese_saler_count_percent' => 'number|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }

        //es配置
        $esConfig = $this->config->get("es");

        //获取
        $es = new Elasticsearch();
//        $client = $es->getClient(['192.168.1.90:9200']);
        $client = $es->getClient($esConfig);

        $categoryIdArr = [];

        $params = [
            'body'    => [
                "_source" => [
                    "cate_id",
                    "cate_name",
                    "cate_name_cn",
                    "category_estimate_month_sales",
                    "category_month_average_sales",
                    "category_month_average_sales_volume",
                    "category_all_average_price",
                    "category_top100_average_price",
                    "category_all_average_customer_reviews",
                    "category_top100_average_customer_reviews",
                    "category_top100_average_score",
                    "category_all_average_score",
                    "category_top100_average_ranking",
                    "category_all_average_BSR_rank",
                    "category_exist_brands_sum_percent",
                    "category_FBA_saler_count_percent",
                    "category_FBM_saler_count_percent",
                    "category_AMZ_saler_count_percent",
                    "category_chinese_saler_count_percent",
                    "category_sum",
                    "category_all_average_weight",
                    "category_all_average_variants_num",
                ],
                "sort" => [
                    [
                        "category_estimate_month_sales" => [
                            "order" => "desc"
                        ]
                    ]
                ]
            ]
        ];
        if(isset($request_data['limit']) && isset($request_data['offset'])){
            $params['body']['from'] = $request_data['offset'];
            $params['body']['size'] = $request_data['limit'];
        }else{
            $params['body']['from'] = 0;
            $params['body']['size'] = 10000;
        }

        if(isset($request_data['category_id'])){
            $categoryIdArr = $this->get_son_cate_id(explode(",",$request_data['category_id']), $client);
            $params['body']['query']['bool']['must'][]['terms']['cate_id'] = $categoryIdArr;
        }


        if(isset($request_data['min_category_estimate_month_sales']) && isset($request_data['max_category_estimate_month_sales'])){
            $params['body']['query']['bool']['must'][]['range']['category_estimate_month_sales'] = [
                "gte" => $request_data['min_category_estimate_month_sales'],
                "lte" => $request_data['max_category_estimate_month_sales'],
            ];
        }

        if(isset($request_data['min_category_estimate_month_sales_volume']) && isset($request_data['max_category_estimate_month_sales_volume'])){
            $params['body']['query']['bool']['must'][]['range']['category_estimate_month_sales_volume'] = [
                "gte" => $request_data['min_category_estimate_month_sales_volume'],
                "lte" => $request_data['max_category_estimate_month_sales_volume'],
            ];
        }

        if(isset($request_data['min_category_sum']) && isset($request_data['max_category_sum'])){
            $params['body']['query']['bool']['must'][]['range']['category_sum'] = [
                "gte" => $request_data['min_category_sum'],
                "lte" => $request_data['max_category_sum'],
            ];
        }

        if(isset($request_data['min_category_all_average_score']) && isset($request_data['max_category_all_average_score'])){
            $params['body']['query']['bool']['must'][]['range']['category_all_average_score'] = [
                "gte" => $request_data['min_category_all_average_score'],
                "lte" => $request_data['max_category_all_average_score'],
            ];
        }

        if(isset($request_data['min_category_all_average_BSR_rank']) && isset($request_data['max_category_all_average_BSR_rank'])){
            $params['body']['query']['bool']['must'][]['range']['category_all_average_BSR_rank'] = [
                "gte" => $request_data['min_category_all_average_BSR_rank'],
                "lte" => $request_data['max_category_all_average_BSR_rank'],
            ];
        }

        if(isset($request_data['min_category_all_average_price']) && isset($request_data['max_category_all_average_price'])){
            $params['body']['query']['bool']['must'][]['range']['category_all_average_price'] = [
                "gte" => $request_data['min_category_all_average_price'],
                "lte" => $request_data['max_category_all_average_price'],
            ];
        }

        if(isset($request_data['min_category_all_average_weight']) && isset($request_data['max_category_all_average_weight'])){
            $params['body']['query']['bool']['must'][]['range']['category_all_average_weight'] = [
                "gte" => $request_data['min_category_all_average_weight'],
                "lte" => $request_data['max_category_all_average_weight'],
            ];
        }

        if(isset($request_data['min_customer_reviews']) && isset($request_data['max_customer_reviews'])){
            $params['body']['query']['bool']['must'][]['range']['customer_reviews'] = [
                "gte" => $request_data['min_customer_reviews'],
                "lte" => $request_data['max_customer_reviews'],
            ];
        }

        if(isset($request_data['min_category_all_average_variants_num']) && isset($request_data['max_category_all_average_variants_num'])){
            $params['body']['query']['bool']['must'][]['range']['category_all_average_variants_num'] = [
                "gte" => $request_data['min_category_all_average_variants_num'],
                "lte" => $request_data['max_category_all_average_variants_num'],
            ];
        }

        if(isset($request_data['min_category_brands_sum']) && isset($request_data['max_category_brands_sum'])){
            $params['body']['query']['bool']['must'][]['range']['category_brands_sum'] = [
                "gte" => $request_data['min_category_brands_sum'],
                "lte" => $request_data['max_category_brands_sum'],
            ];
        }

        if(isset($request_data['min_category_brands_sum_percent']) && isset($request_data['max_category_brands_sum_percent'])){
            $params['body']['query']['bool']['must'][]['range']['category_brands_sum_percent'] = [
                "gte" => $request_data['min_category_brands_sum_percent'] / 100,
                "lte" => $request_data['max_category_brands_sum_percent'] / 100,
            ];
        }

        if(isset($request_data['min_category_FBA_sum_percent']) && isset($request_data['max_category_FBA_sum_percent'])){
            $params['body']['query']['bool']['must'][]['range']['category_FBA_sum_percent'] = [
                "gte" => $request_data['min_category_FBA_sum_percent'] / 100,
                "lte" => $request_data['max_category_FBA_sum_percent'] / 100,
            ];
        }

        if(isset($request_data['min_category_FBM_sum_percent']) && isset($request_data['max_category_FBM_sum_percent'])){
            $params['body']['query']['bool']['must'][]['range']['category_FBM_sum_percent'] = [
                "gte" => $request_data['min_category_FBM_sum_percent'] / 100,
                "lte" => $request_data['max_category_FBM_sum_percent'] / 100,
            ];
        }

        if(isset($request_data['min_category_AMZautarky_sum_percent']) && isset($request_data['max_category_AMZautarky_sum_percent'])){
            $params['body']['query']['bool']['must'][]['range']['category_AMZautarky_sum_percent'] = [
                "gte" => $request_data['min_category_AMZautarky_sum_percent'] / 100,
                "lte" => $request_data['max_category_AMZautarky_sum_percent'] / 100,
            ];
        }

        if(isset($request_data['min_category_chinese_saler_count_percent']) && isset($request_data['max_category_chinese_saler_count_percent'])){
            $params['body']['query']['bool']['must'][]['range']['category_chinese_saler_count_percent'] = [
                "gte" => $request_data['min_category_chinese_saler_count_percent'] / 100,
                "lte" => $request_data['max_category_chinese_saler_count_percent'] / 100,
            ];
        }

        //国家
        $suffix = $this->config->get("site.".$request_data['country_id'].".country_suffix");
        $params['index'] = "amazon_selection_".$suffix;

        $response = $client->search($params);
        $allCate = $this->get_cate($client, $categoryIdArr);
        $response_data = [];

        $fillArr = [
            "cate_id"   => 0,
            "cate_name" => 1,
            "cate_name_cn" => 1,
            "category_estimate_month_sales" => 0,
            "category_month_average_sales" => 0,
            "category_month_average_sales_volume" => 0,
            "category_all_average_price" => 0,
            "category_top100_average_price" => 0,
            "category_all_average_customer_reviews" => 0,
            "category_top100_average_customer_reviews" => 0,
            "category_top100_average_score" => 0,
            "category_all_average_score" => 0,
            "category_top100_average_ranking" => 0,
            "category_all_average_BSR_rank" => 0,
            "category_exist_brands_sum_percent" => 0,
            "category_FBA_saler_count_percent" => 0,
            "category_FBM_saler_count_percent" => 0,
            "category_AMZ_saler_count_percent" => 0,
            "category_chinese_saler_count_percent" => 0,
            "category_sum" => 0,
            "category_all_average_weight" => 0,
            "category_all_average_variants_num" => 0,
        ];
        foreach ($response['hits']['hits'] as $k=>$v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            //大数据没翻译需自己百度翻译
            if(!$v['_source']['cate_name_cn'] && $v['_source']['cate_name']){
                $apiParm = $this->config->get("translateapi.baidu");
                $salt = time();
                $sign = md5($apiParm['appid'].$v['_source']['cate_name'].$salt.$apiParm['secret']);
                $uri = "/api/trans/vip/translate?q=".$v['_source']['cate_name']."&from=auto&to=zh&appid=".$apiParm['appid']."&salt=".$salt."&sign=".$sign;

                //api获取
                $http = new Http();
                $httpClient = $http->getClient($apiParm['api']);
                $httpResponse = $httpClient->get($uri)->getBody()->getContents();
                $httpResponse = json_decode($httpResponse, true);
                if($httpResponse && isset($httpResponse['trans_result'][0]['dst']) && $httpResponse['trans_result'][0]['dst']){
                    $v['_source']['cate_name_cn'] = $httpResponse['trans_result'][0]['dst'];
                    //更新es,优化下次请求
                    $index = $params['index'];
                    $params = [
                        'index' => $index,
                        'type' => '_doc',
                        'id' => $v['_id'],
                        'body' => [
                            'doc' => [
                                'cate_name_cn' => $httpResponse['trans_result'][0]['dst']
                            ]
                        ]
                    ];
                    // 更新文档
                    $client->update($params);
                    //后面翻译付费了可去掉，目前qps为1
                    sleep(1);
                }

            }
            $response_data[] = [
                "cate_name" => $v['_source']['cate_name'],
                "cate_name_trans" => $v['_source']['cate_name_cn'],
                "cate_id" => $v['_source']['cate_id'],
                "category_estimate_month_sales" => $v['_source']['category_estimate_month_sales'],
                "category_month_average_sales" => $v['_source']['category_month_average_sales'],
                "category_month_average_sales_volume" => $v['_source']['category_month_average_sales_volume'],
                "category_all_average_price" => $v['_source']['category_all_average_price'],
                "category_top100_average_price" => $v['_source']['category_top100_average_price'],
                "category_all_average_customer_reviews" => $v['_source']['category_all_average_customer_reviews'],
                "category_top100_average_customer_reviews" => $v['_source']['category_top100_average_customer_reviews'],
                "category_top100_average_score" => $v['_source']['category_top100_average_score'],
                "category_all_average_score" => $v['_source']['category_all_average_score'],
                "category_top100_average_ranking" => $v['_source']['category_top100_average_ranking'],
                "category_all_average_BSR_rank" => $v['_source']['category_all_average_BSR_rank'],
                'path' => isset($allCate[$v['_source']['cate_id']])?$allCate[$v['_source']['cate_id']]:[
                    [
                        'level'    => '1',
                        'cate_id'  =>$v['_source']['cate_id'],
                        'cate_name'=>$v['_source']['cate_name'],
                    ]
                ],
                'category_exist_brands_sum_percent' => $v['_source']['category_exist_brands_sum_percent'] * 100,
                'category_FBA_saler_count_percent' => $v['_source']['category_FBA_saler_count_percent'] * 100,
                'category_FBM_saler_count_percent' => $v['_source']['category_FBM_saler_count_percent'] * 100,
                'category_AMZ_saler_count_percent' => $v['_source']['category_AMZ_saler_count_percent'] * 100,
                'category_chinese_saler_count_percent' => $v['_source']['category_chinese_saler_count_percent'] * 100,
                'category_sum' => $v['_source']['category_sum'],
                'category_all_average_weight' => $v['_source']['category_all_average_weight'],
                'category_all_average_variants_num' => $v['_source']['category_all_average_variants_num'],
            ];
        }

        $data =  [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'num' => $response['hits']['total']['value'],
                'list'=> $response_data
            ]
        ];

        return $data;
    }


    /**
     * 查找子分类id和自己
     * @param array $cateIdArr
     * @param Client $client
     * @return array|bool
     */
    public function get_son_cate_id(array $cateIdArr, Client $client)
    {
        //获取分类id
        $params = [
            'index' => 'amazon_category',
            'body'  => [
                "_source" => [
                    "cate_id",
                    "level"
                ],
                "query" => [
                    "terms" => [
                        "cate_id" => $cateIdArr
                    ]
                ]
            ]
        ];
        $response = $client->search($params);
        if(!$response['hits']['hits']){
            return [];
        }

        //获取子分类id
        $params = [
            'index' => 'amazon_category',
            'body'  => [
                "_source" => [
                    "cate_id",
                ]
            ]
        ];

        $fillArr = [
            "cate_id" => 0,
            "level"   => 0
        ];
        foreach ($response['hits']['hits'] as $v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            $pid = "p_l".$v['_source']['level']."_id";
            $params['body']['query']['bool']['should'][]['term'][$pid] = $v['_source']['cate_id'];
        }

        $response = $client->search($params);
        if(!$response['hits']['hits']){
            return $cateIdArr;
        }

        //返回子分类id和自己
        foreach ($response['hits']['hits'] as $v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            $cateIdArr[] =  $v['_source']['cate_id'];
        }

        return $cateIdArr;
    }

    /**
     * 伪树状
     * @param array $source
     * @param int $cate_id
     * @param string $cate_name
     * @return array
     */
    public function path_tree(array $source, int $cate_id, string $cate_name){
        $path = [];
        //本分类索引值
        $index = 0;
        for ($i=0;$i<=9;$i++){
            $pid = "p_l".($i+1)."_id";
            $pname = "p_l".($i+1)."_name";
            if(isset($source[$pid]) && isset($source[$pname]) && trim($source[$pid])){
                $path[$i]['level'] = $i+1;
                $path[$i]['cate_id'] = $source[$pid];
                $path[$i]['cate_name'] = $source[$pname];
                $index++;
            }else{
                break;
            }
        }

        //加上本分类
        $path[$index]['level'] = $index+1;
        $path[$index]['cate_id'] = $cate_id;
        $path[$index]['cate_name'] = $cate_name;
        return $path;
    }


    /**
     * 查找es获取伪树状
     * @param array $cateIdArr
     * @param Client $client
     * @return array|bool
     */
    public function get_cate(Client $client, array $cateIdArr = [])
    {
        //获取子分类id
        $params = [
            'index' => 'amazon_category',
            'body'  => [
                "_source" => [
                    "cate_id",
                    "cate_name",
                    "p_l1_id",
                    "p_l1_name",
                    "p_l2_id",
                    "p_l2_name",
                    "p_l3_id",
                    "p_l3_name",
                    "p_l4_id",
                    "p_l4_name",
                    "p_l5_id",
                    "p_l5_name",
                    "p_l6_id",
                    "p_l6_name",
                    "p_l7_id",
                    "p_l7_name",
                    "p_l8_id",
                    "p_l8_name",
                    "p_l9_id",
                    "p_l9_name",
                    "p_l10_id",
                    "p_l10_name",
                ]
            ]
        ];
        if($cateIdArr){
            $params['body']['query']['terms']['cate_id'] = $cateIdArr;
        }
        $response = $client->search($params);
        if(!$response['hits']['hits']){
            return false;
        }

        $pathArr = [];
        $fillArr = [
            "cate_id"  => 0,
            "cate_name"=> 1,
            "p_l1_id"  => 1,
            "p_l1_name"=> 1,
            "p_l2_id"  => 1,
            "p_l2_name"=> 1,
            "p_l3_id"  => 1,
            "p_l3_name"=> 1,
            "p_l4_id"  => 1,
            "p_l4_name"=> 1,
            "p_l5_id"  => 1,
            "p_l5_name"=> 1,
            "p_l6_id"  => 1,
            "p_l6_name"=> 1,
            "p_l7_id"  => 1,
            "p_l7_name"=> 1,
            "p_l8_id"  => 1,
            "p_l8_name"=> 1,
            "p_l9_id"  => 1,
            "p_l9_name"=> 1,
            "p_l10_id" => 1,
            "p_l10_name"=> 1,
        ];
        foreach ($response['hits']['hits'] as $v){
            $v['_source'] = $this->fill($v['_source'], $fillArr);
            //获取伪树状结构
            $pathArr[$v['_source']["cate_id"]] = $this->path_tree($v['_source'], intval($v['_source']["cate_id"]), $v['_source']["cate_name"]);
        }

        return $pathArr;
    }

    /**
     * 填满es整个数组缺失的数据(type类型：0数字(补数字0) 1字符串(补空格))
     * @param array $array 最终es数组的单项数组
     * @param array $fieldArr  字段数组
     * @return array
     */
    public function fill(array $array, array $fieldArr){
        foreach ($fieldArr as $field=>$type){
            if(!isset($array[$field])){
                switch ($type) {
                    case 0:
                        $array[$field] = 0;
                        break;
                    case 1:
                        $array[$field] = '';
                        break;
                    case 2:
                        $array[$field] = null;
                        break;
                    default:
                        $array[$field] = '';
                        break;
                }
            }
        }
        return $array;
    }
}
