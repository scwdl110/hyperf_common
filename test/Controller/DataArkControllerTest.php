<?php

namespace HyperfTest\Controller;

use Hyperf\Testing\Client;
use PHPUnit\Framework\TestCase;

class DataArkControllerTest extends TestCase
{
    protected $client;

    protected $sortTargets = [
        'amazon_fba_delivery_fee',
        'amazon_fba_monthly_storage_fee',
        'amazon_fba_return_processing_fee',
        'amazon_fee',
        'amazon_fee_rate',
        'amazon_long_term_storage_fee',
        'amazon_multi_channel_delivery_fee',
        'amazon_order_fee',
        'amazon_other_fee',
        'amazon_refund_deducted_commission',
        'amazon_refund_fee',
        'amazon_return_sale_commission',
        'amazon_return_shipping_fee',
        'amazon_sales_commission',
        'amazon_settlement_fee',
        'amazon_stock_fee',
        'channel_fbm_safe_t_claim_demage',
        'cost_profit_profit',
        'cost_profit_profit_rate',
        'cpc_acos',
        'cpc_ad_fee',
        'cpc_ad_settlement',
        'cpc_avg_click_cost',
        'cpc_click_conversion_rate',
        'cpc_click_number',
        'cpc_click_rate',
        'cpc_cost',
        'cpc_cost_rate',
        'cpc_direct_sales_quota',
        'cpc_direct_sales_volume',
        'cpc_direct_sales_volume_rate',
        'cpc_exposure',
        'cpc_indirect_sales_quota',
        'cpc_indirect_sales_volume',
        'cpc_indirect_sales_volume_rate',
        'cpc_order_number',
        'cpc_order_rate',
        'cpc_turnover',
        'cpc_turnover_rate',
        'evaluation_fee',
        'evaluation_fee_rate',
        'fba_logistics_head_course',
        'fba_refund_num',
        'fba_sales_quota',
        'fba_sales_volume',
        'fbm_logistics_head_course',
        'fbm_refund_num',
        'fbm_sales_quota',
        'goods_adjust_fee',
        'goods_buybox_rate',
        'goods_buyer_visit_rate',
        'goods_conversion_rate',
        'goods_min_rank',
        'goods_rank',
        'goods_views_number',
        'goods_views_rate',
        'goods_visitors',
        'operate_fee',
        'operate_fee_rate',
        'other_other_fee',
        'other_review_enrollment_fee',
        'other_vat_fee',
        'promote_coupon',
        'promote_discount',
        'promote_refund_discount',
        'promote_run_lightning_deal_fee',
        'promote_store_fee',
        'purchase_logistics_cost_rate',
        'purchase_logistics_logistics_cost',
        'purchase_logistics_purchase_cost',
        'sale_many_channel_sales_volume',
        'sale_order_number',
        'sale_refund',
        'sale_refund_rate',
        'sale_return_goods_number',
        'sale_sales_dollars',
        'sale_sales_quota',
        'sale_sales_volume',
        'shipping_charge',
        'tax',
        'ware_house_damage',
        'ware_house_lost',
    ];

    protected $targets = [
        'amazon_fba_delivery_fee',
        'amazon_fba_monthly_storage_fee',
        'amazon_fba_return_processing_fee',
        'amazon_fee',
        'amazon_fee_rate',
        'amazon_long_term_storage_fee',
        'amazon_multi_channel_delivery_fee',
        'amazon_order_fee',
        'amazon_other_fee',
        'amazon_refund_deducted_commission',
        'amazon_refund_fee',
        'amazon_return_sale_commission',
        'amazon_return_shipping_fee',
        'amazon_sales_commission',
        'amazon_settlement_fee',
        'amazon_stock_fee',
        'channel_fbm_safe_t_claim_demage',
        'cost_profit_profit',
        'cost_profit_profit_rate',
        'cost_profit_total_income',
        'cost_profit_total_pay',
        'cpc_acos',
        'cpc_ad_fee',
        'cpc_ad_settlement',
        'cpc_avg_click_cost',
        'cpc_click_conversion_rate',
        'cpc_click_number',
        'cpc_click_rate',
        'cpc_cost',
        'cpc_cost_rate',
        'cpc_direct_sales_quota',
        'cpc_direct_sales_volume',
        'cpc_direct_sales_volume_rate',
        'cpc_exposure',
        'cpc_indirect_sales_quota',
        'cpc_indirect_sales_volume',
        'cpc_indirect_sales_volume_rate',
        'cpc_order_number',
        'cpc_order_rate',
        'cpc_turnover',
        'cpc_turnover_rate',
        'evaluation_fee',
        'evaluation_fee_rate',
        'fba_goods_value',
        'fba_logistics_head_course',
        'fba_need_replenish',
        'fba_predundancy_number',
        'fba_recommended_replenishment',
        'fba_reserve_stock',
        'fba_sales_day',
        'fba_sales_quota',
        'fba_sales_stock',
        'fba_sales_volume',
        'fba_special_purpose',
        'fba_stock',
        'fbm_logistics_head_course',
        'fbm_sales_quota',
        'goods_adjust_fee',
        'goods_buybox_rate',
        'goods_buyer_visit_rate',
        'goods_conversion_rate',
        'goods_min_rank',
        'goods_rank',
        'goods_views_number',
        'goods_views_rate',
        'goods_visitors',
        'operate_fee',
        'operate_fee_rate',
        'other_goods_adjust',
        'other_other_fee',
        'other_remark_fee',
        'other_review_enrollment_fee',
        'other_vat_fee',
        'promote_coupon',
        'promote_discount',
        'promote_refund_discount',
        'promote_run_lightning_deal_fee',
        'promote_store_fee',
        'purchase_logistics_cost_rate',
        'purchase_logistics_logistics_cost',
        'purchase_logistics_purchase_cost',
        'sale_many_channel_sales_volume',
        'sale_order_number',
        'sale_refund',
        'sale_refund_rate',
        'sale_return_goods_number',
        'sale_sales_dollars',
        'sale_sales_quota',
        'sale_sales_volume',
        'shipping_charge',
        'tax',
        'ware_house_damage',
        'ware_house_lost',
    ];

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->client = make(Client::class);
    }

    protected function setUp(): void
    {
        putenv('APP_TEST_RUNNING=true');
    }

    public function testDatasApi()
    {
        foreach ($this->datasProvider() as list($uri, $postData)) {
            $result = $this->client->json($uri, $postData);

            $this->assertSame([
                'lists' => [],
                'count' => 0,
            ], $result);
        }
    }

    protected function datasProvider()
    {
        $enum = [
            'searchVal' => [null, 'abc'],
            'searchType' => [1],
            'page' => [1, 2],
            'sort' => [null] + $this->sortTargets,
            'order' => [null, 'desc', 'asc'],
            'channelIds' => [["288893","288894","288892"]],
            'countTip' => [0, 1, 2],
            'params' => '',
        ];

        $getUri = function() {
            foreach (['/dataark/unGoodsDatas', '/dataark/goodsDatas', '/dataark/operatorsDatas'] as $v) {
                yield $v;
            }
        };

        $generator = function($key) use ($enum) {
            foreach ($enum[$key] as $v) {
                yield $v;
            }
        };


        foreach ($getUri() as $uri) {
            foreach ($generator('searchVal') as $searchVal) {
                foreach ($generator('searchType') as $searchType) {
                    foreach ($generator('page') as $page) {
                        foreach ($generator('sort') as $sort) {
                            foreach ($generator('order') as $order) {
                                foreach ($generator('channelIds') as $channelIds) {
                                    foreach ($generator('countTip') as $countTip) {
                                        foreach ($this->getParams() as $params) {
                                            if ($this->checkParams($params)) {
                                                $data = array_filter(compact(array_keys($enum)), function ($x) {
                                                    return null !== $x;
                                                });

                                                yield [$uri, $data];
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    protected function getParams()
    {
        $enum = [
            'cost_count_type' => [1, 2],
            'count_dimension' => [
                // 'site_group',
                'site_id',
                'channel_id',
                'department',
                'admin_id',
                // 'group',
                // 'tags',
                // 'isku',
                // 'operators',
                // 'class1',
                'sku',
                'asin',
                'parent_asin',
            ],
            'count_periods' => [0, 1, 2, 3, 4, 5],
            'currency_code' => ['ORIGIN'], // ['ORIGIN', 'CNY'],
            'finance_datas_origin' => [1, 2],
            'is_count' => [0, 1],
            // 'is_check_department1' => [null], // [null, 0, 1],
            // 'is_check_department2' => [null], // [null, 0, 1],
            // 'is_check_department3' => [null], // [null, 0, 1],
            'is_distinct_channel' => [0, 1],
            'limit_num' => [0, 10],
            'refund_datas_origin' => [1, 2],
            'sale_datas_origin' => [1, 2],
            'search_end_time' => [0, 1609516799, 1610294399],
            'search_start_time' => [0, 1609430400, 1610208000],
            'show_type' => [1, 2],
            'sort_order' => ['', 'desc', 'asc'],
            'sort_target' => [''] + $this->sortTargets,
            'time_target' => [''] + $this->targets,
            'time_type' => [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 20, 21, 22, 77, 88, 99],
            'user_id' => [0, 11],
            'target' => '',
            'where_detail' => '',
        ];

        $where_detail = [];
        $target = $this->targets;

        $generator = function ($key) use ($enum) {
            foreach ($enum[$key] as $v) {
                yield $v;
            }
        };

        foreach ($generator('cost_count_type') as $cost_count_type) {
            foreach ($generator('count_dimension') as $count_dimension) {
                foreach ($generator('count_periods') as $count_periods) {
                    foreach ($generator('currency_code') as $currency_code) {
                        foreach ($generator('finance_datas_origin') as $finance_datas_origin) {
                            foreach ($generator('is_count') as $is_count) {
                                // foreach ($generator('is_check_department1') as $is_check_department1) {
                                    // foreach ($generator('is_check_department2') as $is_check_department2) {
                                        // foreach ($generator('is_check_department3') as $is_check_department3) {
                                            foreach ($generator('is_distinct_channel') as $is_distinct_channel) {
                                                foreach ($generator('limit_num') as $limit_num) {
                                                    foreach ($generator('refund_datas_origin') as $refund_datas_origin) {
                                                        foreach ($generator('sale_datas_origin') as $sale_datas_origin) {
                                                            foreach ($generator('search_end_time') as $search_end_time) {
                                                                foreach ($generator('search_start_time') as $search_start_time) {
                                                                    foreach ($generator('show_type') as $show_type) {
                                                                        foreach ($generator('sort_order') as $sort_order) {
                                                                            foreach ($generator('sort_target') as $sort_target) {
                                                                                foreach ($generator('time_target') as $time_target) {
                                                                                    foreach ($generator('time_type') as $time_type) {
                                                                                        foreach ($generator('user_id') as $user_id) {
                                                                                            yield array_filter(compact(array_keys($enum)), function ($x) {
                                                                                                return null !== $x;
                                                                                            });
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        // }
                                    // }
                                // }
                            }
                        }
                    }
                }
            }
        }
    }

    protected function getWhereDetail()
    {
        $enum = [
            'target' => [],
            'operators_id' => [null, 1, [1, 2]],
            'transport_mode' => [null, '', 'FBA', 'FMB'],
            'is_care' => [null, 0, 1],
            'is_new' => [null, 0, 1],
            'group_id' => [null, 1, [1, 2]],
            'tag_id' => [null, 1, [1, 2]],
        ];

        $generator = function ($key) use ($enum) {
            foreach ($enum[$key] as $v) {
                yield $v;
            }
        };

        foreach ($generator('operators_id') as $operators_id) {
            foreach ($generator('transport_mode') as $transport_mode) {
                foreach ($generator('is_care') as $is_care) {
                    foreach ($generator('is_new') as $is_new) {
                        foreach ($generator('group_id') as $group_id) {
                            foreach ($generator('tag_id') as $tag_id) {
                                // $target = $this->getTargets();
                                $target = [];
                                yield array_filter(compact(array_keys($enum)), function ($x) {
                                    return null !== $x;
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    protected function getTargets()
    {
        $enum = [
            'formula' => ['>', '<', '>=', '<=', '='],
            'key' => [],
            'value' => [100],
        ];
    }

    protected function checkParams(array $params): bool
    {
        if ($params['cost_count_type'] == 2
            && ($params['sale_datas_origin'] == 1 || $params['refund_datas_origin'] == 1 || $params['finance_datas_origin'] == 1)
        ) {
            return false;
        }

        if ($params['time_type'] == 99 && $params['search_start_time'] == 0 && $params['search_end_time'] == 0) {
            return false;
        }

        if ($params['currency_code'] == 'ORIGIN') {
            if (in_array($params['count_dimension'], ['group', 'tags', 'isku', 'operators', 'class1', 'site_group'])) {
                return false;
            }

            if ($params['is_distinct_channel'] == 0 && in_array($params['count_dimension'], ['sku' , 'asin' , 'parent_asin'])) {
                return false;
            }
        }

        if ($params['show_type'] == 1 && empty($params['time_target'])) {
            return false;
        }

        if (empty($params['target'])) {
            return false;
        }

        if ((in_array('sale_order_number', $params['target']) || in_array('sale_sales_dollars', $params['target']))
            && 2 == $params['sale_datas_origin']
        ) {
            return false;
        }

        if ($params['show_type'] === 1 && (in_array($params['time_target'], [
            'fba_sales_stock',
            'fba_sales_day',
            'fba_reserve_stock',
            'fba_recommended_replenishment',
            'fba_special_purpose',
            'fba_goods_value',
            'fba_stock',
            'fba_need_replenish',
            'fba_predundancy_number',
        ]))) {
            return false;
        }

        return true;
    }
}
