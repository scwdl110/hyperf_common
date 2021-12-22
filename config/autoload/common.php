<?php
return [
    "currency_list" => array(
        "rmb_exchang_rate" => array("id" => 1, "site_id" => 10, "name" => "人民币", "code" => "CNY", "symbol" => "￥"),
        "usd_exchang_rate" => array("id" => 2, "site_id" => 1, "name" => "美元", "code" => "USD", "symbol" => "$",),
        "gbp_exchang_rate" => array("id" => 3, "site_id" => 9, "name" => "英镑", "code" => "GBP", "symbol" => "£"),
        "eur_exchang_rate" => array("id" => 4, "site_id" => array(4, 5, 6, 8, 16), "name" => "欧元", "code" => "EUR", "symbol" => "€"),
        "tr_exchang_rate" => array("id" => 5, "site_id" => 14, "name" => "新土耳其里拉", "code" => "TRY", "symbol" => "₺"),
        "au_exchang_rate" => array("id" => 6, "site_id" => 12, "name" => "澳币", "code" => "AUD", "symbol" => "A$"),
        "jpy_exchang_rate" => array("id" => 7, "site_id" => 11, "name" => "日元", "code" => "JPY", "symbol" => "¥"),
        "ae_exchang_rate" => array("id" => 8, "site_id" => 15, "name" => "阿联酋迪拉姆", "code" => "AED", "symbol" => "AED"),
        "br_exchang_rate" => array("id" => 9, "site_id" => 13, "name" => "巴西雷亚尔", "code" => "BRL", "symbol" => "R$"),
        "in_exchang_rate" => array("id" => 10, "site_id" => 7, "name" => "印度卢比", "code" => "INR", "symbol" => "₹"),
        "cad_exchang_rate" => array("id" => 11, "site_id" => 2, "name" => "加元", "code" => "CAD", "symbol" => "C$"),
        "mxn_exchang_rate" => array("id" => 12, "site_id" => 3, "name" => "比索币", "code" => "MXN", "symbol" => "Mex$"),
        "nl_exchang_rate" => array("id" => 13, "site_id" => 16, "name" => "荷兰盾", "code" => "NLG", "symbol" => "fl"),
        "sa_exchang_rate" => array("id" => 14, "site_id" => 17, "name" => "里亚尔", "code" => "SAR", "symbol" => "SAR"),
        "sg_exchang_rate" => array("id" => 15, "site_id" => 18, "name" => "新加坡元", "code" => "SGD", "symbol" => "S$")
    ),
    /*五大站点对应的国家：1：北美，2：：欧洲，3：印度，4：日本，5：澳洲 6:新加坡*/
    "amzon_site_country1" => array(
        array('id' => 1, "name" => '美国', "country_key" => "g_usa", "currency_code" => "USD", "currency_symbol" => "$", "code" => "US", "site_group_id" => 1),
        array('id' => 2, "name" => '加拿大', "country_key" => "g_canada", "currency_code" => "CAD", "currency_symbol" => "C$", "code" => "CA", "site_group_id" => 1),
        array('id' => 3, "name" => '墨西哥', "country_key" => "g_mexico", "currency_code" => "MXN", "currency_symbol" => "Mex$", "code" => "MX", "site_group_id" => 1),
        array('id' => 4, "name" => '德国', "country_key" => "g_germany", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "DE", "site_group_id" => 2),
        array('id' => 5, "name" => '西班牙', "country_key" => "g_spain", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "ES", "site_group_id" => 2),
        array('id' => 6, "name" => '法国', "country_key" => "g_france", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "FR", "site_group_id" => 2),
        array('id' => 8, "name" => '意大利', "country_key" => "g_italy", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "IT", "site_group_id" => 2),
        array('id' => 9, "name" => '英国', "country_key" => "g_england", "currency_code" => "GBP", "currency_symbol" => "£", "code" => "UK", "site_group_id" => 2),
        array('id' => 7, "name" => '印度', "country_key" => "g_india", "currency_code" => "INR", "currency_symbol" => "₹", "code" => "IN", "site_group_id" => 2),
        array('id' => 11, "name" => '日本', "country_key" => "g_japan", "currency_code" => "JPY", "currency_symbol" => "¥", "code" => "JP", "site_group_id" => 4),
        array('id' => 12, "name" => '澳大利亚', "country_key" => "g_australia", "currency_code" => "AUD", "currency_symbol" => "A$", "code" => "AU", "site_group_id" => 5),
        array('id' => 13, "name" => '巴西', "country_key" => "g_brazil", "currency_code" => "BRL", "currency_symbol" => "R$", "code" => "BR", "site_group_id" => 1),
        array('id' => 14, "name" => '土耳其', "country_key" => "g_turkey", "currency_code" => "TRY", "currency_symbol" => "₺", "code" => "TR", "site_group_id" => 2),
        array('id' => 15, "name" => '阿联酋迪拉姆', "country_key" => "g_arabic", "currency_code" => "AED", "currency_symbol" => "AED", "code" => "AE", "site_group_id" => 2),
        array('id' => 16, "name" => '荷兰盾', "country_key" => "g_netherlands", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "NL", "site_group_id" => 2),
        array('id' => 17, "name" => '沙特阿拉伯', "country_key" => "g_arabia", "currency_code" => "SAR", "currency_symbol" => "SAR", "code" => "SA", "site_group_id" => 2),
        array('id' => 18, "name" => '沙特阿拉伯', "country_key" => "g_singapore", "currency_code" => "SGD", "currency_symbol" => "S$", "code" => "SG", "site_group_id" => 6)
    ),
    "big_selling_users" => env("BIG_SELLING_USERS","20567,323435,21,221336,281463,342133,98271,210568,313691,94952") ,
    //店铺维度 FBA 指标 count_type 1- SUM 聚合 2-取最大最小值范围 3-取max 4-公式计算
    "channel_fba_fields_arr" => array(
        //总库存量
        "fba_total_stock"=>array("count_type"=>1 , "mysql_field"=>"available_stock"),
        //可售
        "fba_sales_stock"=>array("count_type"=>1, "mysql_field"=>"total_fulfillable_quantity"),
        //在途
        "fba_receiving_on_the_way"=>array("count_type"=>1, "mysql_field"=>"inbound_shipped_quantity"),
        //接收中
        "fba_receiving"=>array("count_type"=>1, "mysql_field"=>"inbound_receiving_quantity"),
        //预留库存
        "fba_reserve_stock"=>array("count_type"=>1, "mysql_field"=>"reserved_quantity"),
        //不可售
        "fba_not_sales_stock"=>array("count_type"=>1, "mysql_field"=>"unsellable_quantity"),
        //处理中
        "fba_working"=>array("count_type"=>1, "mysql_field"=>"inbound_working_quantity"),
        //是否当前补货
        "fba_is_buhuo"=>array("count_type"=>3, "mysql_field"=>"is_buhuo"),
        //需补货SKU数
        "fba_need_replenish"=>array("count_type"=>1, "mysql_field"=>"replenishment_sku_nums"),
        //需补货成本
        "fba_need_replenish_cost"=>array("count_type"=>1, "mysql_field"=>"buhuo_cost"),
        //可售天数 单独处理
        "fba_sales_day"=>array("count_type"=>2, "mysql_field"=>"available_days_start,available_days_end"),
        //建议补货数量
        "fba_recommended_replenishment"=>array("count_type"=>1, "mysql_field"=>"replenishment_quantity"),
        //建议补货时间 单独处理
        "fba_suggested_replenishment_time"=>array("count_type"=>2, "mysql_field"=>"suggested_replenishment_time_start,suggested_replenishment_time_end"),
        //冗余sku 数
        "fba_predundancy_number"=>array("count_type"=>1, "mysql_field"=>"redundancy_sku"),
        //库龄 ≤90
        "fba_3_month_age"=>array("count_type"=>1, "mysql_field"=>"_3_month_age"),
        //库龄 90-180
        "fba_3_6_month_age"=>array("count_type"=>1, "mysql_field"=>"_3_6_month_age"),
        //库龄 180-270
        "fba_6_9_month_age"=>array("count_type"=>1, "mysql_field"=>"_6_9_month_age"),
        //库龄 270-365
        "fba_9_12_month_age"=>array("count_type"=>1, "mysql_field"=>"_9_12_month_age"),
        //库龄 >365
        "fba_12_month_age"=>array("count_type"=>1, "mysql_field"=>"_12_month_age"),
        //预计总LTSF
        "fba_total_ltsf_num"=>array("count_type"=>1, "mysql_field"=>"total_ltsf_num"),
        //≤365天LTSF
        "fba_ltsf_6_12"=>array("count_type"=>1, "mysql_field"=>"ltsf_6_12"),
        //>365天LTSF
        "fba_ltsf_12"=>array("count_type"=>1, "mysql_field"=>"ltsf_12"),
        //预估总货值
        "fba_estimate_total"=>array("count_type"=>1, "mysql_field"=>"estimate_total"),
        //在库总成本
        "fba_goods_value"=>array("count_type"=>1, "mysql_field"=>"yjzhz"),
        //周转次数单独处理
        "fba_turnover_times"=>array("count_type"=>4, "mysql_field"=>"(CASE WHEN SUM(_30_day_sale) > 0 THEN SUM(available_stock)/SUM(_30_day_sale) ELSE 0 END )"),
        //上个月商品动销率
        "fba_marketing_rate"=>array("count_type"=>3, "mysql_field"=>"marketing_rate"),
        //已发货
        "fba_shipped_num"=>array("count_type"=>1, "mysql_field"=>"shipped_num"),
        //已收到
        "fba_received_num"=>array("count_type"=>1, "mysql_field"=>"received_num")
    ) ,
    //商品维度 FBA 指标 count_type 1- SUM 聚合 2-取最大最小值范围 3-取max 4-公式计算 5-取AVG
    "goods_fba_fields_arr" => array(
        //可售
        "fba_sales_stock"=>array("count_type"=>1, "mysql_field"=>"total_fulfillable_quantity"),
        //在途
        "fba_receiving_on_the_way"=>array("count_type"=>1, "mysql_field"=>"inbound_shipped_quantity"),
        //接收中
        "fba_receiving"=>array("count_type"=>1, "mysql_field"=>"inbound_receiving_quantity"),
        //接收中
        "fba_receiving"=>array("count_type"=>1, "mysql_field"=>"inbound_receiving_quantity"),
        //FBA专用
        "fba_special_purpose"=>array("count_type"=>1, "mysql_field"=>"available_stock"),
        //预留库存
        "fba_reserve_stock"=>array("count_type"=>1, "mysql_field"=>"reserved_quantity"),
        //不可售
        "fba_not_sales_stock"=>array("count_type"=>1, "mysql_field"=>"unsellable_quantity"),
        //处理中
        "fba_working"=>array("count_type"=>1, "mysql_field"=>"inbound_working_quantity"),
        //建议移除数量
        "fba_sellable_removal_quantity"=>array("count_type"=>1, "mysql_field"=>"sellable_removal_quantity"),
        //昨日销量
        "fba_1_day_sale"=>array("count_type"=>1, "mysql_field"=>"_1_day_sale"),
        //3日销量
        "fba_3_day_sale"=>array("count_type"=>1, "mysql_field"=>"_3_day_sale"),
        //7日销量
        "fba_7_day_sale"=>array("count_type"=>1, "mysql_field"=>"_7_day_sale"),
        //14日销量
        "fba_14_day_sale"=>array("count_type"=>1, "mysql_field"=>"_14_day_sale"),
        //30日销量
        "fba_30_day_sale"=>array("count_type"=>1, "mysql_field"=>"_30_day_sale"),
        //90日销量
        "fba_90_day_sale"=>array("count_type"=>1, "mysql_field"=>"_90_day_sale"),
        //日均销售
        "fba_day_sale"=>array("count_type"=>5, "mysql_field"=>"day_sale"),
        //3日均销售
        "fba_3_avg_sale"=>array("count_type"=>5, "mysql_field"=>"3_avg_sale"),
        //7日均销售
        "fba_7_avg_sale"=>array("count_type"=>5, "mysql_field"=>"7_avg_sale"),
        //14日均销售
        "fba_14_avg_sale"=>array("count_type"=>5, "mysql_field"=>"14_avg_sale"),
        //30日均销售
        "fba_30_avg_sale"=>array("count_type"=>5, "mysql_field"=>"30_avg_sale"),
        //60日均销售
        "fba_60_avg_sale"=>array("count_type"=>5, "mysql_field"=>"60_avg_sale"),
        //90日均销售
        "fba_90_avg_sale"=>array("count_type"=>5, "mysql_field"=>"90_avg_sale"),
        //昨日退货量
        "fba_1_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_1_day_sale_return"),
        //3日退货量
        "fba_3_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_3_day_sale_return"),
        //7日退货量
        "fba_7_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_7_day_sale_return"),
        //14日退货量
        "fba_14_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_14_day_sale_return"),
        //30日退货量
        "fba_30_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_30_day_sale_return"),
        //60日退货量
        "fba_60_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_60_day_sale_return"),
        //90日退货量
        "fba_90_day_sale_return"=>array("count_type"=>1, "mysql_field"=>"_90_day_sale_return"),
        //日均退货量
        "fba_day_sale_return"=>array("count_type"=>5, "mysql_field"=>"day_sale_return"),
        //3日均退货量
        "fba_3_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"3_avg_sale_return"),
        //7日均退货量
        "fba_7_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"7_avg_sale_return"),
        //14日均退货量
        "fba_14_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"14_avg_sale_return"),
        //30日均退货量
        "fba_30_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"30_avg_sale_return"),
        //60日均退货量
        "fba_60_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"60_avg_sale_return"),
        //90日均退货量
        "fba_90_avg_sale_return"=>array("count_type"=>5, "mysql_field"=>"90_avg_sale_return"),
        //3日退货率
        "fba_ravg3"=>array("count_type"=>5, "mysql_field"=>"ravg3"),
        //7日退货率
        "fba_ravg7"=>array("count_type"=>5, "mysql_field"=>"ravg7"),
        //14日退货率
        "fba_ravg14"=>array("count_type"=>5, "mysql_field"=>"ravg14"),
        //30日退货率
        "fba_ravg30"=>array("count_type"=>5, "mysql_field"=>"ravg30"),
        //60日退货率
        "fba_ravg60"=>array("count_type"=>5, "mysql_field"=>"ravg60"),
        //90日退货率
        "fba_ravg90"=>array("count_type"=>5, "mysql_field"=>"ravg90"),
        //是否当前补货
        "fba_is_buhuo"=>array("count_type"=>3, "mysql_field"=>"is_buhuo"),
        //可售天数
        "fba_sales_day"=>array("count_type"=>1, "mysql_field"=>"available_days"),
        //到货天数
        "fba_arrival_days"=>array("count_type"=>1, "mysql_field"=>"arrival_days"),
        //备货天数
        "fba_days_of_preparation"=>array("count_type"=>1, "mysql_field"=>"days_of_preparation"),
        //建议补货数量
        "fba_replenishment_quantity"=>array("count_type"=>1, "mysql_field"=>"replenishment_quantity"),
        //建议补货时间
        "fba_suggested_replenishment_time"=>array("count_type"=>3, "mysql_field"=>"suggested_replenishment_time"),
        //库龄 ≤90
        "fba_3_month_age"=>array("count_type"=>1, "mysql_field"=>"_3_month_age"),
        //库龄 90-180
        "fba_3_6_month_age"=>array("count_type"=>1, "mysql_field"=>"_3_6_month_age"),
        //库龄 180-270
        "fba_6_9_month_age"=>array("count_type"=>1, "mysql_field"=>"_6_9_month_age"),
        //库龄 270-365
        "fba_9_12_month_age"=>array("count_type"=>1, "mysql_field"=>"_9_12_month_age"),
        //库龄 >365
        "fba_12_month_age"=>array("count_type"=>1, "mysql_field"=>"_12_month_age"),
        //预计总LTSF
        "fba_ltsf"=>array("count_type"=>1, "mysql_field"=>"ltsf"),
        //≤365天LTSF
        "fba_ltsf"=>array("count_type"=>1, "mysql_field"=>"ltsf_6_12"),
        //仓储费
        "fba_ccf"=>array("count_type"=>1, "mysql_field"=>"ccf"),
        //仓储费/件
        "fba_ccf_every"=>array("count_type"=>1, "mysql_field"=>"ccf_every"),
        //规格/件
        "fba_volume_new"=>array("count_type"=>3, "mysql_field"=>"volume_new"),
        //高库龄数
        "fba_ltsf_num"=>array("count_type"=>1, "mysql_field"=>"ltsf_num"),
        //高库龄数仓储费
        "fba_glccf"=>array("count_type"=>1, "mysql_field"=>"glccf"),
        //在库总成本
        "fba_reserved_fc_transfers_cost"=>array("count_type"=>1, "mysql_field"=>"reserved_fc_transfers_cost"),
        //高库龄成本
        "fba_glhz"=>array("count_type"=>1, "mysql_field"=>"glhz"),
    ),
    'erp_isku_fields_arr' => [
        //良品量
        'ark_erp_good_num' => ['mysql_field' => 'warehouse_isku.good_num'],
        //次品量
        'ark_erp_bad_num' => ['mysql_field' => 'warehouse_isku.bad_num'],
        //锁仓量
        'ark_erp_lock_num' => ['mysql_field' => 'warehouse_isku.lock_num + warehouse_isku.lock_num_work_order'],
        //采购在途
        'ark_erp_purchasing_num' => ['mysql_field' => 'warehouse_isku.purchasing_num'],
        //调拨在途
        'ark_erp_send_num' => ['mysql_field' => 'warehouse_isku.send_num'],
        //ERP在库总数量
        'ark_erp_total_num' => ['mysql_field' => 'warehouse_isku.total_num'],
        //ERP在库总成本
        'ark_erp_goods_cost_total' => ['mysql_field' => 'warehouse_isku.goods_cost * warehouse_isku.total_num', 'format_type' => 4],
    ],
    'erp_report_fields_arr' => [
        //期末在途总数
        'erp_period_end_purchasing_send_num' => ['mysql_field' => 'warehouse_report.purchasing_send_num'],
        //期末采购在途
        'erp_period_end_purchasing_num' => ['mysql_field' => 'warehouse_report.purchasing_num'],
        //期末调拨在途
        'erp_period_end_send_num' => ['mysql_field' => 'warehouse_report.send_num'],
        //期初数量
        'erp_period_start_num_begin' => ['mysql_field' => 'warehouse_report.num_begin'],
        //期初单位成本
        'erp_period_start_goods_cost_begin' => ['mysql_field' => 'warehouse_report.goods_cost_begin', 'format_type' => 4],
        //期初总成本
        'erp_period_start_goods_cost_total_begin' => ['mysql_field' => 'warehouse_report.goods_cost_total_begin', 'format_type' => 4],
        //本期入库数量
        'erp_period_current_in_num' => ['mysql_field' => 'warehouse_report.in_num'],
        //本期入库总成本
        'erp_period_current_in_cost' => ['mysql_field' => 'warehouse_report.in_cost', 'format_type' => 4],
        //本期出库数量
        'erp_period_current_out_num' => ['mysql_field' => 'warehouse_report.out_num'],
        //本期出库总成本
        'erp_period_current_out_cost' => ['mysql_field' => 'warehouse_report.out_cost', 'format_type' => 4],
        //本期成本调整
        'erp_period_current_supplement_cost' => ['mysql_field' => 'warehouse_report.supplement_cost', 'format_type' => 4],
        //期末结存库存
        'erp_period_end_num_end' => ['mysql_field' => 'warehouse_report.num_end'],
        //期末结存单位成本
        'erp_period_end_goods_cost_end' => ['mysql_field' => 'warehouse_report.goods_cost_end', 'format_type' => 4],
        //期末结存在库总成本
        'erp_period_end_goods_cost_total_end' => ['mysql_field' => 'warehouse_report.goods_cost_total_end', 'format_type' => 4],
        //本期库存周转率
        'erp_period_current_stock_rate' => ['mysql_field' => 'warehouse_report.stock_rate'],
    ],

];
