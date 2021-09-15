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

];