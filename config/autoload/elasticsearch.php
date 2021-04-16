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

// 使用立即执行的匿名函数，避免 $cluster1 等变量污染了上层变量
return (function() {
    // hosts 为数组，每个数组元素为一个 host
    // 支持如下格式的 host
    // 127.0.0.1:9200, http://127.0.0.1:9200, https://127.0.0.1:443,
    // 具体说明见
    // v.a. https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/host-config.html
    // 需要账号密码的，可以使用如下格式的 host
    // http://user:password@127.0.0.1:9200
    // 具体见
    // v.a. https://www.php.net/manual/en/function.parse-url.php
    // 只要 parse_url 函数支持的 url，都可以被解析
    // 注意！！如果没显示设置端口，默认使用 9200 端口
    $cluster1 = [
        'hosts' => explode(',', env('ES_HOSTS_001', '127.0.0.1:9200')),
        'retries' => env('ES_RETRIES_001', 0),
        # sql_path 为使用 sql 语句查询时的 path，和标准 es 查询时的 path 不一样
        # 比如，aws es 的 sql_path 为 /_opendistro/_sql，而标准查询在默认情况下 path 为 /
        'sql_path' => env('ES_SQL_PATH_001', ''),
    ];

    $cluster2 = [
        'hosts' => explode(',', env('ES_HOSTS_002', '127.0.0.1:9200')),
        'retries' => env('ES_RETRIES_002', 0),
        'sql_path' => env('ES_SQL_PATH_002', ''),
    ];

    $cluster3 = [
        'hosts' => explode(',', env('ES_HOSTS_003', '127.0.0.1:9200')),
        'retries' => env('ES_RETRIES_003', 0),
        'sql_path' => env('ES_SQL_PATH_003', ''),
    ];

    return [
        '001' => $cluster1,
        '002' => $cluster1,
        '003' => $cluster1,
        '004' => $cluster1,
        '005' => $cluster1,

        '006' => $cluster2,
        '007' => $cluster2,
        '008' => $cluster2,
        '009' => $cluster2,
        '010' => $cluster2,

        '011' => $cluster3,
        '012' => $cluster3,
        '013' => $cluster3,
        '014' => $cluster3,
        '015' => $cluster3,
        '016' => $cluster3,
    ];
})();
