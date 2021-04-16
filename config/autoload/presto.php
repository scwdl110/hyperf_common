<?php

declare(strict_types=1);

// 使用立即执行的匿名函数，避免 $cluster1 等变量污染了上层变量
return (function() {
    $cluster1 = [
        // presto 服务器地址，含端口，可以加上 http:// 或 https:// 协议，不加协议默认为 http
        'server' => env('PRESTO_SERVER_001', 'http://127.0.0.1:8080'),
        'user' => env('PRESTO_USER_001', 'user'),
        'catalog' => env('PRESTO_CATALOG_001', 'system'),
        'schema' => env('PRESTO_SCHEMA_001', 'runtime'),
        // 请求 presto 遇到 503 服务器错误时，最多重试几次，上限为 20 次
        'retries' => env('PRESTO_RETRIES_001', 3),
    ];

    $cluster2 = [
        'server' => env('PRESTO_SERVER_002', 'http://127.0.0.1:8080'),
        'user' => env('PRESTO_USER_002', 'user'),
        'catalog' => env('PRESTO_CATALOG_002', 'system'),
        'schema' => env('PRESTO_SCHEMA_002', 'runtime'),
        'retries' => env('PRESTO_RETRIES_002', 3),
    ];

    $cluster3 = [
        'server' => env('PRESTO_SERVER_003', 'http://127.0.0.1:8080'),
        'user' => env('PRESTO_USER_003', 'user'),
        'catalog' => env('PRESTO_CATALOG_003', 'system'),
        'schema' => env('PRESTO_SCHEMA_003', 'runtime'),
        'retries' => env('PRESTO_RETRIES_003', 3),
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
