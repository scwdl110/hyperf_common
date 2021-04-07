<?php

declare(strict_types=1);

return [
    // presto 服务器地址，含端口，可以加上 http:// 或 https:// 协议，不加协议默认为 http
    'server' => env('PRESTO_SERVER', 'http://127.0.0.1:8080'),
    'user' => env('PRESTO_USER', 'user'),
    'catalog' => env('PRESTO_CATALOG', 'system'),
    'schema' => env('PRESTO_SCHEMA', 'runtime'),
    // 请求 presto 遇到 503 服务器错误时，最多重试几次，上限为 20 次
    'retries' => env('PRESTO_RETRIES', 3),
];
