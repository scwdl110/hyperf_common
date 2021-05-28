<?php

declare(strict_types=1);

/**
 * 本文件存放杂项配置
 * 对于代码中使用 env() 函数获取的配置都应该放在此配置中
 * 然后通过 $config->get('misc.ENV_KEY') 来获取
 * 这样才能通过 apollo 动态配置参数
 */
return [
    'presto_schema_ods' => env('PRESTO_SCHEMA_ODS', 'ods'),
    'presto_schema_dws' => env('PRESTO_SCHEMA_DWS', 'dws'),
    'presto_schema_dim' => env('PRESTO_SCHEMA_DIM', 'dim'),
    'presto_defautl_cache' => env('PRESTO_DEFAULT_CACHE', false),
    'elasticsearch_default_cache' => env('ES_DEFAULT_CACHE', false),
    // 记录方舟请求
    'dataark_log_req' => env('APP_DATAARK_LOG', false),
    // git url，如果密码没有记住，需要使用形如 http://username:password@1.2.3.4/git_repo_path 的形式
    'git_url' => env('APP_GIT_URL', ''),
];
