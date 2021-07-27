<?php
//客户端连接consul配置
$registry = [
    'protocol' => env('REGISTER_PROTOCOL1', 'consul'),
    'address' => env('REGISTER_ADDRESS1', 'http://127.0.0.1:8500')
];
return [
    'consumers' => [
        [
            'name' => "OtherService",
            'registry' => $registry,
        ],
    ],
];