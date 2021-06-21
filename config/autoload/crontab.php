<?php
return [
    // 是否开启定时任务
    'enable' => env('HYPERF_CRONTAB_ENABLE', true),
    //sbv并发数
    'sbv_concurrency_num' => swoole_cpu_num(),
    //sbv超时处理并发数
    'sbv_overtime_concurrency_num' => swoole_cpu_num()/2,
    //sbv超时时间半小时
    'sbv_overtime_time' => 1800,

];