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
use Hyperf\Server\Server;
use Hyperf\Server\Event;

return [
    'mode' => SWOOLE_PROCESS,
    'servers' => [
        [
            'name' => env('HYPERF_SERVER_NAME', 'http'),
            'type' => (int)env('HYPERF_SERVER_TYPE', Server::SERVER_HTTP),
            'host' => env('HYPERF_HOST', '0.0.0.0'),
            'port' => (int)env('HYPERF_PORT', 9501),
            'sock_type' => (int)env('HYPERF_SOCK_TYPE', SWOOLE_SOCK_TCP),
            'callbacks' => [
                Event::ON_REQUEST => [Hyperf\HttpServer\Server::class, 'onRequest'],
            ],
        ],
        [
            'name' => env('HYPERF_RPC_SERVER_NAME', 'jsonrpc-http'),
            'type' => (int)env('HYPERF_RPC_SERVER_TYPE', Server::SERVER_HTTP),
            'host' => env('HYPERF_HOST', '0.0.0.0'),
            'port' => (int)env('HYPERF_PORT', 9504),
            'sock_type' => (int)env('HYPERF_SOCK_TYPE', SWOOLE_SOCK_TCP),
            'callbacks' => [
                Event::ON_REQUEST => [\Hyperf\JsonRpc\HttpServer::class, 'onRequest'],
            ],
        ],
    ],
    'settings' => [
        'enable_coroutine' => true,
        'worker_num' => swoole_cpu_num(),
        // Task Worker 数量，根据您的服务器配置而配置适当的数量
        'task_worker_num' => swoole_cpu_num(),
        // 因为 `Task` 主要处理无法协程化的方法，所以这里推荐设为 `false`，避免协程下出现数据混淆的情况
        'task_enable_coroutine' => false,
        'pid_file' => BASE_PATH . '/runtime/hyperf.pid',
        'open_tcp_nodelay' => true,
        'max_coroutine' => 100000,
        'open_http2_protocol' => true,
        'max_request' => 100000,
        'socket_buffer_size' => 2 * 1024 * 1024,
        'buffer_output_size' => 2 * 1024 * 1024,
        'daemonize' => (int)env('HYPERF_DAEMONIZE', 1),
        'log_file' => (int)env('HYPERF_DAEMONIZE', 1) ? (BASE_PATH . '/runtime/logs/daemon.log') : '',
    ],
    'callbacks' => [
        Event::ON_WORKER_START => [Hyperf\Framework\Bootstrap\WorkerStartCallback::class, 'onWorkerStart'],
        Event::ON_PIPE_MESSAGE => [Hyperf\Framework\Bootstrap\PipeMessageCallback::class, 'onPipeMessage'],
        Event::ON_WORKER_EXIT => [Hyperf\Framework\Bootstrap\WorkerExitCallback::class, 'onWorkerExit'],
        // Task callbacks
        Event::ON_TASK => [Hyperf\Framework\Bootstrap\TaskCallback::class, 'onTask'],
        Event::ON_FINISH => [Hyperf\Framework\Bootstrap\FinishCallback::class, 'onFinish'],
        // Shutodwn callbacks
        // Event::ON_SHUTDOWN => [Hyperf\Framework\Bootstrap\ShutdownCallback::class, 'onShutdown'],
    ],
    'restart_console' => [
        // 重启服务时等待服务退出的超时时间，超时未退出视为重启失败。单位 秒，可选值 3-60
        'timeout' => (int)env('RESTART_CONSOLE_TIMEOUT', 60),
        // 重启脚本路径
        'script_path' => env('RESTART_CONSOLE_SCRIPT', BASE_PATH . '/bin/restart.php'),
        // 钉钉警报机器人 webhook access token
        'dingtalk_token' => env('RESTART_CONSOLE_DD_TOKEN', ''),
        // 钉钉警报机器人 secret，适用于 安全设置 勾选 “加签” 模式。“自定义关键词” 和 “IP地址(段)” 模式不需要配置
        'dingtalk_secret' => env('RESTART_CONSOLE_DD_SECRET', ''),
        /**
         * 钉钉警报机器人消息，不配置默认发送 text 消息，消息内容为
         * 重启微服务({APP_NAME})[{HOST_NAME}:{IP_ADDR}]失败，项目路径({APP_PATH})
         *
         * 需要发送其他类型消息的，配置成完整的 json 消息体即可，具体见
         * https://developers.dingtalk.com/document/app/custom-robot-access/title-72m-8ag-pqw#title-72m-8ag-pqw
         *
         * 可使用以下占位符
         * {APP_NAME}  app 名，见 .env APP_NAME
         * {APP_PATH}  即 BASE_PATH
         * {HOST_NAME} 服务器 hostname
         * {IP_ADDR}   服务器本地 IP 地址，多个 IP 的情况下见 \App\getLocalIP() 函数说明
         */
        'dingtalk_message' => env('RESTART_CONSOLE_DD_MESSAGE', ''),
    ],
];
