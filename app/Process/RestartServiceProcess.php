<?php

namespace App\Process;

use Swoole\Server;
use Swoole\Process;
use Swoole\Timer;
use Hyperf\Process\AbstractProcess;
use Hyperf\Process\ProcessCollector;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Process\Exception\ServerInvalidException;
use Hyperf\Process\Event\AfterProcessHandle;
use Hyperf\Process\Event\BeforeProcessHandle;

class RestartServiceProcess extends  AbstractProcess
{
    /** @var string */
    public const PROCESS_NAME = 'restart-service-console';

    /** @var string */
    public $name = self::PROCESS_NAME;

    /** @var int */
    public $nums = 1;

    /** @var bool */
    public $redirectStdinStdout = false;

    /** @var int */
    public $pipeType = \SOCK_DGRAM;

    /** @var bool */
    public $enableCoroutine = false;

    /** @var \Swoole\Server */
    private $server = null;

    /** @var bool */
    private $processed = false;

    /** {@inheritdoc} */
    public function bind($server): void
    {
        if (!($server instanceof Server)) {
            throw new ServerInvalidException(sprintf('Server %s is invalid.', get_class($server)));
        }

        $this->server = $server;
        $process = new Process(function (Process $process) {
            try {
                $this->event && $this->event->dispatch(new BeforeProcessHandle($this, 0));

                $this->process = $process;
                $this->handle();
            } catch (\Throwable $throwable) {
                $this->logThrowable($throwable);
            } finally {
                $this->event && $this->event->dispatch(new AfterProcessHandle($this, 0));
                if (isset($quit)) {
                    $quit->push(true);
                }
                Timer::clearAll();
                sleep($this->restartInterval);
            }
        }, false, \SOCK_DGRAM, false);

        // 使用 SystemV 消息队列
        // 第二个参数，swoole 只支持配置 IPC_NOWAIT 和 ipc_mode(这个参数只有2种选择，2 和 非2） 使用位操作赋值
        // ipc_mode 为 2 的话，相当于 msgrcv(pop) 的 msgp.mtype = 0 否则 msgp.mtype = pid + 1
        // 2者的区别在于 mtype = 0 时，获取队列的第一条消息，不为 0 时，获取 mtype 为指定值的第一条消息
        // 附例：
        // 0                                开启阻塞模式，接收 mtype = pid+1 的第一条消息(只要参数不是 2, 256, 258 即可)
        // 2                                开启阻塞模式，接收队列的第一条消息（\Swoole\Process::useQueue 的默认值）
        // \Swoole\Process::IPC_NOWAIT      开启非阻塞模式，接收 mtype = pid+1 的第一条消息
        // \Swoole\Process::IPC_NOWAIT | 2  开启非阻塞模式且接收队列的第一条消息
        $process->useQueue(ftok(__DIR__, 'c'), 0, 1024);
        $server->addProcess($process);

        ProcessCollector::add($this->name, $process);
    }

    /** {@inheritdoc} */
    public function handle(): void
    {
        $logger = $this->container->get(StdoutLoggerInterface::class);
        while (true) {
            if (false !== @$this->process->pop()) {
                if ($this->processed) {
                    break;
                }

                $this->processed = true;

                $config = $this->container->get(ConfigInterface::class);
                $script = trim(strval($config->get('server.restart_console.script_path', BASE_PATH . '/bin/restart.php')));
                if (!is_readable($script)) {
                    $logger->error("[{$this->name}] 未配置重启脚本路径 或 脚本不存在，请检查配置");
                    $this->processed = false;
                    continue;
                }
                $script = realpath($script);

                $accessToken = trim(strval($config->get('server.restart_console.dingtalk_token', '')));
                if (1 !== preg_match('/^[0-9a-f]{64}$/', $accessToken)) {
                    $logger->error("[{$this->name}] 未配置钉钉机器人 token 或 token 非预期，请检查配置");
                    $this->processed = false;
                    continue;
                }

                $dingTalkSecret = trim(strval($config->get('server.restart_console.dingtalk_secret', '')));
                if ('' !== $dingTalkSecret && 1 !== preg_match('/^SEC[0-9a-f]{64}$/', $dingTalkSecret)) {
                    $logger->error("[{$this->name}] 钉钉机器人 secret 不合法，请检查配置");
                    $this->processed = false;
                    continue;
                }

                $message = base64_encode(strtr(trim(strval($config->get(
                    'server.restart_console.dingtalk_message',
                    '重启微服务({APP_NAME})[{HOST_NAME}:{IP_ADDR}]失败，项目路径({APP_PATH})'
                ))), [
                    '{APP_PATH}' => BASE_PATH,
                    '{IP_ADDR}' => \App\getLocalIP(),
                    '{HOST_NAME}' => \gethostname(),
                    '{APP_NAME}' => $config->get('app_name'),
                ]));

                $hyperfPath = realpath($_SERVER['SCRIPT_FILENAME']);
                if (false === $hyperfPath) {
                    $hyperfPath = realpath(BASE_PATH . "/{$_SERVER['SCRIPT_FILENAME']}");
                    if (false === $hyperfPath) {
                        $logger->error("[$this->name] 获取 Hyperf 启动脚本路径失败");
                        $this->processed = false;
                        continue;
                    }
                }

                $timeout = min(60, max(3, (int)$config->get('server.restart_console.timeout', 60)));
                $pidFile = $this->server->setting['pid_file'];
                $pid = $this->server->getMasterPid();
                $port = (int)$config->get('server.servers.0.port');

                (new Process(function(Process $proc) use (
                    $pid,
                    $port,
                    $script,
                    $message,
                    $pidFile,
                    $timeout,
                    $hyperfPath,
                    $accessToken,
                    $dingTalkSecret
                ) {
                    $this->process->freeQueue();

                    $proc->exec(PHP_BINARY, [
                        $script,
                        "-c {$hyperfPath}",
                        "-f {$pidFile}",
                        "-p {$pid}",
                        "-a {$accessToken}",
                        "-P {$port}",
                        "-t {$timeout}",
                        "-m {$message}",
                        "-s {$dingTalkSecret}"
                    ]);
                }))->start();

                break;
            }

            sleep(1);
        }
    }
}
