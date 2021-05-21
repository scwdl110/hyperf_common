<?php

declare(strict_types=1);

namespace App\Process;

use Hyperf\ConfigApollo\Process\ConfigFetcherProcess;
use Hyperf\ConfigApollo\ClientInterface;
use Hyperf\ConfigApollo\PipeMessage;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Process\ProcessCollector;
use Psr\Container\ContainerInterface;

class ApolloConfigFetcherProcess extends ConfigFetcherProcess
{
    /**
     * @var Server
     */
    private $server;

    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * @var ConfigInterface
     */
    private $config;

    /**
     * @var StdoutLoggerInterface
     */
    private $logger;

    public function __construct(ContainerInterface $container)
    {
        parent::__construct($container);
        $this->client = $container->get(ClientInterface::class);
        $this->config = $container->get(ConfigInterface::class);
        $this->logger = $container->get(StdoutLoggerInterface::class);
    }

    public function bind($server): void
    {
        $this->server = $server;
        parent::bind($server);
    }

    public function handle(): void
    {
        $workerCount = $this->server->setting['worker_num'] + $this->server->setting['task_worker_num'] - 1;
        $ipcCallback = function ($configs, $namespace) use ($workerCount) {
            if (isset($configs['configurations'], $configs['releaseKey'])) {
                $configs['namespace'] = $namespace;
                $pipeMessage = new PipeMessage($configs);
                if ($pipeMessage->needToRestartService()) {
                    foreach (ProcessCollector::get(RestartServiceProcess::PROCESS_NAME) as $proc) {
                        if ($proc->msgQueueKey) {
                            $proc->push('1');
                            return ;
                        }
                    }
                }

                for ($workerId = 0; $workerId <= $workerCount; ++$workerId) {
                    $this->server->sendMessage($pipeMessage, $workerId);
                }

                $string = serialize($pipeMessage);

                $processes = ProcessCollector::all();
                /** @var \Swoole\Process $process */
                foreach ($processes as $process) {
                    $result = $process->exportSocket()->send($string, 10);
                    if ($result === false) {
                        $this->logger->error('Configuration synchronization failed. Please restart the server.');
                    }
                }
            }
        };
        while (true) {
            $callbacks = [];
            $namespaces = $this->config->get('apollo.namespaces', []);
            foreach ($namespaces as $namespace) {
                if (is_string($namespace)) {
                    $callbacks[$namespace] = $ipcCallback;
                }
            }
            $this->client->pull($namespaces, $callbacks);
            sleep($this->config->get('apollo.interval', 5));
        }
    }
}
