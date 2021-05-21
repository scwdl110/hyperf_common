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
namespace App\Controller;

use App\Process\RestartServiceProcess;

use Swoole\Coroutine\System;
use Hyperf\Process\ProcessCollector;
use Hyperf\HttpServer\Annotation\AutoController;

/**
 * @AutoController()
 */
class ConsoleController extends AbstractController
{
    protected static $updating = false;

    public function restart()
    {
        return $this->sendRestartSignl() ? 'restarting...' : 'failed';
    }

    public function update()
    {
        if (static::$updating) {
            return 'updating...';
        }

        static::$updating = true;

        $gitUrl = config('misc.git_url', '');
        $command = 'cd ' . BASE_PATH . '; git pull';

        if ($gitUrl) {
            $command .= " {$gitUrl} `git rev-parse --abbrev-ref HEAD`";
        }

        $result = System::exec($command);
        static::$updating = false;

        if (0 === $result['code']) {
            if (strpos($result['output'], ' is up to date.')) {
                return 'current code is up to date';
            } else {
                return $this->sendRestartSignl() ? 'update success, restarting...' : 'update success, restart failed';
            }
        } else {
            return "failed\n{$result['output']}";
        }
    }

    private function sendRestartSignl(): bool
    {
        foreach (ProcessCollector::get(RestartServiceProcess::PROCESS_NAME) as $process) {
            if ($process->msgQueueKey) {
                $process->push('1');
                return true;
            }
        }

        return false;
    }
}
