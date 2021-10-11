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

namespace Captainbi\Hyperf\Process\AsyncQueue;

use Hyperf\AsyncQueue\Process\ConsumerProcess;
use Hyperf\Process\Annotation\Process;

///**
// * @Process(name="async-queue")
// */
class AsyncQueueConsumer extends ConsumerProcess
{
    /**
     * $message = make(Hyperf\AsyncQueue\Message::class, [Hyperf\AsyncQueue\JobInterface $job]);
     * 任务执行流转流程主要包括以下几个队列:
     * 队列名          备注
     * waiting     等待消费的队列     $this->redis->lPush($this->channel->getWaiting(), $message)   数据类型 list
     * reserved    正在消费的队列     $this->redis->zadd($this->channel->getReserved(), time() + $this->handleTimeout, $message);  数据类型 zset 有序集合
     * delayed     延迟消费的队列     $this->redis->zAdd($this->channel->getDelayed(), time() + $delay, $message)                  数据类型 zset 有序集合
     * failed      消费失败的队列     $this->redis->lPush($this->channel->getFailed(), $message)   数据类型 list
     * timeout     消费超时的队列 (虽然超时，但可能执行成功)  $this->redis->lPush($this->channel->getTimeout(), $message)
     */

}
