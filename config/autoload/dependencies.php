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
return [
    \Hyperf\ConfigApollo\Process\ConfigFetcherProcess::class => \App\Process\ApolloConfigFetcherProcess::class,
    \Hyperf\ConfigApollo\Listener\BootProcessListener::class => \App\Listener\ApolloBootProcessListener::class,
    \Hyperf\ConfigApollo\Listener\OnPipeMessageListener::class => \App\Listener\ApolloOnPipeMessageListener::class,
];
