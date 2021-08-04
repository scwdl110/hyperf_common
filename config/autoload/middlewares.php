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
    'http' => [
        \Captainbi\Hyperf\Middleware\CorsMiddleware::class,
        \Captainbi\Hyperf\Middleware\OAuth2Middleware::class,
        \Hyperf\Validation\Middleware\ValidationMiddleware::class,
        //\Captainbi\Hyperf\Middleware\RecordLoggerMiddleware::class,
    ],
];