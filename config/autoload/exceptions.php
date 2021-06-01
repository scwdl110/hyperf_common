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
    'handler' => [
        'http' => [
//            App\Exception\Handler\AppExceptionHandler::class,
//            \Hyperf\ExceptionHandler\Handler\WhoopsExceptionHandler::class,
//            Hyperf\HttpServer\Exception\Handler\HttpExceptionHandler::class,
//            App\Exception\Handler\AppExceptionHandler::class,
            \Captainbi\Hyperf\Exception\Handle\BusinessExceptionHandle::class,
            \Captainbi\Hyperf\Exception\Handle\BiHttpExceptionHandler::class,
            \Hyperf\Validation\ValidationExceptionHandler::class,
            \Captainbi\Hyperf\Exception\Handle\BiAppExceptionHandler::class,
        ],
    ],
];
