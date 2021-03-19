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
    'scan' => [
        'paths' => [
            BASE_PATH . '/app',
        ],
        'ignore_annotations' => [
            'OA\Header',
            'mixin',
            'OA\Get',
            'OA\Post',
            'OA\RequestBody',
            'OA\Info',
            'OA\Items',
            'OA\MediaType',
            'OA\Parameter',
            'OA\Property',
            'OA\Response',
            'OA\Schema'
        ],
    ],
];
