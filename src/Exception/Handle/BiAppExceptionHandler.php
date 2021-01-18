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
namespace Captainbi\Hyperf\Exception\Handle;

use Throwable;

class BiAppExceptionHandler extends BaseExceptionHandler
{
    /**
     * @var $code
     */
    protected $code = 500;

    /**
     * @var $message
     */
    protected $message = 'Internal Server Error.';

    /**
     * @var $data
     */
    protected $data = [];

    public function isValid(Throwable $throwable): bool
    {
        return true;
    }
}