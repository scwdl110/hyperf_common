<?php
declare (strict_types=1);

namespace Captainbi\Hyperf\Exception\Handle;

use Captainbi\Hyperf\Exception\BusinessException;
use Throwable;

/**
 * HTTP异常处理
 */
class BusinessExceptionHandle extends BaseExceptionHandler
{
    public function isValid(Throwable $throwable): bool
    {
        return $throwable instanceof BusinessException;
    }

}
