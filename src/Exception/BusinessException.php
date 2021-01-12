<?php
declare (strict_types=1);

namespace Captainbi\Hyperf\Exception;

use Throwable;

/**
 * HTTP异常
 */
class BusinessException extends \RuntimeException
{
    public function __construct($code = 10001, $message = '', Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
