<?php
declare (strict_types=1);

namespace Captainbi\Hyperf\Exception;

use Hyperf\Server\Exception\ServerException;
use Throwable;

/**
 * HTTP异常
 */
class OpenException extends ServerException
{
    public function __construct($code = 10001, $message = 'business fail', Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
