<?php
declare (strict_types=1);

namespace Captainbi\Hyperf\Exception\Handle;

use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Result;
use Hyperf\ExceptionHandler\ExceptionHandler;
use Hyperf\HttpMessage\Stream\SwooleStream;
use Psr\Http\Message\ResponseInterface;
use Throwable;

/**
 * HTTP异常处理
 */
class BusinessExceptionHandle extends ExceptionHandler
{
    public function handle(Throwable $throwable, ResponseInterface $response)
    {
        // 判断被捕获到的异常是希望被捕获的异常
        if ($throwable instanceof BusinessException) {
            // 格式化输出
            $data = Result::fail([], $throwable->getMessage(), $throwable->getCode());
            // 阻止异常冒泡
            $this->stopPropagation();
            return $response->withStatus(500)->withBody(new SwooleStream($data));
        }

//        // 交给下一个异常处理器
        return $response;
    }

    /**
     * 判断该异常处理器是否要对该异常进行处理
     */
    public function isValid(Throwable $throwable): bool
    {
        return true;
    }

}
