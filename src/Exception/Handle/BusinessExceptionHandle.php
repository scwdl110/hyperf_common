<?php
declare (strict_types=1);

namespace Captainbi\Hyperf\Exception\Handle;

use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Result;
use think\exception\Handle;
use think\exception\HttpException;
use think\exception\ValidateException;
use think\Response;
use Throwable;

/**
 * HTTP异常
 */
class BusinessExceptionHandle extends Handle
{
    public function render($request, Throwable $e): Response
    {
        // 参数验证错误
        if ($e instanceof BusinessException) {
            return Result::fail($e->getCode(),$e->getMessage());
        }
        // 其他错误交给系统处理
        return parent::render($request, $e);
    }

}
