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

use Hyperf\HttpMessage\Stream\SwooleStream;
use Hyperf\HttpServer\Exception\Handler\HttpExceptionHandler;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class BiHttpExceptionHandler extends HttpExceptionHandler
{

    /**
     * Handle the exception, and return the specified result.
     * @param HttpException $throwable
     */
    public function handle(Throwable $throwable, ResponseInterface $response)
    {
        $this->logger->debug($this->formatter->format($throwable));

        $this->stopPropagation();
        // 格式化输出
        $data = Result::fail([], $throwable->getCode(),$throwable->getMessage());

        return $response->withStatus($throwable->getStatusCode())->withBody(new SwooleStream($data));
    }
}
