<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Hyperf\HttpMessage\Stream\SwooleStream;
use Hyperf\Utils\Context;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

class CorsMiddleware implements MiddlewareInterface
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $origin = $request->getHeader('origin') ?? '*';

        $response = Context::get(ResponseInterface::class);
        $response = $response->withHeader('Access-Control-Allow-Origin', $origin)
            ->withHeader('Access-Control-Allow-Credentials', 'true')
            // Headers 可以根据实际情况进行改写。
            ->withHeader('Access-Control-Allow-Headers', 'uid,token,Keep-Alive,User-Agent,Cache-Control,Content-Type,HYPERF_SESSION_ID_Token')
            ->withHeader('Content-Type', 'application/json');

        Context::set(ResponseInterface::class, $response);

        if ($request->getMethod() == 'OPTIONS' || strpos($request->url(),"/ping") !== false) {
            return $response->withBody(new SwooleStream('success'));
        }

        return $handler->handle($request);
    }
}