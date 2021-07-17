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
        $access_control = $request->getHeader('Access-Control-Request-Headers');

        $response = Context::get(ResponseInterface::class);
        $response = $response->withHeader('Access-Control-Allow-Origin', $origin)
            ->withHeader('Access-Control-Allow-Credentials', 'true')
            // Headers 可以根据实际情况进行改写。
//            ->withHeader('Access-Control-Allow-Headers', 'uid,token,Keep-Alive,User-Agent,Cache-Control,Content-Type,HYPERF_SESSION_ID_Token,X-Token,lang')
            ->withHeader('Content-Type', 'application/json')
            //该字段可选，用来指定本次预检请求的有效期，在此期间，不用发出另一条预检请求,20天。
            ->withHeader('Access-Control-Max-Age', 1728000)
            //根据浏览器提供的允许访问空值头设置
            ->withHeader('Access-Control-Allow-Headers', $access_control);

        Context::set(ResponseInterface::class, $response);

        if ($request->getMethod() == 'OPTIONS' || strpos($request->url(),"/ping") !== false) {
            return $response->withBody(new SwooleStream('success'));
        }

        return $handler->handle($request);
    }
}