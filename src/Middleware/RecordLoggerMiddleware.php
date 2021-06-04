<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Hyperf\Utils\Context;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use Captainbi\Hyperf\Util\Logger;

class RecordLoggerMiddleware implements MiddlewareInterface
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $context = Context::get(ServerRequestInterface::class);
        $userInfo = $context->getAttribute('userInfo');

        $admin_id = $userInfo['admin_id'];
        $user_id = $userInfo['user_id'];
        $access_url = $request->getUri()->getHost() . ":" . $request->getUri()->getPort() . $request->getUri()->getPath();
        $query_string = $request->getUri()->getQuery();
        $http_header = json_encode($request->getHeaders());
        $http_method = $request->getMethod();
        $http_params = json_encode($request->getQueryParams());

        Logger::access_log($admin_id, $user_id, $access_url, $query_string, $http_header, $http_method, $http_params);

        return $handler->handle($request);
    }
}