<?php

declare(strict_types=1);

namespace App\Middleware;

use Hyperf\Server\Exception\ServerException;
use Hyperf\Utils\Context;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface as HttpResponse;
use Hyperf\Utils\Coroutine;
use Psr\Container\ContainerInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use Hyperf\Di\Annotation\Inject;
use App\Lib\Common;

class UserMiddleware implements MiddlewareInterface
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var RequestInterface
     */
    protected $request;

    /**
     * @var HttpResponse
     */
    protected $response;

    /**
     * @Inject()
     * @var Common
     */
    protected $Common;

    public function __construct(ContainerInterface $container, HttpResponse $response, RequestInterface $request)
    {
        $this->container = $container;
        $this->response = $response;
        $this->request = $request;
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        //获取用户dbhost 和 codeno
        $request = $request->withAttribute('dbhost', '');
        $request = $request->withAttribute('codeno', '');
        $db_code = $this->Common->getDbCode();
        if (!empty($db_code)) {
            $request = $request->withAttribute('dbhost', $db_code['dbhost']);
            $request = $request->withAttribute('codeno', $db_code['codeno']);
        } else {
            throw new ServerException("未找到用户数据库编号");
        }
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }
}
