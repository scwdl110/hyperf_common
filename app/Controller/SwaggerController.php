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
namespace App\Controller;

use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller()
 * @OA\Info(title="财务",version="1.0")
 */
class SwaggerController extends BaseController
{
    /**
     * @Inject()
     * @var RequestInterface
     */
    protected $request;

    /**
     * @Inject()
     * @var ResponseInterface
     */
    protected $response;

    /**
     * @var
     */
    protected $service;

    /**
     * @RequestMapping(path="/swagger", methods="get,post")
     */
    public function swagger(){
//        $openapi = \OpenApi\scan(BASE_PATH."/app/Controller");
//        return $this->response->withHeader('Content-Type', 'application/x-yaml')->withContent($openapi->toJson());
        $openapi = file_get_contents(BASE_PATH."/api_document.json");
        return $this->response->withHeader('Content-Type', 'application/x-yaml')->withContent($openapi);
    }
}
