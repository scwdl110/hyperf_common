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


use App\Service\KingdeeService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller(prefix="info")
 */
class InfoController extends BaseController
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
     * @Inject()
     * @var KingdeeService
     */
    protected $service;

    /**
     * @RequestMapping(path="list", methods="get,post")
     */
    public function getInfo(){
        $request_data = $this->request->all();
        $data =$this->service->getKingDeeInfo($request_data);
        if($data['code']== 0){
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'],$data['msg']);
    }
}
