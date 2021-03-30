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
     * @OA\Post(
     *     path="/info/list",
     *     summary="金蝶第三方接口",
     *     tags={"info"},
     *     @OA\Parameter(
     *          name="date",
     *          required=true,
     *          description="日期",
     *          @OA\Schema(type="string")
     *     ),
     *     @OA\Parameter(
     *          name="authorization",
     *          required=true,
     *          in="header",
     *          description="令牌",
     *          @OA\Schema(type="string")
     *     ),
     *
     *
     * @RequestMapping(path="list", methods="post")
     * @return mixed
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
