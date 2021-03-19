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

use App\Service\CommonService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller(prefix="common")
 */
class CommonController extends BaseController
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
     * @var CommonService
     */
    protected $service;

    /**
     * @OA\Get(
     *     path="/common/get_result",
     *     summary="通过异步id获取结果(返回结果data里面的数据不是一定的，根据不同的类型id返回不同的值)",
     *     tags={"common"},
     *     @OA\Parameter(
     *          name="authorization",
     *          required=true,
     *          in="header",
     *          description="令牌",
     *          @OA\Schema(type="string")
     *     ),
     *     @OA\Parameter(
     *          name="id",
     *          required=true,
     *          in="query",
     *          description="异步id",
     *          @OA\Schema(type="string"),
     *     ),
     *     @OA\Response(response=200,
     *          description="获取成功",
     *             @OA\MediaType(mediaType="application/json",
     *                  @OA\Schema(
     *                       @OA\Property(
     *                          property="code",
     *                          type="integer",
     *                          description="200",
     *                          example="200"
     *                      ),
     *                      @OA\Property(
     *                          property="msg",
     *                          type="string",
     *                          description="msg"
     *                      ),
     *                      @OA\Property(
     *                           property="data",
     *                           type="object",
     *                           @OA\Property(
     *                               property="url",
     *                               type="string",
     *                               description="获取视频"
     *                           )
     *                       )
     *                  )
     *               )
     *          )
     * )
     *
     *
     * @RequestMapping(path="get_result", methods="get")
     * @return mixed
     */
    public function getResult()
    {
        $request_data = $this->request->all();
        $header = $this->request->getHeaders();
        $data =$this->service->getResult($request_data, $header);
        if($data['code']== 0){
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'],$data['msg']);

    }
}
