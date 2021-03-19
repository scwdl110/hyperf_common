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

use App\Service\AuthService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller(prefix="auth")
 */
class AuthController extends BaseController
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
     * @var AuthService
     */
    protected $service;

    /**
     * @OA\Get(
     *     path="/auth/jwt_token",
     *     summary="获取jwt_token",
     *     tags={"auth"},
     *     @OA\Parameter(
     *          name="custom_id",
     *          in="query",
     *          description="客户获取token凭证",
     *          @OA\Schema(type="string")
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
     *                               property="access_token",
     *                               type="string",
     *                               description="access_token"
     *                           )
     *                       )
     *                  )
     *               )
     *          )
     * )
     *
     *
     * @RequestMapping(path="jwt_token", methods="get")
     * @return mixed
     */
    public function jwtToken()
    {
        $request_data = $this->request->all();
        $data =$this->service->jwtToken($request_data);
        if($data['code']== 0){
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'],$data['msg']);

    }


}
