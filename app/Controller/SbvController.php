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

use App\Service\SbvService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller(prefix="sbv")
 */
class SbvController extends BaseController
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
     * @var SbvService
     */
    protected $service;

    /**
     * @OA\Post(
     *     path="/sbv/submit_video",
     *     summary="sbv视频提交参数",
     *     tags={"sbv"},
     *     @OA\Parameter(
     *          name="authorization",
     *          required=true,
     *          in="header",
     *          description="令牌",
     *          @OA\Schema(type="string")
     *     ),
     *     @OA\RequestBody(
     *         @OA\MediaType(
     *             mediaType="application/form-data",
     *             @OA\Schema(required={"total_time","video_list"},
     *                  @OA\Property(
     *                      property="display_type",
     *                       type="integer",
     *                       description="展示类型：0（横板）"
     *                   ),
     *                   @OA\Property(
     *                        property="total_time",
     *                         type="integer",
     *                         description="总时长"
     *                    ),
     *                    @OA\Property(
     *                          property="pix_type",
     *                          type="integer",
     *                          description="像素类型：0（1280*720）1（1920*1080）2（3840*2160）"
     *                    ),
     *                     @OA\Property(
     *                          property="font_list",
     *                              type="array",
     *                              @OA\Items(
     *                                @OA\Property(
     *                                   property="time_start",
     *                                   type="integer",
     *                                   description="开始时间秒数"
     *                                ),
     *                                @OA\Property(
     *                                   property="time_end",
     *                                   type="integer",
     *                                   description="结束时间秒数"
     *                                ),
     *                                @OA\Property(
     *                                   property="content",
     *                                   type="string",
     *                                   description="字体内容"
     *                                ),
     *                                 @OA\Property(
     *                                   property="font_type",
     *                                   type="integer",
     *                                   description="字体类型：(附件)"
     *                                ),
     *                                 @OA\Property(
     *                                   property="font_size",
     *                                   type="integer",
     *                                   description="字体大小"
     *                                ),
     *                                 @OA\Property(
     *                                   property="font_bold",
     *                                   type="integer",
     *                                   description="字体粗体：0（标准）1（粗体）"
     *                                ),
     *                                 @OA\Property(
     *                                   property="font_color",
     *                                   type="string",
     *                                   description="字体颜色"
     *                                ),
     *                                 @OA\Property(
     *                                   property="x",
     *                                   type="number",
     *                                   description="x坐标"
     *                                ),
     *                                 @OA\Property(
     *                                   property="y",
     *                                   type="number",
     *                                   description="y坐标"
     *                                )
     *                             )
     *                      ),
     *                      @OA\Property(
     *                          property="video_list",
     *                              type="array",
     *                              @OA\Items(
     *                                @OA\Property(
     *                                   property="time_start",
     *                                   type="integer",
     *                                   description="开始时间秒数"
     *                                ),
     *                                @OA\Property(
     *                                   property="time_end",
     *                                   type="integer",
     *                                   description="结束时间秒数"
     *                                ),
     *                                @OA\Property(
     *                                   property="pic_url",
     *                                   type="string",
     *                                   description="图片url"
     *                                ),
     *                                 @OA\Property(
     *                                   property=" ",
     *                                   type="integer",
     *                                   description="动效类型：0：无特效（其他:附件）"
     *                                )
     *                             )
     *                      ),
     *                      @OA\Property(
     *                        property="music_url",
     *                         type="string",
     *                         description="音乐url"
     *                    )
     *             )
     *         )
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
     *                               property="id",
     *                               type="string",
     *                               description="获取文件id"
     *                           )
     *                       )
     *                  )
     *               )
     *          )
     * )
     *
     *
     * @RequestMapping(path="submit_video", methods="post")
     * @return mixed
     */
    public function submitVideo()
    {
        $request_data = $this->request->all();
        $header = $this->request->getHeaders();
        $data =$this->service->submitVideo($request_data, $header);
        if($data['code']== 0){
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'],$data['msg']);

    }

}
