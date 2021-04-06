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
     *     summary="金蝶第三方数据信息",
     *     tags={"info"},
     *     @OA\Parameter(
     *          name="authorization",
     *          in="header",
     *          required=true,
     *          description="令牌"
     *     ),
     *     @OA\RequestBody(
     *         @OA\MediaType(
     *             mediaType="application/x-www-form-urlencoded",
     *             @OA\Schema(required={"country_id"},
     *                 @OA\Property(
     *                     property="date",
     *                     type="string",
     *                     description="日期"
     *                 ),
     *         )
     *      )
     *),
     *      @OA\Response(response=200,
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
     *                                property="commodity_sales",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="fba_sales_quota",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fbm_sales_quota",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  )
     *                                )
     *                           ),
     *                           @OA\Property(
     *                                property="promotion_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="promote_discount",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="cpc_sb_cost",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="coupon",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="run_lightning_deal_fee",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                )
     *                               ),
     *                                @OA\Property(
     *                                property="order_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="platform_sales_commission",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_generation_delivery_cost",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="profit",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                )
     *                             ),
     *                               @OA\Property(
     *                                property="return_refund_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="return_and_return_commission",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_refund_treatment_fee",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="return_and_return_sales_commission",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="returnshipping",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="refund_variableclosingfee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="inventory_cost",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="fba_storage_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_long_term_storage_fee",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_disposal_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_removal_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="restocking_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_convenience_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_defect_fee",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="polybagging_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_shipment_carton_level_info_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_transportation_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_transportation_program_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_overage_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="other_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="reserved_field17",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="misc_adjustment",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="review_enrollment_fee",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="commodity_adjustment_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="ware_house_lost",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="ware_house_damage",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="reversal_reimbursement",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="return_postage_billing_postage",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="missing_from_inbound",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="missing_from_inbound_clawback",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_per_unit_fulfillment_fee",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="reserved_field16",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="reserved_field10",
     *                                      type="string",
     *                                      description="fbm销售额'"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="purchasing_cost",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="logistics_head_course",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fbm",
     *                                      type="string",
     *                                      description="fba销售额"
     *                                  ),
     *                                )
     *                             ),
     *                      )
     *             )
     *      )
     *   )
     * )
     * @RequestMapping(path="list", methods="post")
     * @return mixed
     *
     */
    public function getInfo()
    {
        $request_data = $this->request->all();
        $data = $this->service->getKingDeeInfo($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }

    /**
     * @OA\Get(
     *     path="/info/shop_list",
     *     summary="金蝶第三方店铺信息",
     *     tags={"info"},
     *     @OA\Parameter(
     *          name="authorization",
     *          required=true,
     *          in="header",
     *          description="令牌",
     *          @OA\Schema(type="string")
     *     ),
     *      @OA\Response(response=200,
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
     *                     @OA\Property(
     *                          property="data",
     *                          type="array",
     *                          @OA\Items(
     *                            @OA\Property(
     *                               property="shop_id",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                            @OA\Property(
     *                               property="shop_name",
     *                               type="string",
     *                               description="fbm销售额'"
     *                            ),
     *                            @OA\Property(
     *                               property="modified_time",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                         )
     *                     ),
     *                  )
     *               )
     *          )
     *
     * )
     *
     * @RequestMapping(path="shop_list", methods="post")
     * @return mixed
     */
    public function getShopInfo()
    {
        $request_data = $this->request->all();
        $data = $this->service->getShopList($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }

    /**
     * @OA\Get(
     *     path="/info/exchange_rate_list",
     *     summary="接口信息表",
     *     tags={"info"},
     *     @OA\Parameter(
     *          name="authorization",
     *          required=true,
     *          in="header",
     *          description="令牌",
     *          @OA\Schema(type="string")
     *     ),
     *      @OA\Response(response=200,
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
     *                       @OA\Property(
     *                          property="data",
     *                          type="array",
     *                          @OA\Items(
     *                            @OA\Property(
     *                               property="id",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                            @OA\Property(
     *                               property="name",
     *                               type="string",
     *                               description="fbm销售额'"
     *                            ),
     *                            @OA\Property(
     *                               property="code",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                            @OA\Property(
     *                               property="symbol",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                           @OA\Property(
     *                               property="usd_exchang_rate",
     *                               type="string",
     *                               description="fba销售额"
     *                            ),
     *                         )
     *                     ),
     *                  )
     *               )
     *          )
     * )
     *
     * @RequestMapping(path="exchange_rate_list", methods="post")
     * @return mixed
     */
    public function getExchangeRateList()
    {
        $request_data = $this->request->all();
        $data = $this->service->getExchangeRateList($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }
}
