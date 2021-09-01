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


use App\Service\AccountingService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Annotation\Controller;
use Hyperf\HttpServer\Annotation\RequestMapping;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\HttpServer\Contract\ResponseInterface;

/**
 * @Controller()
 */
class AccountingController extends BaseController
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
     * @var AccountingService
     */
    protected $service;


    /**
     * @OA\Post(
     *     path="/accounting/fiancial",
     *     summary="金蝶第三方数据信息",
     *     tags={"accounting"},
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
     *             @OA\Property(
     *               property="date",
     *               type="string",
     *               description="日期"
     *             ),
     *             @OA\Property(
     *               property="offset",
     *               type="integer",
     *               description="偏移量"
     *             ),
     *             @OA\Property(
     *                property="limit",
     *                type="integer",
     *                description="每页条数"
     *             ),
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
     *                          property="data",
     *                          type="array",
     *                          @OA\Items(
     *                           @OA\Property(
     *                            property="channel_id",
     *                            type="number",
     *                            description="店铺id"
     *                           ),
     *                           @OA\Property(
     *                                property="commodity_sales",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="fba_sales_quota",
     *                                      type="number",
     *                                      description="FBA销售额"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fbm_sales_quota",
     *                                      type="number",
     *                                      description="FBM销售额"
     *                                  )
     *                                )
     *                           ),
     *                           @OA\Property(
     *                                property="promotion_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="promote_discount",
     *                                      type="number",
     *                                      description="Promote折扣"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="cpc_sb_cost",
     *                                      type="number",
     *                                      description="退款返还Promote折扣"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="coupon",
     *                                      type="number",
     *                                      description="coupon优惠券"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="run_lightning_deal_fee",
     *                                      type="number",
     *                                      description="RunLightningDealFee"
     *                                  ),
     *                                )
     *                               ),
     *                                @OA\Property(
     *                                property="order_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="platform_sales_commission",
     *                                      type="number",
     *                                      description="亚马逊销售佣金"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_generation_delivery_cost",
     *                                      type="number",
     *                                      description="FBA代发货费用"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="profit",
     *                                      type="number",
     *                                      description="多渠道配送费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="other_order_fee",
     *                                      type="number",
     *                                      description="其他订单费用"
     *                                  ),
     *                                )
     *                             ),
     *                               @OA\Property(
     *                                property="return_refund_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="return_and_return_commission",
     *                                      type="number",
     *                                      description="退款扣除佣金"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_refund_treatment_fee",
     *                                      type="number",
     *                                      description="FBA退货处理费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="return_and_return_sales_commission",
     *                                      type="number",
     *                                      description="返还亚马逊销售佣金"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="returnshipping",
     *                                      type="number",
     *                                      description="返还运费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="refund_variableclosingfee",
     *                                      type="number",
     *                                      description="可变结算费-退款"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="inventory_cost",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="fba_storage_fee",
     *                                      type="number",
     *                                      description="FBA月仓储费用"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_long_term_storage_fee",
     *                                      type="number",
     *                                      description="FBA长期仓储费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_disposal_fee",
     *                                      type="number",
     *                                      description="FBA处理费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_removal_fee",
     *                                      type="number",
     *                                      description="FBA移除费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="restocking_fee",
     *                                      type="number",
     *                                      description="FBA重新入仓费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_convenience_fee",
     *                                      type="number",
     *                                      description="库存配置服务费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_defect_fee",
     *                                      type="number",
     *                                      description="FBA入库缺陷费"
     *                                  ),
     *                                   @OA\Property(
     *                                      property="labeling_fee",
     *                                      type="number",
     *                                      description="贴标费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="polybagging_fee",
     *                                      type="number",
     *                                      description="包装费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_shipment_carton_level_info_fee",
     *                                      type="number",
     *                                      description="人工处理费用"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_transportation_fee",
     *                                      type="number",
     *                                      description="入仓运输费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_inbound_transportation_program_fee",
     *                                      type="number",
     *                                      description="FBA入境运输费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_overage_fee",
     *                                      type="number",
     *                                      description="库存仓储超量费"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="other_amazon_fee",
     *                                      type="number",
     *                                      description="其他亚马逊费用"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="other_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="reserved_field17",
     *                                      type="number",
     *                                      description="VAT"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="misc_adjustment",
     *                                      type="number",
     *                                      description="其他"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="review_enrollment_fee",
     *                                      type="number",
     *                                      description="早期评论者计划"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="cpc_cost",
     *                                      type="number",
     *                                      description="CPC花费"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="commodity_adjustment_fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="ware_house_lost",
     *                                      type="number",
     *                                      description="FBA仓丢失赔款"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="ware_house_damage",
     *                                      type="number",
     *                                      description="FBA仓损坏赔款"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="reversal_reimbursement",
     *                                      type="number",
     *                                      description="REVERSAL REIMBURSEMENT"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="return_postage_billing_postage",
     *                                      type="number",
     *                                      description="ReturnPostageBilling_postage"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="missing_from_inbound",
     *                                      type="number",
     *                                      description="入库丢失赔偿"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="missing_from_inbound_clawback",
     *                                      type="number",
     *                                      description="入库丢失赔偿(夺回)"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fba_per_unit_fulfillment_fee",
     *                                      type="number",
     *                                      description="费用盘点-重量和尺寸更改"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fee_adjustment",
     *                                      type="number",
     *                                      description="其他商品调整费用"
     *                                  ),
     *                                )
     *                             ),
     *                             @OA\Property(
     *                                property="fee",
     *                                type="array",
     *                                @OA\Items(
     *                                  @OA\Property(
     *                                      property="reserved_field16",
     *                                      type="number",
     *                                      description="运营费用"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="reserved_field10",
     *                                      type="number",
     *                                      description="测评费用"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="purchasing_cost",
     *                                      type="number",
     *                                      description="商品成本"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="logistics_head_course",
     *                                      type="number",
     *                                      description="FBA头程"
     *                                  ),
     *                                  @OA\Property(
     *                                      property="fbm",
     *                                      type="number",
     *                                      description="物流"
     *                                  ),
     *                                )
     *                             ),
     *                           )
     *                      )
     *             )
     *      )
     *   )
     * )
     * @RequestMapping(path="fiancial", methods="post")
     * @return mixed
     *
     */
    public function getFiancialProfitInfo()
    {
        $request_data = $this->request->all();
        $data = $this->service->getFiancialProfitInfo($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }

    /**
     * 回款单
     * @RequestMapping(path="moneycallback", methods="post")
     * @return mixed
     *
     */
    public function getMoneyCallBackInfo()
    {
        $request_data = $this->request->all();
        $data = $this->service->getMoneyCallBackInfo($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }

    /**
     * @OA\Get(
     *     path="/accounting/shop_list",
     *     summary="金蝶第三方店铺信息",
     *     tags={"accounting"},
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
     *                           @OA\Property(
     *                            property="total",
     *                            type="number",
     *                            description="总条数"
     *                           ),
     *                           @OA\Property(
     *                               property="list",
     *                               type="array",
     *                               @OA\Items(
     *                                @OA\Property(
     *                                  property="shop_id",
     *                                   type="integer",
     *                                   description="店铺ID"
     *                                ),
     *                               @OA\Property(
     *                                   property="shop_name",
     *                                  type="string",
     *                                  description="店铺名称"
     *                               ),
     *                               @OA\Property(
     *                                   property="modified_time",
     *                                  type="integer",
     *                                  description="修改时间"
     *                               ),
     *                            )
     *                           )
     *                       )
     *                     ),
     *                  )
     *               )
     *          )
     *
     * )
     *
     * @RequestMapping(path="shop_list", methods="get")
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
     * 第三方店铺信息
     * @RequestMapping(path="shop_info", methods="post")
     * @return mixed
     */
    public function postShopInfo()
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
     *     path="/accounting/exchange_rate_list",
     *     summary="汇率接口",
     *     tags={"accounting"},
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
     *                               type="integer",
     *                               description="货币ID"
     *                            ),
     *                            @OA\Property(
     *                               property="name",
     *                               type="string",
     *                               description="货币名称"
     *                            ),
     *                            @OA\Property(
     *                               property="code",
     *                               type="integer",
     *                               description="货币代码"
     *                            ),
     *                            @OA\Property(
     *                               property="symbol",
     *                               type="string",
     *                               description="货币符号"
     *                            ),
     *                           @OA\Property(
     *                               property="usd_exchang_rate",
     *                               type="number",
     *                               description="货币汇率"
     *                            ),
     *                         ),
     *                     ),
     *                  )
     *               )
     *          )
     * )
     *
     * @RequestMapping(path="exchange_rate_list", methods="get")
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

    /**
     * @OA\Post(
     *     path="/accounting/get_username",
     *     summary="用户名接口",
     *     tags={"get_username"},
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
     *             @OA\Property(
     *               property="client_id",
     *               type="string",
     *               description="调用方id"
     *             )
     *         )
     *        )
     *      ),
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
     *                               property="uuid",
     *                               type="string",
     *                               description="加密后第三方id"
     *                            ),
     *                            @OA\Property(
     *                               property="user_name",
     *                               type="string",
     *                               description="用户名"
     *                            ),
     *                         ),
     *                     ),
     *                  )
     *               )
     *          )
     * )
     *
     * @RequestMapping(path="get_username", methods="post")
     * @return mixed
     */
    public function getUsername()
    {
        $request_data = $this->request->all();
        $data = $this->service->getUserName($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }


    /**
     * @OA\Post(
     *     path="/accounting/bind",
     *     summary="绑定第三方信息接口",
     *     tags={"get_username"},
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
     *             @OA\Property(
     *               property="ext_info",
     *               type="string",
     *               description="第三方信息"
     *             ),
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
     *                     ),
     *                  )
     *               )
     *          )
     * )
     *
     * @RequestMapping(path="bind", methods="post")
     * @return mixed
     */
    public function bindUser()
    {
        $request_data = $this->request->all();
        $data = $this->service->bindUser($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }


    /**
     * 同步接口
     * @RequestMapping(path="syncOp", methods="post")
     * @return mixed
     */
    public function syncOp()
    {
        $request_data = $this->request->all();
        $data = $this->service->syncOp($request_data);
        if ($data['code'] == 0) {
            return Result::fail([], $data['msg']);
        }
        return Result::success($data['data'], $data['msg']);
    }

}
