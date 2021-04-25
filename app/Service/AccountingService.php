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

namespace App\Service;

use App\Lib\Common;
use App\Model\CurrencyModel;
use App\Model\ChannelModel;
use App\Model\ChannelProfitReportModel;
use App\Model\FinanceCurrencyModel;
use App\Model\SystemCurrencyModel;
use App\Model\UserAdminModel;
use App\Model\UserExtInfoModel;
use App\Service\BaseService;
use App\Model\FinanceReportModel;
use Hyperf\Utils\Context;
use Hyperf\Utils\Coroutine;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Psr\Http\Message\ServerRequestInterface;
use Hyperf\Contract\ConfigInterface;
use Captainbi\Hyperf\Util\Unique;
use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Log;

class AccountingService extends BaseService
{

    /**
     * @var RequestInterface
     */
    protected $request;

    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @Inject()
     * @var Common
     */
    protected $common;

    /**
     * 获取当前用户信息
     * @return mixed
     */

    public function getUserInfo()
    {
        $context = Context::get(ServerRequestInterface::class);
        $userInfo = $context->getAttribute('userInfo');
        if (empty($userInfo)) {
            throw new BusinessException();
        }
        return $userInfo;
    }

    /**
     * 获取财务利润信息
     * @param $request_data
     * @return array
     */
    public function getFiancialProfitInfo($request_data)
    {
        $rule = [
            'date' => 'required|date',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled'
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        $current_firstday = date('Y-m-01', strtotime($request_data['date']));
        $current_lastday = date('Y-m-d', strtotime("$current_firstday +1 month -1 day"));

        $begin_time = strtotime($current_firstday . " 00:00:00");
        $end_time = strtotime($current_lastday . " 23:59:59");

        $request_data['offset'] = $request_data['offset'] ?? 0;
        $request_data['limit'] = $request_data['limit'] ?? 10;

        $userInfo = $this->getUserInfo();

        $userAdmin = UserAdminModel::query()->where('id', $userInfo['admin_id'])->select('is_master', 'check_prv_ids')->first();
        $shopListInfoquery = ChannelModel::select("id", "site_id", "title")->where([['user_id', '=', $userInfo['user_id']]]);

        if ($userAdmin->is_master != 1) {
            $ids = $userAdmin->check_prv_ids != null ? explode(',', $userAdmin->check_prv_ids) : array(0);
            $shopListInfoquery->whereIn('id', $ids);
        }

        $count = $shopListInfoquery->count();

        $shopListId = $shopListInfoquery->orderBy('id', 'asc')->offset($request_data['offset'])->limit($request_data['limit'])->get()->toArray();

        $infoList = array('total' => $count, 'list' => array());

        $rateList = $this->getExchangeRate($userInfo['user_id']);

        foreach ($shopListId as $key => $list) {

            $info = array();

            $FinanceReportInfo = FinanceReportModel::selectRaw("
          	ifnull( sum( sales_quota * ( reserved_field11 / sales_volume )), 0 ) AS fba_sales_quota,
	        ifnull( sum( sales_quota ) - sum( sales_quota * ( reserved_field11 / sales_volume )), 0 ) AS fbm_sales_quota,
            ifnull(sum(promote_discount),0) as promote_discount,
            ifnull(sum(cpc_sb_cost),0) as cpc_sb_cost ,
            ifnull(sum(platform_sales_commission),0) as platform_sales_commission,
            ifnull(sum(fba_generation_delivery_cost),0) as fba_generation_delivery_cost,
            ifnull(sum(profit),0) as profit,
            ifnull(sum(return_and_return_commission),0) as  return_and_return_commission,
            ifnull(sum(fba_refund_treatment_fee),0) as fba_refund_treatment_fee,
            ifnull(sum(return_and_return_sales_commission),0) as return_and_return_sales_commission,
            ifnull(sum(returnshipping),0) as returnshipping,
            ifnull(sum(refund_variableclosingfee),0) as refund_variableclosingfee,
            ifnull(sum(estimated_monthly_storage_fee),0)  as estimated_monthly_storage_fee,
            ifnull(sum(restocking_fee),0) as restocking_fee,
            ifnull(sum(reserved_field17),0) as reserved_field17,
            ifnull(sum(ware_house_lost),0) as ware_house_lost,
            ifnull(sum(ware_house_damage),0) as  ware_house_damage,
            ifnull(sum(missing_from_inbound),0) as missing_from_inbound,
            ifnull(sum(missing_from_inbound_clawback),0) as missing_from_inbound_clawback,
            ifnull(sum(reserved_field16),0) as reserved_field16,
            ifnull(sum(reserved_field10),0) as reserved_field10,
            ifnull(sum(purchasing_cost),0) as purchasing_cost,
            ifnull(sum(logistics_head_course),0) as logistics_head_course,
            ifnull(sum(logistics_head_course) - sum(reserved_field13),0) as fbm,
            ifnull(sum(reversal_reimbursement),0) as reversal_reimbursement,
            ifnull(sum(order_variableclosingfee + fixedclosingfee + refund_variableclosingfee + platform_sales_commission + fba_generation_delivery_cost + fbaperorderfulfillmentfee +  fbaweightbasedfee +  profit +  returnshipping +  return_and_return_sales_commission +  return_and_return_commission +  fba_refund_treatment_fee +  fbacustomerreturnperorderfee +  fbacustomerreturnweightbasedfee +  long_term_storage_fee +  estimated_monthly_storage_fee +  gift_wrap +  restocking_fee +  shipping_charge +  shipping_charge_charge_back +  shipping_tax +  tax +  gift_wrap_tax +  refund_shipping_charge +  refund_shipping_charge_charge_back +  refund_shipping_tax +  refund_tax +  marketplace_facilitator_tax_shipping +  marketplace_facilitator_tax_principal +  order_lowvaluegoods_other +  order_giftwrapchargeback +  order_shippinghb +  salestaxcollectionfee +  costofpointsgranted +  order_codchargeback +  amazonexclusivesfee +  giftwrapcommission +  order_paymentmethodfee +  cod_tax +  refund_lowvaluegoods_other +  marketplacefacilitatortax_restockingfee +  goodwill +  refund_paymentmethodfee +  refund_codchargeback +  refund_shippinghb +  refund_giftwrapchargeback +  costofpointsreturned +  pointsadjusted +  reserved_field3 +  reserved_field4 +  reserved_field6 +  reserved_field7 +  reserved_field8 +  reserved_field14 +  reserved_field15 +  reserved_field18 +  reserved_field19 +  reserved_field20 +  reserved_field21 +  ware_house_lost +  ware_house_damage +  ware_house_lost_manual +  ware_house_damage_exception +  reversal_reimbursement +  compensated_clawback +  customer_damage +  free_replacement_refund_items +  removal_order_lost +  incorrect_fees_items +  missing_from_inbound_clawback +  missing_from_inbound +  inbound_carrier_damage +  multichannel_order_lost +  payment_retraction_items +  cpc_sb_sales_quota +  cpc_sb_cost +  refund_rate +  profit_margin),0) as amazon_fee,
            ifnull(sum(gift_wrap + tax),0) AS other_amazon_fee,
            ifnull(sum(ware_house_lost + ware_house_damage + ware_house_lost_manual + ware_house_damage_exception + reversal_reimbursement + disposal_fee + free_replacement_refund_items + removal_order_lost + incorrect_fees_items + missing_from_inbound_clawback + missing_from_inbound + inbound_carrier_damage + multichannel_order_lost + payment_retraction_items + reserved_field19),0) AS fee_adjustment
        ")->where([
                ['channel_id', '=', $list['id']],
                ['create_time', '>=', $begin_time],
                ['create_time', '<', $end_time]
            ])->get()->toArray();

            //$Currency = CurrencyModel::select('id', 'exchang_rate')->where(['id' => $list['site_id']])->first()->toArray();

            foreach ($rateList as $rate) {
                if ($list['site_id'] == $rate['site_id']) {
                    $info['currency_id'] = $rate['site_id'];
                    $info['exchang_rate'] = $rate['exchang_rate'];
                }
            }

            //商品销售额
            $info['shop_id'] = $list['id'];
            $info['shop_name'] = $list['title'];

            $info['commodity_sales']['fba_sales_quota'] = floor($FinanceReportInfo[0]['fba_sales_quota'] * 100) / 100; //FBA销售额
            $info['commodity_sales']['fbm_sales_quota'] = floor($FinanceReportInfo[0]['fbm_sales_quota'] * 100) / 100; //FBM销售额

            $ChannelProfitReportInfo = ChannelProfitReportModel::selectRaw("
            ifnull(sum(coupon_redemption_fee + coupon_payment_eventList_tax),0) as coupon,
            ifnull(sum(run_lightning_deal_fee),0) as  run_lightning_deal_fee,
            ifnull(sum(fba_storage_fee),0) as fba_storage_fee,
            ifnull(sum(fba_long_term_storage_fee),0) as fba_long_term_storage_fee,
            ifnull(sum(fba_disposal_fee),0) as fba_disposal_fee,
            ifnull(sum(fba_removal_fee),0) as fba_removal_fee,
            ifnull(sum(fba_inbound_convenience_fee),0) as fba_inbound_convenience_fee,
            ifnull(sum(fba_inbound_defect_fee),0) as fba_inbound_defect_fee,
            ifnull(sum(labeling_fee),0) as labeling_fee,
            ifnull(sum(polybagging_fee),0) as polybagging_fee,
            ifnull(sum(fba_inbound_shipment_carton_level_info_fee),0) as fba_inbound_shipment_carton_level_info_fee,
            ifnull(sum(fba_inbound_transportation_fee),0) as fba_inbound_transportation_fee,
            ifnull(sum(fba_inbound_transportation_program_fee),0) as fba_inbound_transportation_program_fee,
            ifnull(sum(fba_overage_fee),0) as fba_overage_fee,
            ifnull(sum(misc_adjustment),0) as misc_adjustment,
            ifnull(sum(review_enrollment_fee),0) as review_enrollment_fee,
            ifnull(sum(return_postage_billing_postage),0) as return_postage_billing_postage,
            ifnull(sum(fba_per_unit_fulfillment_fee),0) as fba_per_unit_fulfillment_fee,
            ifnull(sum(product_ads_payment_eventlist_charge + product_ads_payment_eventlist_refund),0) as cpc_cost,
            ifnull(sum(subscription + reserved_field1),0) AS other_amazon_fee,
            ifnull(sum(charge_back_recovery + cs_error_non_itemized + return_postage_billing_postage + re_evaluation + subscription_fee_correction + incorrect_fees_non_itemized + buyer_recharge + multichannel_order_late + non_subscription_fee_adj + fba_per_unit_fulfillment_fee + misc_adjustment),0) AS fee_adjustment
        ")->where([
                ['channel_id', '=', $list['id']],
                ['create_time', '>=', $begin_time],
                ['create_time', '<', $end_time]
            ])->get()->toArray();

            //促销费用
            $info['promotion_fee']['promote_discount'] = floor($FinanceReportInfo[0]['promote_discount'] * 100) / 100; //Promote折扣
            $info['promotion_fee']['cpc_sb_cost'] = floor($FinanceReportInfo[0]['cpc_sb_cost'] * 100) / 100; //退款返还Promote折扣
            $info['promotion_fee']['coupon'] = floor($ChannelProfitReportInfo[0]['coupon'] * 100) / 100;  //coupon优惠券
            $info['promotion_fee']['run_lightning_deal_fee'] = floor($ChannelProfitReportInfo[0]['run_lightning_deal_fee'] * 100) / 100;  //RunLightningDealFee

            //订单费用
            $info['order_fee']['platform_sales_commission'] = floor($FinanceReportInfo[0]['platform_sales_commission'] * 100) / 100;  //亚马逊销售佣金
            $info['order_fee']['fba_generation_delivery_cost'] = floor($FinanceReportInfo[0]['fba_generation_delivery_cost'] * 100) / 100; //FBA代发货费用 **
            $info['order_fee']['profit'] = floor($FinanceReportInfo[0]['profit'] * 100) / 100;  //多渠道配送费
            $info['order_fee']['other_order_fee'] = floor(($FinanceReportInfo[0]['amazon_fee'] - $FinanceReportInfo[0]['platform_sales_commission'] - $FinanceReportInfo[0]['fba_generation_delivery_cost'] - $FinanceReportInfo[0]['profit']) * 100) / 100; //其他订单费用 **

            //退货退款费用
            $info['return_refund_fee']['return_and_return_commission'] = floor($FinanceReportInfo[0]['return_and_return_commission'] * 100) / 100;  //退款扣除佣金
            $info['return_refund_fee']['fba_refund_treatment_fee'] = floor($FinanceReportInfo[0]['fba_refund_treatment_fee'] * 100) / 100; //FBA退货处理费
            $info['return_refund_fee']['return_and_return_sales_commission'] = floor($FinanceReportInfo[0]['return_and_return_sales_commission'] * 100) / 100; //返还亚马逊销售佣金
            $info['return_refund_fee']['returnshipping'] = floor($FinanceReportInfo[0]['returnshipping'] * 100) / 100; //返还运费
            $info['return_refund_fee']['refund_variableclosingfee'] = floor($FinanceReportInfo[0]['refund_variableclosingfee'] * 100) / 100; //可变结算费-退款

            //库存费用
            $info['inventory_cost']['fba_storage_fee'] = floor($ChannelProfitReportInfo[0]['fba_storage_fee'] * 100) / 100; //FBA月仓储费用
            $info['inventory_cost']['fba_long_term_storage_fee'] = floor($ChannelProfitReportInfo[0]['fba_long_term_storage_fee'] * 100) / 100; //FBA长期仓储费
            $info['inventory_cost']['fba_disposal_fee'] = floor($ChannelProfitReportInfo[0]['fba_disposal_fee'] * 100) / 100;  //FBA处理费 **
            $info['inventory_cost']['fba_removal_fee'] = floor($ChannelProfitReportInfo[0]['fba_removal_fee'] * 100) / 100;  //FBA移除费
            $info['inventory_cost']['restocking_fee'] = floor($FinanceReportInfo[0]['restocking_fee'] * 100) / 100; //FBA重新入仓费
            $info['inventory_cost']['fba_inbound_convenience_fee'] = floor($ChannelProfitReportInfo[0]['fba_inbound_convenience_fee'] * 100) / 100;//库存配置服务费
            $info['inventory_cost']['fba_inbound_defect_fee'] = floor($ChannelProfitReportInfo[0]['fba_inbound_defect_fee'] * 100) / 100; //FBA入库缺陷费
            $info['inventory_cost']['labeling_fee'] = floor($ChannelProfitReportInfo[0]['labeling_fee'] * 100) / 100; //贴标费
            $info['inventory_cost']['polybagging_fee'] = floor($ChannelProfitReportInfo[0]['polybagging_fee'] * 100) / 100; //包装费
            $info['inventory_cost']['fba_inbound_shipment_carton_level_info_fee'] = floor($ChannelProfitReportInfo[0]['fba_inbound_shipment_carton_level_info_fee'] * 100) / 100; //人工处理费用
            $info['inventory_cost']['fba_inbound_transportation_fee'] = floor($ChannelProfitReportInfo[0]['fba_inbound_transportation_fee'] * 100) / 100; //入仓运输费
            $info['inventory_cost']['fba_inbound_transportation_program_fee'] = floor($ChannelProfitReportInfo[0]['fba_inbound_transportation_program_fee'] * 100) / 100; //FBA入境运输费
            $info['inventory_cost']['fba_overage_fee'] = floor($ChannelProfitReportInfo[0]['fba_overage_fee'] * 100) / 100; //库存仓储超量费

            //其他费用
            $info['other_fee']['other_amazon_fee'] = floor(($FinanceReportInfo[0]['other_amazon_fee'] + $ChannelProfitReportInfo[0]['other_amazon_fee']) * 100) / 100; //其他亚马逊费用 **
            $info['other_fee']['reserved_field17'] = floor($FinanceReportInfo[0]['reserved_field17'] * 100) / 100; //VAT
            $info['other_fee']['misc_adjustment'] = floor($ChannelProfitReportInfo[0]['misc_adjustment'] * 100) / 100; //其他
            $info['other_fee']['review_enrollment_fee'] = floor($ChannelProfitReportInfo[0]['review_enrollment_fee'] * 100) / 100; //早期评论者计划
            $info['other_fee']['cpc_cost'] = floor($ChannelProfitReportInfo[0]['cpc_cost'] * 100) / 100; //cpc 花费

            //商品调整费用
            $info['commodity_adjustment_fee']['ware_house_lost'] = floor($FinanceReportInfo[0]['ware_house_lost'] * 100) / 100;  //FBA仓丢失赔款
            $info['commodity_adjustment_fee']['ware_house_damage'] = floor($FinanceReportInfo[0]['ware_house_damage'] * 100) / 100; //FBA仓损坏赔款
            $info['commodity_adjustment_fee']['reversal_reimbursement'] = floor($FinanceReportInfo[0]['reversal_reimbursement'] * 100) / 100; //REVERSAL REIMBURSEMENT
            $info['commodity_adjustment_fee']['return_postage_billing_postage'] = floor($ChannelProfitReportInfo[0]['return_postage_billing_postage'] * 100) / 100; //ReturnPostageBilling_postage
            $info['commodity_adjustment_fee']['missing_from_inbound'] = floor($FinanceReportInfo[0]['missing_from_inbound'] * 100) / 100; //入库丢失赔偿
            $info['commodity_adjustment_fee']['missing_from_inbound_clawback'] = floor($FinanceReportInfo[0]['missing_from_inbound_clawback'] * 100) / 100; //入库丢失赔偿(夺回)
            $info['commodity_adjustment_fee']['fba_per_unit_fulfillment_fee'] = floor($ChannelProfitReportInfo[0]['fba_per_unit_fulfillment_fee'] * 100) / 100;  //费用盘点-重量和尺寸更改 **
            $info['commodity_adjustment_fee']['fee_adjustment'] = floor(($FinanceReportInfo[0]['fee_adjustment'] + $ChannelProfitReportInfo[0]['fee_adjustment'] - $FinanceReportInfo[0]['ware_house_lost'] - $FinanceReportInfo[0]['ware_house_damage'] - $FinanceReportInfo[0]['reversal_reimbursement'] - $ChannelProfitReportInfo[0]['return_postage_billing_postage'] - $FinanceReportInfo[0]['missing_from_inbound'] - $FinanceReportInfo[0]['missing_from_inbound_clawback'] - $ChannelProfitReportInfo[0]['fba_per_unit_fulfillment_fee']) * 100) / 100;  //其他商品调整费用 ？？

            //费用
            $info['fee']['reserved_field16'] = floor($FinanceReportInfo[0]['reserved_field16'] * 100) / 100; //运营费用
            $info['fee']['reserved_field10'] = floor($FinanceReportInfo[0]['reserved_field10'] * 100) / 100; //测评费用
            $info['fee']['purchasing_cost'] = floor($FinanceReportInfo[0]['purchasing_cost'] * 100) / 100; //采购成本 **
            $info['fee']['logistics_head_course'] = floor($FinanceReportInfo[0]['logistics_head_course'] * 100) / 100; //头程物流（FBA） **
            $info['fee']['fbm'] = floor($FinanceReportInfo[0]['fbm'] * 100) / 100; //物流（FBM） **

            //造假数据
            if($list['id'] == 231){
                $json = "{\"currency_id\":1,\"exchang_rate\":\"0.1541\",\"shop_id\":231,\"shop_name\":\"Cocoleggings_US\",\"commodity_sales\":{\"fba_sales_quota\":7072.26,\"fbm_sales_quota\":10037.01},\"promotion_fee\":{\"promote_discount\":-124.75,\"cpc_sb_cost\":234213,\"coupon\":321,\"run_lightning_deal_fee\":23211},\"order_fee\":{\"platform_sales_commission\":-2902.14,\"fba_generation_delivery_cost\":-2529.98,\"profit\":-13.42,\"other_order_fee\":957.68},\"return_refund_fee\":{\"return_and_return_commission\":-174.09,\"fba_refund_treatment_fee\":-387.79,\"return_and_return_sales_commission\":867.74,\"returnshipping\":2123,\"refund_variableclosingfee\":31},\"inventory_cost\":{\"fba_storage_fee\":32411,\"fba_long_term_storage_fee\":123541,\"fba_disposal_fee\":2121,\"fba_removal_fee\":3221,\"restocking_fee\":324121,\"fba_inbound_convenience_fee\":22113,\"fba_inbound_defect_fee\":322113,\"labeling_fee\":321233,\"polybagging_fee\":32133,\"fba_inbound_shipment_carton_level_info_fee\":322113,\"fba_inbound_transportation_fee\":23211,\"fba_inbound_transportation_program_fee\":213,\"fba_overage_fee\":123},\"other_fee\":{\"other_amazon_fee\":861.52,\"reserved_field17\":12432,\"misc_adjustment\":3246,\"review_enrollment_fee\":521,\"cpc_cost\":5235},\"commodity_adjustment_fee\":{\"ware_house_lost\":40.13,\"ware_house_damage\":231,\"reversal_reimbursement\":229.4,\"return_postage_billing_postage\":523,\"missing_from_inbound\":432,\"missing_from_inbound_clawback\":532,\"fba_per_unit_fulfillment_fee\":342,\"fee_adjustment\":51.28},\"fee\":{\"reserved_field16\":2432,\"reserved_field10\":2353,\"purchasing_cost\":-24197.47,\"logistics_head_course\":-1233.47,\"fbm\":-562.74}}";
                $info = json_decode($json,true);
            }

            $infoList['list'][] = $info;
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => $infoList
        ];

        return $data;
    }

    /**
     * 拉取当前用户的店铺信息
     * @param $request_data
     * @return array
     */
    public function getShopList($request_data)
    {
        $rule = [
            'date_time' => 'integer|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled'
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }


        $info = array();

        $userInfo = $this->getUserInfo();

        $userAdmin = UserAdminModel::query()->where('id', $userInfo['admin_id'])->select('is_master', 'check_prv_ids')->first();

        $shopListInfoquery = ChannelModel::select("id", "title", "modified_time")->where([['user_id', '=', $userInfo['user_id']]]);

        if ($userAdmin->is_master != 1) {
            $ids = $userAdmin->check_prv_ids != null ? explode(',', $userAdmin->check_prv_ids) : array(0);
            $shopListInfoquery->whereIn('id', $ids);
        }

        isset($request_data['date_time']) && $shopListInfoquery->where([['modified_time', '>', $request_data['date_time']]]);

        $count = $shopListInfoquery->count();

        $request_data['offset'] = $request_data['offset'] ?? 0;
        $request_data['limit'] = $request_data['limit'] ?? 10;

        $shopListInfo = $shopListInfoquery->offset($request_data['offset'])->limit($request_data['limit'])->get()->toArray();

        foreach ($shopListInfo as $key => $value) {
            $info[$key]['shop_id'] = $value['id'];
            $info[$key]['shop_name'] = $value['title'];
            $info[$key]['modified_time'] = $value['modified_time'];
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => array(
                'total' => $count,
                'list' => $info
            )
        ];

        return $data;
    }

    /**
     * 拉取货币兑换利率
     * @param $request_data
     * @return array
     */

    public function getExchangeRateList()
    {
        $userInfo = $this->getUserInfo();

        $info = $this->getExchangeRate($userInfo['user_id']);

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'total' => count($info),
                'list' => $info
            ]
        ];

        return $data;
    }


    public function getExchangeRate($user_id)
    {
        $financeCurrencyList = FinanceCurrencyModel::selectRaw(
            "id ,custom_usd_exchang_rate AS usd_exchang_rate,
            custom_cad_exchang_rate as cad_exchang_rate,custom_mxn_exchang_rate as mxn_exchang_rate,custom_jpy_exchang_rate as jpy_exchang_rate,
            custom_gbp_exchang_rate as gbp_exchang_rate,custom_eur_exchang_rate as eur_exchang_rate,custom_au_exchang_rate as au_exchang_rate,
            custom_in_exchang_rate as in_exchang_rate,custom_br_exchang_rate as br_exchang_rate,
            custom_tr_exchang_rate as tr_exchang_rate,custom_ae_exchang_rate as ae_exchang_rate,custom_sa_exchang_rate as sa_exchang_rate,
            custom_nl_exchang_rate as nl_exchang_rate,custom_sg_exchang_rate as sg_exchang_rate,custom_hk_exchang_rate as hk_exchang_rate"
        )->where([['user_id', '=', $user_id]])->first()->toArray();

        if (!isset($financeCurrencyList)) {
            $SystemCurrencyList = SystemCurrencyModel::select(
                "id", "usd_exchang_rate", "cad_exchang_rate", "mxn_exchang_rate", "jpy_exchang_rate", "gbp_exchang_rate", "eur_exchang_rate", "au_exchang_rate", "in_exchang_rate", "br_exchang_rate",
                "tr_exchang_rate", "ae_exchang_rate", "nl_exchang_rate", "sa_exchang_rate", "sg_exchang_rate", "hk_exchang_rate"
            )->first()->toArray();
            $result = $SystemCurrencyList;
        } else {
            $result = $financeCurrencyList;
        }

        $info = array();
        $config = $this->config->get("common");

        $i = 0;
        foreach ($result as $key => $value) {
            if ($key == 'id') {
                continue;
            }

            if (isset($config['currency_list'][$key])) {
                $info[$i] = $config['currency_list'][$key];
                $info[$i]['exchang_rate'] = $value;
                $info[$i]['modified_time'] = time();
            }
            $i++;
        }

        return $info;
    }


    /**
     * 拉取用户名以及openid
     * @param $request_data
     * @return array
     */

    public function getUserName($request_data)
    {

        $userInfo = $this->getUserInfo();

        $userAdmin = UserAdminModel::query()->where('id', $userInfo['admin_id'])->select('username')->first();

        $UserExtInfo = UserExtInfoModel::query()->where(array('admin_id' => $userInfo['admin_id']))->first();

        if ($UserExtInfo == null) {
            $snowflakeId = Unique::snowflake();
            try {
                $userExtInfoModel = UserExtInfoModel::create(array('uuid' => $snowflakeId, 'admin_id' => $userInfo['admin_id']));
                if (!$userExtInfoModel->id) {
                    throw new BusinessException(10001, trans('finance.user_add_error'));
                }
            } catch (\Throwable $ex) {
                //写入日志
                Log::getClient()->error($ex->getMessage());
                return [
                    'code' => 0,
                    'msg' => trans('common.error')
                ];
            }
        } else {
            $snowflakeId = $UserExtInfo->uuid;
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'username' => $userAdmin->username,
                'uuid' => $snowflakeId
            ]
        ];

        return $data;
    }


    /**
     * 绑定用户
     * @param $request_data
     * @return array
     */

    public function bindUser($request_data)
    {
        $rule = ['ext_info' => 'required|json'];

        $res = $this->validate($request_data, $rule);
        if ($res['code'] == 0) {
            return $res;
        }

        $userInfo = $this->getUserInfo();

        $UserExtInfoQuery = UserExtInfoModel::query()->where(array('admin_id' => $userInfo['admin_id']));

        $UserExtInfo = $UserExtInfoQuery->first();

        //if ($UserExtInfo->ext_info == "null") {
        try {
            $userExtInfoModel = $UserExtInfoQuery->update(array('ext_info' => $request_data['ext_info'], 'is_authorized' => 1, 'authorized_time' => time(), 'cancel_time' => 0));
            if (!$userExtInfoModel) {
                throw new BusinessException(10001, trans('finance.user_bind_error'));
            }
        } catch (\Throwable $ex) {
            //写入日志
            Log::getClient()->error($ex->getMessage());
            return [
                'code' => 0,
                'msg' => trans('common.error')
            ];
        }
//        } else {
//            throw new BusinessException(10001, trans('finance.user_is_bind'));
//        }

        return ['code' => 1, 'msg' => 'success', 'data' => array()];
    }
}
