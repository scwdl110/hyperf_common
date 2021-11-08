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
use App\Model\FinanceMoneyBackModel;
use App\Model\SeapigeonCategoryListModel;
use App\Model\SiteMessageModel;
use App\Model\SynchronouslyManagementTaskModel;
use App\Model\SystemCurrencyModel;
use App\Model\UserAdminModel;
use App\Model\UserExtInfoModel;
use App\Service\BaseService;
use App\Model\FinanceReportModel;
use Hyperf\RpcServer\Annotation\RpcService;
use Hyperf\Utils\Context;
use Hyperf\Utils\Coroutine;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Psr\Http\Message\ServerRequestInterface;
use Hyperf\Contract\ConfigInterface;
use Captainbi\Hyperf\Util\Unique;
use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Log;
use Hyperf\DbConnection\Db;
use Hyperf\Guzzle\ClientFactory;
use Hyperf\Utils\ApplicationContext;

/**
 * @RpcService(name="AccountingService", protocol="jsonrpc-http", server="jsonrpc-http", publishTo="consul")
 */
class AccountingService extends BaseService
{

    /**
     * @Inject()
     * @var ServerRequestInterface
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
     * @Inject()
     * @var FinanceService
     */
    protected $financeService;


    /**
     * 获取财务利润信息
     * @param $request_data
     * @return array
     */
    public function getFiancialProfitInfo($request_data, $isRpc = false, $userInfo = array())
    {
        isset($request_data['date']) && $request_data['date_time'] = $request_data['date'];
        !isset($request_data['date_time']) && $request_data['date_time'] = "2021-05";

        $rule = [
            'date_time' => 'string|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
            'shop_name' => 'string|filled',
            'shop_ids' => 'string|filled',
            'no_limit' => 'integer|filled',
            'country_site' => 'integer|filled',
            'site_id' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        if ($isRpc == true) {
            $request = $this->request->withAttribute('userInfo', [
                'admin_id' => $userInfo['admin_id'],
                'user_id' => $userInfo['user_id'],
                'is_master' => $userInfo['is_master'],
                'dbhost' => $userInfo['dbhost'],
                'codeno' => $userInfo['codeno'],
            ]);
            Context::set(ServerRequestInterface::class, $request);
        } else {
            $userInfo = $this->getUserInfo();
        }


        $res = $this->getShopInfo($request_data, $userInfo);

        $rateList = $this->getExchangeRate($res['user_info']['user_id']);

        $infoList = array('total' => $res['count'], 'list' => array());

        foreach ($res['shopListInfo'] as $key => $list) {

            $info = array();

            $FinanceReportInfo = Unique::getArray(FinanceReportModel::selectRaw("
          	ifnull( sum( sales_quota * ( reserved_field11 / sales_volume )), 0 ) AS fba_sales_quota,
	        ifnull( sum( sales_quota ) - sum( sales_quota * ( reserved_field11 / sales_volume )), 0 ) AS fbm_sales_quota,
            ifnull(sum(promote_discount),0) as promote_discount,
            ifnull(sum(refund_promote_discount),0) as cpc_sb_cost ,
            ifnull(sum(platform_sales_commission),0) as platform_sales_commission,
            ifnull(sum(fba_generation_delivery_cost - profit),0) as fba_generation_delivery_cost,
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
            ifnull(sum(gift_wrap + shipping_charge + shipping_charge_charge_back + shipping_tax + tax + gift_wrap_tax + refund_shipping_charge + refund_shipping_charge_charge_back + refund_shipping_tax + refund_tax + marketplace_facilitator_tax_shipping + marketplace_facilitator_tax_principal + order_lowvaluegoods_other + order_giftwrapchargeback + order_shippinghb + salestaxcollectionfee + costofpointsgranted + order_codchargeback + amazonexclusivesfee + giftwrapcommission + order_paymentmethodfee + cod_tax + refund_lowvaluegoods_other + marketplacefacilitatortax_restockingfee + goodwill + refund_paymentmethodfee + refund_codchargeback + refund_shippinghb + refund_giftwrapchargeback + costofpointsreturned + pointsadjusted + reserved_field3 + reserved_field4 + reserved_field6 + reserved_field7 + reserved_field8 + reserved_field14 + reserved_field15 + reserved_field18 + cpc_sb_sales_quota + cpc_sb_cost + refund_rate + profit_margin),0) AS other_amazon_fee,
            ifnull(sum(ware_house_lost + ware_house_damage + ware_house_lost_manual + ware_house_damage_exception + reversal_reimbursement + disposal_fee + free_replacement_refund_items + removal_order_lost + incorrect_fees_items + missing_from_inbound_clawback + missing_from_inbound + inbound_carrier_damage + multichannel_order_lost + payment_retraction_items + reserved_field19),0) AS fee_adjustment
        ")->where([
                ['channel_id', '=', $list['id']],
                ['create_time', '>=', $res['begin_time']],
                ['create_time', '<=', $res['end_time']],
                ['user_id', '=', $res['user_info']['user_id']]
            ])->get());

            foreach ($rateList as $rate) {
                if ((is_array($rate['site_id']) && in_array($list['site_id'], $rate['site_id'])) || $list['site_id'] == $rate['site_id']) {
                    $info['currency_id'] = $rate['id'];
                    $info['exchang_rate'] = $rate['exchang_rate'];
                }
            }

            //商品销售额
            $info['shop_id'] = $list['id'];
            $info['shop_name'] = $list['title'];

            $info['commodity_sales']['fba_sales_quota'] = floor($FinanceReportInfo[0]['fba_sales_quota'] * 100) / 100; //FBA销售额
            $info['commodity_sales']['fbm_sales_quota'] = floor($FinanceReportInfo[0]['fbm_sales_quota'] * 100) / 100; //FBM销售额
            $info['commodity_sales']['total_amount'] = $info['commodity_sales']['fba_sales_quota'] + $info['commodity_sales']['fbm_sales_quota']; //**商品销售额

            $ChannelProfitReportInfo = Unique::getArray(ChannelProfitReportModel::selectRaw("
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
            ifnull(sum(subscription + reserved_field1 + reserved_field10),0) AS other_amazon_fee,
            ifnull(sum(charge_back_recovery + cs_error_non_itemized + return_postage_billing_postage + re_evaluation + subscription_fee_correction + incorrect_fees_non_itemized + buyer_recharge + multichannel_order_late + non_subscription_fee_adj + fba_per_unit_fulfillment_fee + misc_adjustment),0) AS fee_adjustment
        ")->where([
                ['channel_id', '=', $list['id']],
                ['create_time', '>=', $res['begin_time']],
                ['create_time', '<=', $res['end_time']],
                ['user_id', '=', $res['user_info']['user_id']]
            ])->get());


            //促销费用
            $info['promotion_fee']['promote_discount'] = floor($FinanceReportInfo[0]['promote_discount'] * 100) / 100; //Promote折扣
            $info['promotion_fee']['cpc_sb_cost'] = floor($FinanceReportInfo[0]['cpc_sb_cost'] * 100) / 100; //退款返还Promote折扣
            $info['promotion_fee']['coupon'] = floor($ChannelProfitReportInfo[0]['coupon'] * 100) / 100;  //coupon优惠券
            $info['promotion_fee']['run_lightning_deal_fee'] = floor($ChannelProfitReportInfo[0]['run_lightning_deal_fee'] * 100) / 100;  //RunLightningDealFee
            $info['promotion_fee']['total_amount'] = $info['promotion_fee']['promote_discount'] + $info['promotion_fee']['cpc_sb_cost'] + $info['promotion_fee']['coupon'] + $info['promotion_fee']['run_lightning_deal_fee'];   //**促销费用

            //订单费用
            $info['order_fee']['platform_sales_commission'] = floor($FinanceReportInfo[0]['platform_sales_commission'] * 100) / 100;  //亚马逊销售佣金
            $info['order_fee']['fba_generation_delivery_cost'] = floor($FinanceReportInfo[0]['fba_generation_delivery_cost'] * 100) / 100; //FBA代发货费用 **
            $info['order_fee']['profit'] = floor($FinanceReportInfo[0]['profit'] * 100) / 100;  //多渠道配送费
            $info['order_fee']['other_order_fee'] = floor(($FinanceReportInfo[0]['amazon_fee'] - $FinanceReportInfo[0]['platform_sales_commission'] - $FinanceReportInfo[0]['fba_generation_delivery_cost'] - $FinanceReportInfo[0]['profit']) * 100) / 100; //其他订单费用 **
            $info['order_fee']['total_amount'] = $info['order_fee']['platform_sales_commission'] + $info['order_fee']['fba_generation_delivery_cost'] + $info['order_fee']['profit'] + $info['order_fee']['other_order_fee'];  //**订单费用


            //退货退款费用
            $info['return_refund_fee']['return_and_return_commission'] = floor($FinanceReportInfo[0]['return_and_return_commission'] * 100) / 100;  //退款扣除佣金
            $info['return_refund_fee']['fba_refund_treatment_fee'] = floor($FinanceReportInfo[0]['fba_refund_treatment_fee'] * 100) / 100; //FBA退货处理费
            $info['return_refund_fee']['return_and_return_sales_commission'] = floor($FinanceReportInfo[0]['return_and_return_sales_commission'] * 100) / 100; //返还亚马逊销售佣金
            $info['return_refund_fee']['returnshipping'] = floor($FinanceReportInfo[0]['returnshipping'] * 100) / 100; //返还运费
            $info['return_refund_fee']['refund_variableclosingfee'] = floor($FinanceReportInfo[0]['refund_variableclosingfee'] * 100) / 100; //可变结算费-退款
            $info['return_refund_fee']['total_amount'] = $info['return_refund_fee']['return_and_return_commission'] + $info['return_refund_fee']['fba_refund_treatment_fee'] + $info['return_refund_fee']['return_and_return_sales_commission'] + $info['return_refund_fee']['returnshipping'] + $info['return_refund_fee']['refund_variableclosingfee'];  //**退货退款费用


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
            $info['inventory_cost']['total_amount'] = $info['inventory_cost']['fba_storage_fee'] + $info['inventory_cost']['fba_long_term_storage_fee'] + $info['inventory_cost']['fba_disposal_fee'] + $info['inventory_cost']['fba_removal_fee'] + $info['inventory_cost']['restocking_fee'] + $info['inventory_cost']['fba_inbound_convenience_fee'] + $info['inventory_cost']['fba_inbound_defect_fee'] + $info['inventory_cost']['labeling_fee'] + $info['inventory_cost']['polybagging_fee'] + $info['inventory_cost']['fba_inbound_shipment_carton_level_info_fee'] + $info['inventory_cost']['fba_inbound_transportation_fee'] + $info['inventory_cost']['fba_inbound_transportation_program_fee'] + $info['inventory_cost']['fba_overage_fee'];    //** 库存费用

            //其他费用
            $info['other_fee']['other_amazon_fee'] = floor(($FinanceReportInfo[0]['other_amazon_fee'] + $ChannelProfitReportInfo[0]['other_amazon_fee']) * 100) / 100; //**其他亚马逊费用
            $info['other_fee']['reserved_field17'] = floor($FinanceReportInfo[0]['reserved_field17'] * 100) / 100; //**VAT
            $info['other_fee']['misc_adjustment'] = floor($ChannelProfitReportInfo[0]['misc_adjustment'] * 100) / 100; //**其他
            $info['other_fee']['review_enrollment_fee'] = floor($ChannelProfitReportInfo[0]['review_enrollment_fee'] * 100) / 100; // **早期评论者计划
            $info['other_fee']['cpc_cost'] = floor($ChannelProfitReportInfo[0]['cpc_cost'] * 100) / 100;   //**cpc花费


            //商品调整费用
            $info['commodity_adjustment_fee']['ware_house_lost'] = floor($FinanceReportInfo[0]['ware_house_lost'] * 100) / 100;  //FBA仓丢失赔款
            $info['commodity_adjustment_fee']['ware_house_damage'] = floor($FinanceReportInfo[0]['ware_house_damage'] * 100) / 100; //FBA仓损坏赔款
            $info['commodity_adjustment_fee']['reversal_reimbursement'] = floor($FinanceReportInfo[0]['reversal_reimbursement'] * 100) / 100; //REVERSAL REIMBURSEMENT
            $info['commodity_adjustment_fee']['return_postage_billing_postage'] = floor($ChannelProfitReportInfo[0]['return_postage_billing_postage'] * 100) / 100; //ReturnPostageBilling_postage
            $info['commodity_adjustment_fee']['missing_from_inbound'] = floor($FinanceReportInfo[0]['missing_from_inbound'] * 100) / 100; //入库丢失赔偿
            $info['commodity_adjustment_fee']['missing_from_inbound_clawback'] = floor($FinanceReportInfo[0]['missing_from_inbound_clawback'] * 100) / 100; //入库丢失赔偿(夺回)
            $info['commodity_adjustment_fee']['fba_per_unit_fulfillment_fee'] = floor($ChannelProfitReportInfo[0]['fba_per_unit_fulfillment_fee'] * 100) / 100;  //费用盘点-重量和尺寸更改 **
            $info['commodity_adjustment_fee']['fee_adjustment'] = floor(($FinanceReportInfo[0]['fee_adjustment'] + $ChannelProfitReportInfo[0]['fee_adjustment'] - $FinanceReportInfo[0]['ware_house_lost'] - $FinanceReportInfo[0]['ware_house_damage'] - $FinanceReportInfo[0]['reversal_reimbursement'] - $ChannelProfitReportInfo[0]['return_postage_billing_postage'] - $FinanceReportInfo[0]['missing_from_inbound'] - $FinanceReportInfo[0]['missing_from_inbound_clawback'] - $ChannelProfitReportInfo[0]['fba_per_unit_fulfillment_fee']) * 100) / 100;  //其他商品调整费用
            $info['commodity_adjustment_fee']['total_amount'] = $info['commodity_adjustment_fee']['ware_house_lost'] + $info['commodity_adjustment_fee']['ware_house_damage'] + $info['commodity_adjustment_fee']['reversal_reimbursement'] + $info['commodity_adjustment_fee']['return_postage_billing_postage'] + $info['commodity_adjustment_fee']['missing_from_inbound'] + $info['commodity_adjustment_fee']['missing_from_inbound_clawback'] + $info['commodity_adjustment_fee']['fba_per_unit_fulfillment_fee'] + $info['commodity_adjustment_fee']['fee_adjustment'];  //** 商品调整费用

            //费用
            $info['fee']['reserved_field16'] = floor($FinanceReportInfo[0]['reserved_field16'] * 100) / 100; //**  运营费用
            $info['fee']['reserved_field10'] = floor($FinanceReportInfo[0]['reserved_field10'] * 100) / 100; //**  测评费用
            $info['fee']['purchasing_cost'] = floor($FinanceReportInfo[0]['purchasing_cost'] * 100) / 100; //**  采购成本
            $info['fee']['logistics_head_course'] = floor($FinanceReportInfo[0]['logistics_head_course'] * 100) / 100; //** 头程物流（FBA）
            $info['fee']['fbm'] = floor($FinanceReportInfo[0]['fbm'] * 100) / 100; //**  物流（FBM）
            $info['fee']['gross_profit'] = $info['commodity_sales']['total_amount'] + $info['promotion_fee']['total_amount'] + $info['order_fee']['total_amount'] + $info['return_refund_fee']['total_amount'] + $info['inventory_cost']['total_amount'] + $info['other_fee']['other_amazon_fee'] + $info['other_fee']['reserved_field17'] + $info['other_fee']['misc_adjustment'] + $info['other_fee']['review_enrollment_fee'] + $info['other_fee']['cpc_cost'] + $info['commodity_adjustment_fee']['total_amount'] + $info['fee']['reserved_field16'] + $info['fee']['reserved_field10'] + $info['fee']['purchasing_cost'] + $info['fee']['logistics_head_course'] + $info['fee']['fbm'];  //**  毛利润

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
     * 回款单
     * @param $request_data
     * @param bool $isRpc
     * @param array $userInfo
     * @return array
     */

    public function getMoneyCallBackInfo($request_data, $isRpc = false, $userInfo = array())
    {
        isset($request_data['date']) && $request_data['date_time'] = $request_data['date'];
        !isset($request_data['date_time']) && $request_data['date_time'] = "2021-05";

        $rule = [
            'date_time' => 'string|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
            'shop_name' => 'string|filled',
            'shop_ids' => 'string|filled',
            'no_limit' => 'integer|filled',
            'country_site' => 'integer|filled',
            'site_id' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        if ($isRpc == true) {
            $request = $this->request->withAttribute('userInfo', [
                'admin_id' => $userInfo['admin_id'],
                'user_id' => $userInfo['user_id'],
                'is_master' => $userInfo['is_master'],
                'dbhost' => $userInfo['dbhost'],
                'codeno' => $userInfo['codeno'],
            ]);
            Context::set(ServerRequestInterface::class, $request);
        } else {
            $userInfo = $this->getUserInfo();
        }


        $res = $this->getShopInfo($request_data, $userInfo);

        $rateList = $this->getExchangeRate($res['user_info']['user_id']);

        $infoList = array('total' => $res['count'], 'list' => array());

        foreach ($res['shopListInfo'] as $key => $list) {

            $info = array();

            //商品销售额
            $info['shop_id'] = $list['id'];
            $info['shop_name'] = $list['title'];
            $info['site_id'] = $list['site_id'];
            $info['currency_symbol'] = $list['currency_symbol'];

            foreach ($rateList as $rate) {
                if ((is_array($rate['site_id']) && in_array($list['site_id'], $rate['site_id'])) || $list['site_id'] == $rate['site_id']) {
                    $info['currency_id'] = $rate['id'];
                    $info['exchang_rate'] = $rate['exchang_rate'];
                }
            }

            $FinanceMoneyBackInfo = Unique::getArray(FinanceMoneyBackModel::selectRaw("
            sum( currency_amount ) AS money_back_amount
            ")->where([
                ['channel_id', '=', $list['id']],
                ['fundtransfer_date_int', '>=', $res['begin_time']],
                ['fundtransfer_date_int', '<=', $res['end_time']],
                ['user_id', '=', $res['user_info']['user_id']],
                ['fundtransfer_status', '=', 0]
            ])->get());

            $info['money_back_amount'] = floor($FinanceMoneyBackInfo[0]['money_back_amount'] * 100) / 100; //**  回款费用

            $info['code'] = $list['code'];
            $infoList['total'] = $res['count'];
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
     * 财务指标
     * @param $request_data
     * @param bool $isRpc
     * @param array $userInfo
     * @return array
     */

    public function getFinanceIndex($request_data, $isRpc = false, $userInfo = array())
    {
        $rule = [
            'date_time' => 'string|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
            'shop_name' => 'string|filled',
            'shop_ids' => 'string|filled',
            'no_limit' => 'integer|filled',
            'country_site' => 'integer|filled',
            'site_id' => 'integer|filled',
            'document_id' => 'integer|filled',
            'app_id' => 'integer|filled'
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        if ($isRpc == true) {
            $request = $this->request->withAttribute('userInfo', [
                'admin_id' => $userInfo['admin_id'],
                'user_id' => $userInfo['user_id'],
                'is_master' => $userInfo['is_master'],
                'dbhost' => $userInfo['dbhost'],
                'codeno' => $userInfo['codeno'],
            ]);
            Context::set(ServerRequestInterface::class, $request);
        } else {
            $userInfo = $this->getUserInfo();
        }


        $category_list = Unique::getArray(SeapigeonCategoryListModel::query()->selectRaw("group_concat(`index_summary_name`) as index_summary")->where([
            ["document_id", "=", $request_data['document_id']],
            ["app_id", "=", $request_data['app_id']],
        ])->first());


        $infoList['total'] = 0;
        $infoList['list'] = array();

        if (!empty($category_list['index_summary'])) {
            //拉取所有店铺
            $res = $this->getShopInfo($request_data, $userInfo);

            $rateList = $this->getExchangeRate($res['user_info']['user_id']);
            if ($category_list['index_summary'] == "callback_money") {
                foreach ($res['shopListInfo'] as $key => $list) {
                    $info = array();
                    //商品销售额
                    $info['shop_id'] = $list['id'];
                    $info['shop_name'] = $list['title'];
                    $info['site_id'] = $list['site_id'];
                    $info['currency_symbol'] = $list['currency_symbol'];
                    $info['code'] = $list['code'];

                    foreach ($rateList as $rate) {
                        if ((is_array($rate['site_id']) && in_array($list['site_id'], $rate['site_id'])) || $list['site_id'] == $rate['site_id']) {
                            $info['currency_id'] = $rate['id'];
                            $info['exchang_rate'] = $rate['exchang_rate'];
                        }
                    }

                    $FinanceMoneyBackInfo = Unique::getArray(FinanceMoneyBackModel::selectRaw("
                        sum( currency_amount ) AS money_back_amount
                        ")->where([
                        ['channel_id', '=', $list['id']],
                        ['fundtransfer_date_int', '>=', $res['begin_time']],
                        ['fundtransfer_date_int', '<=', $res['end_time']],
                        ['user_id', '=', $res['user_info']['user_id']],
                        ['fundtransfer_status', '=', 0]
                    ])->get());

                    $info['indicators']['money_back_amount'] = floor($FinanceMoneyBackInfo[0]['money_back_amount'] * 100) / 100; //**  回款费用

                    $infoList['total'] = $res['count'];
                    $infoList['list'][] = $info;
                }
            } else {
                $channelIds = array();

                foreach ($res['shopListInfo'] as $shop) {
                    $channelIds[] = $shop['id'];
                }

                $finance_data = array(
                    "channelIds" => $channelIds,
                    "searchVal" => '',
                    "searchKey" => '',
                    "matchType" => '',
                    "searchType" => 0,
                    "is_new_index" => 1,
                    "params" => array(
                        "user_id" => $userInfo['user_id'],
                        "admin_id" => $userInfo['admin_id'],
                        "target" => $category_list['index_summary'],
                        "ark_name" => "",
                        "template_type" => 1,
                        "count_dimension" => "channel_id",
                        "count_periods" => 0,
                        "target_template" => 1,
                        "remark" => "",
                        "currency_code" => "ORIGIN",
                        "sale_datas_origin" => 2,
                        "refund_datas_origin" => 2,
                        "finance_datas_origin" => 2,
                        "cost_count_type" => 1,
                        "time_type" => 99,
                        "search_start_time" => $res['begin_time'],
                        "search_end_time" => $res['end_time'],
                        "is_distinct_channel" => 0,
                        "show_type" => 2,
                        "time_target" => "",
                        "channel_ids_arr" => $channelIds,
                        "limit_num" => 0,
                        "page" => 1,
                        "rows" => 99999,
                        "where_detail" => array(),
                        "is_count" => 0
                    ),
                    "page" => 1,
                    "rows" => 99999,
                );

                $index_summary_list = explode(",", $category_list['index_summary']);

                $finance_index_res = $this->financeService->handleRequest(0, $finance_data);

                foreach ($res['shopListInfo'] as $key => $list) {
                    $info = array();
                    //商品销售额
                    $info['shop_id'] = $list['id'];
                    $info['shop_name'] = $list['title'];
                    $info['site_id'] = $list['site_id'];
                    $info['currency_symbol'] = $list['currency_symbol'];
                    $info['code'] = $list['code'];

                    foreach ($rateList as $rate) {
                        if ((is_array($rate['site_id']) && in_array($list['site_id'], $rate['site_id'])) || $list['site_id'] == $rate['site_id']) {
                            $info['currency_id'] = $rate['id'];
                            $info['exchang_rate'] = $rate['exchang_rate'];
                        }
                    }

                    foreach ($index_summary_list as $index_summary) {
                        $info['indicators'][$index_summary] = "0.00";
                        foreach ($finance_index_res['lists'] as $index) {
                            if ($info['shop_id'] == $index['channel_id']) {
                                if (isset($index[$index_summary])) {
                                    $info['indicators'][$index_summary] = $index[$index_summary];
                                } else {
                                    $info['indicators'][$index_summary] = "0.00";
                                }
                            }
                        }
                    }

                    $infoList['total'] = $res['count'];
                    $infoList['list'][] = $info;
                }
            }
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
    public function getShopList($request_data, $isRpc = false, $userInfo = array())
    {
        isset($request_data['date']) && $request_data['date_time'] = $request_data['date'];
        !isset($request_data['date_time']) && $request_data['date_time'] = "2021-05";

        $rule = [
            'date_time' => 'string|filled',
            'offset' => 'integer|filled',
            'limit' => 'integer|filled',
            'shop_name' => 'string|filled',
            'shop_ids' => 'string|filled',
            'no_limit' => 'integer|filled',
            'country_site' => 'integer|filled',
            'site_id' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        if ($isRpc == true) {
            $request = $this->request->withAttribute('userInfo', [
                'admin_id' => $userInfo['admin_id'],
                'user_id' => $userInfo['user_id'],
                'is_master' => $userInfo['is_master'],
                'dbhost' => $userInfo['dbhost'],
                'codeno' => $userInfo['codeno'],
            ]);
            Context::set(ServerRequestInterface::class, $request);
        } else {
            $userInfo = $this->getUserInfo();
        }

        $res = $this->getShopInfo($request_data, $userInfo);

        $info = array();

        foreach ($res['shopListInfo'] as $key => $value) {
            $info[$key]['shop_id'] = $value['id'];
            $info[$key]['shop_name'] = $value['title'];
            $info[$key]['modified_time'] = $value['modified_time'];
            $info[$key]['code'] = $value['code'];
            $info[$key]['site_id'] = $value['site_id'];
            $info[$key]['currency_symbol'] = $value['currency_symbol'];
            $info[$key]['currency_name'] = $value['currency_name'];
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => array(
                'total' => $res['count'],
                'list' => $info
            )
        ];

        return $data;
    }

    /**
     * 获取店铺信息
     * @param $request_data
     * @param bool $isRpc
     * @param array $userInfo
     * @return array
     */

    private function getShopInfo($request_data, $userInfo = array())
    {
        $current_firstday = date('Y-m-01', strtotime($request_data['date_time']));
        $current_lastday = date('Y-m-d', strtotime("$current_firstday +1 month -1 day"));

        $begin_time = strtotime($current_firstday . " 00:00:00");
        $end_time = strtotime($current_lastday . " 23:59:59");


        $userAdmin = UserAdminModel::query()->where('id', $userInfo['admin_id'])->select('is_master', 'check_prv_ids')->first();

        $shopListInfoquery = ChannelModel::select("id", "title", "site_id", "modified_time")->where([['user_id', '=', $userInfo['user_id']]]);

        $country_info = $this->config->get('common.amzon_site_country1');

        $currency_info = $this->config->get('common.currency_list');

        if (isset($request_data['country_site'])) {
            $request_data['site_id'] = '';
            foreach ($country_info as $country) {
                if ($country['site_group_id'] == $request_data['country_site']) {
                    $request_data['site_id'] .= $country['id'] . ',';
                }
            }
            $request_data['site_id'] = trim($request_data['site_id'], ',');
        }

        if (isset($request_data['site_id'])) {

            $request_data['site_id'] = explode(",", $request_data['site_id']);

            $shopListInfoquery->whereIn('site_id', $request_data['site_id']);
        }

        if (isset($request_data['shop_name'])) {
            $shopListInfoquery->where('title', 'like', '%' . $request_data['shop_name'] . '%');
        }

        if (isset($request_data['shop_ids'])) {

            $request_data['shop_ids'] = explode(",", $request_data['shop_ids']);

            $shopListInfoquery->whereIn('id', $request_data['shop_ids']);
        }

        if ($userAdmin->is_master != 1) {
            $ids = $userAdmin->check_prv_ids != null ? explode(',', $userAdmin->check_prv_ids) : array(0);
            $shopListInfoquery->whereIn('id', $ids);
        }

        isset($request_data['date_time']) && $shopListInfoquery->where([['modified_time', '>', $request_data['date_time']]]);

        $shopListInfoquery->where([
            ['channel_type', '=', 2],
            ['status', '=', 1]
        ]);

        $count = $shopListInfoquery->count();

        if (isset($request_data['no_limit']) && $request_data['no_limit'] == 1) {
            $shopListInfo = Unique::getArray($shopListInfoquery->get());
        } else {
            $request_data['offset'] = $request_data['offset'] ?? 0;
            $request_data['limit'] = $request_data['limit'] ?? 10;
            $shopListInfo = Unique::getArray($shopListInfoquery->offset($request_data['offset'])->limit($request_data['limit'])->get());
        }

        $data = array();


        foreach ($shopListInfo as $key => $shop) {
            foreach ($country_info as $country) {
                if ($country['id'] == $shop['site_id']) {
                    $shop['code'] = $country['code'];
                    $shop['currency_symbol'] = $country['currency_symbol'];
                    $data[$key] = $shop;
                }
            }
            foreach ($currency_info as $currency) {
                if (is_array($currency['site_id']) && in_array($shop['site_id'], $currency['site_id'])) {
                    $shop['currency_name'] = $currency['name'];
                    $data[$key] = $shop;
                } else {
                    if ($currency['site_id'] == $shop['site_id']) {
                        $shop['currency_name'] = $currency['name'];
                        $data[$key] = $shop;
                    }
                }
            }
        }

        return array('count' => $count, 'shopListInfo' => $data, 'begin_time' => $begin_time, 'end_time' => $end_time, 'user_info' => $userInfo);
    }

    /**
     * 拉取货币兑换利率
     * @param $request_data
     * @return array
     */

    public function getExchangeRateList($request_data)
    {
        $rule = [
            'id' => 'string|filled',
            'name' => 'string|filled'
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        $request_data['id'] = $request_data['id'] ?? '';
        $request_data['name'] = $request_data['name'] ?? '';

        $userInfo = $this->getUserInfo();

        $info = $this->getExchangeRate($userInfo['user_id'], $request_data['id'], $request_data['name']);

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


    public function getExchangeRate($user_id, $id = '', $name = '')
    {
        $financeCurrencyList = Unique::getArray(FinanceCurrencyModel::selectRaw(
            "id ,custom_usd_exchang_rate AS usd_exchang_rate,
            custom_cad_exchang_rate as cad_exchang_rate,custom_mxn_exchang_rate as mxn_exchang_rate,custom_jpy_exchang_rate as jpy_exchang_rate,
            custom_gbp_exchang_rate as gbp_exchang_rate,custom_eur_exchang_rate as eur_exchang_rate,custom_au_exchang_rate as au_exchang_rate,
            custom_in_exchang_rate as in_exchang_rate,custom_br_exchang_rate as br_exchang_rate,
            custom_tr_exchang_rate as tr_exchang_rate,custom_ae_exchang_rate as ae_exchang_rate,custom_sa_exchang_rate as sa_exchang_rate,
            custom_nl_exchang_rate as nl_exchang_rate,custom_sg_exchang_rate as sg_exchang_rate,custom_hk_exchang_rate as hk_exchang_rate"
        )->where([['user_id', '=', $user_id]])->first());

        if (empty($financeCurrencyList)) {
            $SystemCurrencyList = Unique::getArray(SystemCurrencyModel::select(
                "id", "usd_exchang_rate", "cad_exchang_rate", "mxn_exchang_rate", "jpy_exchang_rate", "gbp_exchang_rate", "eur_exchang_rate", "au_exchang_rate", "in_exchang_rate", "br_exchang_rate",
                "tr_exchang_rate", "ae_exchang_rate", "nl_exchang_rate", "sa_exchang_rate", "sg_exchang_rate", "hk_exchang_rate"
            )->first());
            $result = $SystemCurrencyList;
        } else {
            $result = $financeCurrencyList;
        }

        $infos = array();
        $config = $this->config->get("common");

        $i = 0;
        foreach ($result as $key => $value) {
            if ($key == 'id') {
                continue;
            }

            if (isset($config['currency_list'][$key])) {
                $infos[$i] = $config['currency_list'][$key];
                $infos[$i]['exchang_rate'] = $value;
                $infos[$i]['modified_time'] = time();
            }
            $i++;
        }

        if ($id != '') {
            $tmp = array();
            $ids = explode(",", $id);
            foreach ($ids as $id) {
                foreach ($infos as $info_key => $info) {
                    if (intval($info['id']) == intval($id)) {
                        $tmp[$info_key] = $infos[$info_key];
                        continue;
                    }
                }
            }
            $infos = $tmp;
        }

        if ($name != '') {
            foreach ($infos as $info_key => $info) {
                if (strstr(strval($info['name']), strval($name)) == false) {
                    unset($infos[$info_key]);
                }
            }
        }

        return $infos;
    }


    /**
     * 拉取用户名以及openid
     * @param $request_data
     * @return array
     */

    public function getUserName($request_data)
    {
        $rule = [
            'client_id' => 'required|string',
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        $userInfo = $this->getUserInfo();

        $userAdmin = UserAdminModel::query()->where('id', $userInfo['admin_id'])->select('username')->first();

        $UserExtInfo = UserExtInfoModel::query()->where(array('admin_id' => $userInfo['admin_id'], 'client_id' => $request_data['client_id']))->first();

        if (!$UserExtInfo) {
            $data = [
                'code' => 0,
                'msg' => trans('auth.no_authorized'),
            ];
            return $data;
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'username' => $userAdmin->username,
                'uuid' => $UserExtInfo->uuid
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
        $rule = [
            'ext_info' => 'required|json',
            'uuid' => 'required|integer'
        ];

        $res = $this->validate($request_data, $rule);
        if ($res['code'] == 0) {
            return $res;
        }

        $userInfo = $this->getUserInfo();

        $UserExtInfoQuery = UserExtInfoModel::query()->where(array('uuid' => $request_data['uuid']));

        $UserExtInfo = $UserExtInfoQuery->first();

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

        return ['code' => 1, 'msg' => 'success', 'data' => array()];
    }


    public function syncOp($request_data)
    {
        //验证
        $rule = [
            'id' => 'required|integer',
            'synchronously_status' => 'required|integer',
            'synchronously_info' => 'required|string',
            'synchronously_time' => 'required|integer'
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        $where = [
            'id' => $request_data['id'],
        ];

        $synchronouslyManagementTask = Unique::getArray(SynchronouslyManagementTaskModel::query()->where($where)->select(array("user_id", "admin_id", "mmouth", "myear"))->first());

        if (empty($synchronouslyManagementTask)) {
            $data = [
                'code' => 0,
                'msg' => trans('auth.no_sync_id'),
            ];
            return $data;
        }

        try {
            SynchronouslyManagementTaskModel::query()->where($where)->update(
                [
                    'synchronously_status' => $request_data['synchronously_status'],
                    'synchronously_info' => $request_data['synchronously_info'],
                    'synchronously_time' => $request_data['synchronously_time']
                ]);

            if ($request_data['synchronously_status'] == 3) {
                $siteMessageData = array(
                    "user_id" => $synchronouslyManagementTask['user_id'],
                    "user_admin_id" => $synchronouslyManagementTask['admin_id'],
                    "message_type" => 33,
                    "notice_type" => 3,
                    "message_title" => "数据同步提醒",
                    "send_time" => time() + env("OPEN_TEST_TIME", 0),
                    "message_content" => json_encode(array(
                        "title" => "数据同步提醒",
                        "content" => array(
                            array(
                                "key" => "同步内容",
                                "value" => $synchronouslyManagementTask["myear"] . "-" . $synchronouslyManagementTask["mmouth"] . "期财务数据",
                            ),
                            array(
                                "key" => "接收方",
                                "value" => "金蝶云星辰",
                            ),
                            array(
                                "key" => "状态",
                                "value" => "同步成功",
                            ),
                        ),
                        "remark" => "请前往金蝶云星辰系统内及时查收。",
                        "url" => "/amzcaptain-authorized-authorizationManagement.html"
                    )));
            } else {
                $siteMessageData = array(
                    "user_id" => $synchronouslyManagementTask['user_id'],
                    "user_admin_id" => $synchronouslyManagementTask['admin_id'],
                    "message_type" => 33,
                    "notice_type" => 3,
                    "message_title" => "数据同步提醒",
                    "send_time" => time() + env("OPEN_TEST_TIME", 0),
                    "message_content" => json_encode(array(
                        "title" => "数据同步提醒",
                        "content" => array(
                            array(
                                "key" => "同步内容",
                                "value" => $synchronouslyManagementTask["myear"] . "-" . $synchronouslyManagementTask["mmouth"] . "期财务数据",
                            ),
                            array(
                                "key" => "接收方",
                                "value" => "金蝶云星辰",
                            ),
                            array(
                                "key" => "状态",
                                "value" => "同步失败",
                            ),
                        ),
                        "remark" => "请前往船长-应用授权管理页面处理。",
                        "url" => "/amzcaptain-authorized-authorizationManagement.html"
                    )));
            }
            SiteMessageModel::create($siteMessageData);
        } catch (\Exception $e) {
            $data = [
                'code' => 0,
                'msg' => trans('common.error'),
            ];
            Log::getClient()->error($e->getMessage());
            return $data;
        }

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => []
        ];

        return $data;
    }

    public function syncOps($request_data)
    {
        //验证
        $rule = [
            'ids' => 'required|string',
            'uuid' => 'required|string',
        ];

        $res = $this->validate($request_data, $rule);

        if ($res['code'] == 0) {
            return $res;
        }

        $ids = explode(",", trim($request_data['ids'], ","));

        Db::beginTransaction();

        foreach ($ids as $id) {
            try {
                $where = [
                    'id' => $id,
                ];

                $synchronouslyManagementTask = SynchronouslyManagementTaskModel::query()->where($where)->select(array("mmouth", "myear"))->first()->toArray();

                if (empty($synchronouslyManagementTask)) {
                    throw new \Exception(trans('auth.no_sync_id'));
                }

                $Host = env("OPEN_PLATFROM_URL");

                $httpClient = (new ClientFactory(ApplicationContext::getContainer()))->create();

                $begin_time = date('Y-m-01', strtotime($synchronouslyManagementTask['myear'] . "-" . $synchronouslyManagementTask['mmouth']));
                $end_time = date('Y-m-d', strtotime("$begin_time +1 month -1 day"));

                $params = array(
                    'bill_type' => 27,
                    'bill_start_time' => $begin_time,
                    'bill_end_time' => $end_time,
                    'kh_uid' => $request_data['uuid'],
                    'id' => $id
                );

                $resp = $httpClient->post($Host . "/yxc/sync/syncVirtualBill", ['form_params' => $params]);

                if ($resp->getStatusCode() == 200) {
                    $rawResp = (string)$resp->getBody();

                    $resp = @json_decode($rawResp, true);

                    if ($resp['code'] != 1) {
                        throw new \Exception($resp['msg']);
                    }
                } else {
                    throw new \Exception(trans('auth.send_error'));
                }

                $synchronouslyManagementTaskRes = SynchronouslyManagementTaskModel::query()->where($where)->update(['synchronously_status' => 1, 'synchronously_callback_time' => time() + env("OPEN_TEST_TIME", 0) + env("OPEN_CALLBACK_TIME", 600)]);

                if (!$synchronouslyManagementTaskRes) {
                    throw new \Exception(trans('common.error'));
                }

            } catch (\Exception $e) {
                $data = [
                    'code' => 0,
                    'msg' => $e->getMessage(),
                ];
                Db::rollBack();
                return $data;
            }
        }

        Db::commit();

        $data = [
            'code' => 1,
            'msg' => 'success',
            'data' => []
        ];

        return $data;
    }
}
