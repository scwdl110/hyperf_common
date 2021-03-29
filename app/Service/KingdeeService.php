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

use App\Model\ChannelProfitReportModel;
use App\Service\BaseService;
use App\Model\FinanceReportModel;

use Hyperf\Utils\Context;
use Hyperf\Utils\Coroutine;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Psr\Http\Message\ServerRequestInterface;


class KingdeeService extends BaseService
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

    public function getKingDeeInfo($request_data)
    {
        $rule = [
            'date' => 'required|date',
            'user_id' => 'integer|filled',
        ];

        $res = $this->validate($request_data, $rule);
        if ($res['code'] == 0) {
            return $res;
        }

        $info = array();

        $beigin_time = $request_data['date'] . ' 00:00:00';
        $end_time = $request_data['date'] . ' 23:59:59';

        $FinanceReportInfo = FinanceReportModel::selectRaw("
        ifnull(sum(format( sales_quota * ( reserved_field11 / sales_volume + group_id ), 2 )),0) AS fba_sales_quota,           
        ifnull(sum(sales_quota) - sum(format( sales_quota * ( reserved_field11 / sales_volume + group_id ), 2 )),0) as fbm_sales_quota,
        sum(promote_discount),
	    sum(cpc_sb_cost),
	    sum(platform_sales_commission),	
	    
        ")->where([
            ['user_id', '=', $request_data['user_id']],
            ['create_time', '>=', strtotime($beigin_time)],
            ['create_time', '<=', strtotime($end_time)]
        ])->get()->toArray();
        var_dump($FinanceReportInfo);

        $info['commodity_sales']['fba_sales_quota'] = $FinanceReportInfo[0]['fba_sales_quota'];
        $info['commodity_sales']['fbm_sales_quota'] = $FinanceReportInfo[0]['fbm_sales_quota'];


        $ChannelProfitReportInfo = ChannelProfitReportModel::selectRaw("
        sum(coupon_redemption_fee + coupon_payment_eventList_tax) as coupon,
        sum(run_lightning_deal_fee) as  run_lightning_deal_fee
        ")->where([
            ['user_id', '=', $request_data['user_id']],
            ['create_time', '>=', strtotime($beigin_time)],
            ['create_time', '<=', strtotime($end_time)]
        ])->get()->toArray();

        $info['promotion_fee']['promote_discount'] = $FinanceReportInfo[0]['promote_discount'];
        $info['promotion_fee']['cpc_sb_cost'] = $FinanceReportInfo[0]['cpc_sb_cost'];
        $info['promotion_fee']['coupon'] = $ChannelProfitReportInfo[0]['coupon'];
        $info['promotion_fee']['run_lightning_deal_fee'] = $ChannelProfitReportInfo[0]['run_lightning_deal_fee'];





        var_dump($ChannelProfitReportInfo);









    }
}
