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

use App\Model\CrontabModel;
use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Auth;
use Captainbi\Hyperf\Util\Log;
use Captainbi\Hyperf\Util\Unique;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class CommonService extends BaseService {
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
     * @param $request_data
     * @return array
     */
    public function getResult($request_data, $header){
        //验证
        $rule = [
            'id' => 'required|string'
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }

        if(!isset($header['authorization'][0])){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_authorization'),
            ];
        }
        $authorization = $header['authorization'][0];

        //jwt配置
        $jwtConfig = $this->config->get("auth.jwt");
        $keyName = $jwtConfig['key'];
        $auth = Auth::jwtDecode($authorization, $keyName);
        if(!$auth){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_authorization'),
            ];
        }
        $consumerId = $auth['consumer_id'];



        //获取数据
        //需要redis优化
        $where = [
            ['consumer_id', '=', $consumerId],
            ['snow_flake_id', '=', $request_data['id']],
        ];
        //获取url
        $crontabModel = CrontabModel::query()->where($where)->select("result", "is_success")->first();
        if(!$crontabModel){
            return [
                'code' => 0,
                'msg'  => trans('common.no_id'),
            ];
        }

        switch ($crontabModel['is_success'])
        {
            case 2:
                break;
            case 4:
                return [
                    'code' => 0,
                    'msg'  => trans('common.crontab_error'),
                ];
                break;
            default:
                return [
                    'code' => 0,
                    'msg'  => trans('common.crontab_running'),
                ];
                break;
        }

        if(!$crontabModel['result']){
            return [
                'code' => 0,
                'msg'  => trans('common.crontab_running'),
            ];
        }

        $data =  [
            'code' => 1,
            'msg'  => 'success',
            'data' => [
                'url' => $crontabModel['result'],
            ],
        ];

        return $data;
    }

    /**
     * function getCurrencyBySiteId
     * desc: 通过site_id 获取 货币符号
     * author: LWZ
     * editTime: 2021-05-16 18:20
     */
    public function getCurrencyBySiteId($site_id = 0){
        $amzon_site = \App\getAmazonSitesConfig() ;
        if(!empty($amzon_site[$site_id])){
            return $amzon_site[$site_id]['currency_code'] ;
        }else{
            return 'USD';
        }
    }

    /**
     * function currencyExchange
     * desc: 货币转换函数
     * param int $val 转换金额
     * param string $from_currency_code  原货币符号
     * param string $to_currency_code  目标货币符号
     * param string $rate_info  汇率信息 ， 通过getCurrencyInfo() 获得
     * return float|int  转换结果
     * author: LWZ
     * editTime: 2019-08-23 15:57
     */
    public function currencyExchange($val = 0 , $from_currency_code = 'USD' , $to_currency_code = 'CNY',$rate_info = array()){
        if($from_currency_code == $to_currency_code){
            return $val ;
        }else if($val == 0){
            return 0 ;
        }else{
            //先转换成人民币
            if($from_currency_code == 'CNY'){
                $val_cn = $val;
            }else{
                $rate1 = $rate_info[$from_currency_code];
                if(empty($rate1)){
                    $rate1 = 1;
                }
                $val_cn = round(($val/$rate1),2);
            }
            if($to_currency_code == 'CNY'){
                return $val_cn;
            }else{
                $rate2 = $rate_info[$to_currency_code];
                if(empty($rate2)){
                    $rate2 = 1;
                }
                $val_rt = round($rate2*$val_cn , 2);
                return $val_rt;
            }
        }
    }


}
