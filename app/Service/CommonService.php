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
        $amzon_site = array(
            1 => array("name" => '美国', "country_key" => "g_usa", "currency_code" => "USD", "currency_symbol" => "$", "code" => "US", "area" => "NorthAmerica", "MarketplaceId" => "ATVPDKIKX0DER", "siteUrl" => "https://mws.amazonservices.com", "website" => "https://www.amazon.com","site_group_id" => 1 , "volume_unit"=> "ft³"),
            2 => array("name" => '加拿大', "country_key" => "g_canada", "currency_code" => "CAD", "currency_symbol" => "C$", "code" => "CA", "is_sale" => "NorthAmerica", "MarketplaceId" => "A2EUQ1WTGCTBG2", "siteUrl" => "https://mws.amazonservices.ca", "website" => "http://www.amazon.ca" ,"site_group_id" => 1 , "volume_unit"=> "m³"),
            3 => array("name" => '墨西哥', "country_key" => "g_mexico", "currency_code" => "MXN", "currency_symbol" => "Mex$", "code" => "MX", "is_sale" => "NorthAmerica", "MarketplaceId" => "A1AM78C64UM0Y8", "siteUrl" => "https://mws.amazonservices.com", "website" => "http://www.amazon.com.mx","site_group_id" => 1 , "volume_unit"=> "dm ³"),
            4 => array("name" => '德国', "country_key" => "g_germany", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "DE", "is_sale" => "Europe", "MarketplaceId" => "A1PA6795UKMFR9", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.de" ,"site_group_id" => 2 , "volume_unit"=> "m³"),
            5 => array("name" => '西班牙', "country_key" => "g_spain", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "ES", "is_sale" => "Europe", "MarketplaceId" => "A1RKKUPIHCS9HS", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.es" ,"site_group_id" => 2 , "volume_unit"=> "m³"),
            6 => array("name" => '法国', "country_key" => "g_france", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "FR", "is_sale" => "Europe", "MarketplaceId" => "A13V1IB3VIYZZH", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.fr","site_group_id" => 2 , "volume_unit"=> "m³"),
            7 => array("name" => '印度', "country_key" => "g_india", "currency_code" => "INR", "currency_symbol" => "₹", "code" => "IN", "is_sale" => "Europe", "MarketplaceId" => "A21TJRUUN4KGV", "siteUrl" => "https://mws.amazonservices.in", "website" => "http://www.amazon.in","site_group_id" => 2 , "volume_unit"=> "0"),
            8 => array("name" => '意大利', "country_key" => "g_italy", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "IT", "is_sale" => "Europe", "MarketplaceId" => "APJ6JRA9NG5V4", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.it","site_group_id" => 2 , "volume_unit"=> "m³"),
            9 => array("name" => '英国', "country_key" => "g_england", "currency_code" => "GBP", "currency_symbol" => "£", "code" => "UK", "is_sale" => "Europe", "MarketplaceId" => "A1F83G8C2ARO7P", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.co.uk"  ,"site_group_id" => 2 , "volume_unit"=> "ft³"),
            10 => array("name" => '中国', "country_key" => "g_china", "currency_code" => "CNY", "currency_symbol" => "￥", "code" => "CN", "is_sale" => "China", "MarketplaceId" => "AAHKV2X7AFYLW", "siteUrl" => "https://mws.amazonservices.com.cn", "website" => "http://www.amazon.cn" ,"site_group_id" => 3, "volume_unit"=> ""),
            11 => array("name" => '日本', "country_key" => "g_japan", "currency_code" => "JPY", "currency_symbol" => "¥", "code" => "JP", "is_sale" => "Japan", "MarketplaceId" => "A1VC38T7YXB528", "siteUrl" => "https://mws.amazonservices.jp", "website" => "http://www.amazon.co.jp","site_group_id" => 4, "volume_unit"=> "cm³"),
            12 => array("name" => '澳大利亚', "country_key" => "g_australia", "currency_code" => "AUD", "currency_symbol" => "A$", "code" => "AU", "is_sale" => "Australia", "MarketplaceId" => "A39IBJ37TRP1C6", "siteUrl" => "https://mws.amazonservices.com.au", "website" => "http://www.amazon.com.au" ,"site_group_id" => 5, "volume_unit"=> "m³"),
            13 => array("name" => '巴西', "country_key" => "g_brazil", "currency_code" => "BRL", "currency_symbol" => "R$", "code" => "BR", "is_sale" => "NorthAmerica", "MarketplaceId" => "A2Q3Y263D00KWC", "siteUrl" => "https://mws.amazonservices.com", "website" => "http://www.amazon.com.br/" ,"site_group_id" => 1 , "volume_unit"=> "m³"),
            14 => array("name" => '土耳其', "country_key" => "g_turkey", "currency_code" => "TRY", "currency_symbol" => "₺", "code" => "TR", "is_sale" => "Europe", "MarketplaceId" => "A33AVAJ2PDY3EV", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.com.tr/","site_group_id" => 2 , "volume_unit"=> "m³"),
            15 => array("name" => '阿拉伯联合酋长国' ,  "country_key" => "g_arabic", "currency_code" => "AED", "currency_symbol" => "AED", "code" => "AE", "is_sale" => "Europe", "MarketplaceId" => "A2VIGQ35RCS4UG", "siteUrl" => "https://mws.amazonservices.ae", "website" => "http://www.amazon.ae/" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
            16 => array("name" =>  '荷兰',"country_key"=>"g_netherlands", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "NL", "is_sale" => "Europe", "MarketplaceId" => "A1805IZSGTT6HS", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "https://www.amazon.nl" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
            17 => array("name" =>  '沙特阿拉伯',"country_key"=>"g_arabia", "currency_code" => "SAR", "currency_symbol" => "SAR", "code" => "SA", "is_sale" => "Europe", "MarketplaceId" => "A17E79C6D8DWNP", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "https://www.amazon.sa" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
            18 => array("name" =>  '新加坡',"country_key"=>"g_singapore", "currency_code" => "SGD", "currency_symbol" => "S$", "code" => "SG", "is_sale" => "Singapore", "MarketplaceId" => "A19VAU5U5O7RUS", "siteUrl" => "https://mws-fe.amazonservices.com", "website" => "https://www.amazon.sg","site_group_id" => 6, "volume_unit"=> "m³") ,
        ) ;

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
