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


}
