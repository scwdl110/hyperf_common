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

use Captainbi\Hyperf\Util\Auth;
use Captainbi\Hyperf\Util\Pgsql;
use Firebase\JWT\JWT;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class AuthService extends BaseService {
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
    public function jwtToken($request_data){
        //验证
        $rule = [
            'custom_id' => 'required|string'
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }

        //kong配置
        //后续优化加redis
        $kongConfig = $this->config->get("pgsql.kong");
        $kong = new Pgsql();
        $client = $kong->getClient($kongConfig);
        $client->prepare("my_query", "select ws_id,id,key,secret,algorithm from  jwt_secrets where id = $1");
        $res = $client->execute("my_query", array($request_data['custom_id']));
        if(!$res){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_jwt'),
            ];
        }
        $jwtSecrets = $client->fetchAssoc($res);
        if(!$jwtSecrets){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_jwt'),
            ];
        }


        //jwt配置
        $jwtConfig = $this->config->get("auth.jwt");
        $key = $jwtSecrets['secret'];
        $payload = array(
            $jwtConfig['key'] => $jwtSecrets['key'],
            "exp"             => time()+$jwtConfig['max_exp'],
            "ws_id"           => $jwtSecrets['ws_id'],
        );

        $jwt = Auth::jwtEncode($payload, $key, $jwtSecrets['algorithm']);

        $data =  [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'access_token' => $jwt
            ],
        ];

        return $data;
    }

}
