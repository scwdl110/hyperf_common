<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use Firebase\JWT\JWT;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;

class Auth {

    /**
     * base64urlencode
     * @param string $string
     * @return mixed|string|string[]
     */
    public static function base64UrlEncode(string $string){
        $data = base64_encode($string);
        $data = str_replace(array('+','/','='),array('-','_',''),$data);
        return $data;
    }

    /**
     * base64urldecode
     * @param string $string
     * @return mixed|string|string[]
     */
    public static function base64UrlDecode(string $string){
        $data = str_replace(array('-','_'),array('+','/'),$string);
        $mod4 = strlen($data) % 4;
        if ($mod4) {
            $data .= substr('====', $mod4);
        }
        return base64_decode($data);
    }

    /**
     * @param array $payload
     * @param string $key
     * @param string $alg
     * @return string
     */
    public static function jwtEncode(array $payload, string $key, string $alg = 'HS256'){
        $jwt = JWT::encode($payload, $key, $alg);
        $jwt = "Bearer ".$jwt;
        return $jwt;
    }

    /**
     * @param string $authorization
     * @param string $keyName
     * @return array|bool
     */
    public static function jwtDecode(string $authorization, string $keyName){
        $data = [];
//        $jwt = trim(str_replace('Bearer', '', $authorization));
        //权限验证
        $authorizationArr = preg_split('/[\s\.]+/is', $authorization);
        if(!isset($authorizationArr[1]) || !isset($authorizationArr[2])){
            return false;
        }

        //获取key
        $authorization = static::base64UrlDecode($authorizationArr[2]);
        $authorizationJsonArr = json_decode($authorization,true);
        if(!isset($authorizationJsonArr[$keyName])){
            return false;
        }
        $data['key'] = $authorizationJsonArr[$keyName];
        $data['ws_id'] = $authorizationJsonArr['ws_id'];

        //获取加密方式
        $authorization = Auth::base64UrlDecode($authorizationArr[1]);
        $authorizationJsonArr = json_decode($authorization,true);
        if(!isset($authorizationJsonArr['alg'])){
            return false;
        }
        $data['alg'] = $authorizationJsonArr['alg'];

        //kong配置
        //后续优化加redis
        $configInterface = ApplicationContext::getContainer()->get(ConfigInterface::class);
        $kongConfig = $configInterface->get("pgsql.kong");
        $kong = new Pgsql();
        $client = $kong->getClient($kongConfig);
        $client->prepare("my_query", "select consumer_id,secret from jwt_secrets where ws_id = $1 and key= $2");
        $res = $client->execute("my_query", array($data['ws_id'], $data['key']));
        if(!$res){
            return false;
        }
        $jwtSecrets = $client->fetchAssoc($res);
        if(!$jwtSecrets){
            return false;
        }

        $data['consumer_id'] = $jwtSecrets['consumer_id'];
        $data['secret'] = $jwtSecrets['secret'];

        return $data;
    }

}