<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Warning;


use Captainbi\Hyperf\Util\Http;
use Captainbi\Hyperf\Util\Log;

class DingDing {
    /**
     * @param array $dingding
     * @param string $params
     * @return bool
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public static function send(array $dingding, string $params){
        if(!isset($dingding['token']) || !isset($dingding['secret']) || !isset($dingding['url'])){
            Log::getCrontabClient()->error('dingding:no params');
            return false;
        }
        $uri = "/robot/send?access_token=".$dingding['token'];
        //毫秒
        $timestamp = time()*1000;
        $sign = urlencode(base64_encode(hash_hmac('sha256', $timestamp."\n".$dingding['secret'], $dingding['secret'], true)));
        $uri .= "&timestamp={$timestamp}&sign={$sign}";

        $http = new Http();
        $httpClient = $http->getClient($dingding['url']);

        $httpResponse = $httpClient->post($uri,['form_params' => $params])->getBody()->getContents();
        $httpResponse = json_decode($httpResponse, true);
        if(isset($httpResponse['errcode']) && $httpResponse['errcode']==0){
            //ok
            return true;
        }else{
            return false;
        }
    }

}