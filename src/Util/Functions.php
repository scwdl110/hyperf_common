<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use Hyperf\DbConnection\Db;
use Hyperf\Redis\RedisFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\Context;
use Psr\Http\Message\ServerRequestInterface;

class Functions {

    /**
     * 二维数组转化为某个字段为键值的数组
     * @param array $array
     * @param string $column
     * @return array|bool
     */
    public static function arrayToFieldArray(array $array, string $column)
    {
        if(!$array){
            return false;
        }
        $newArray = [];
        foreach ($array as $k=>$v){
            if(isset($v[$column])){
                $newArray[$v[$column]] = $v;
            }
        }
        return $newArray;
    }

    /**
     * 删除不要的数据
     * @param $request_data
     * @param $keys
     * @return bool|array
     */
    public function unsetData($request_data,$keys){
        if(!is_array($request_data) || !is_array($keys)){
            return false;
        }
        foreach ($request_data as $k=>$v){
            if(!in_array($k,$keys)){
                unset($request_data[$k]);
            }
        }
        return $request_data;
    }


    /**
     * 获取mac地址
     * @return string|null
     */
    public static function getMacAddress(): ?string
    {
        $macAddresses = swoole_get_local_mac();

        foreach ($macAddresses as $name => $address) {
            if ($address && $address !== '00:00:00:00:00:00') {
                return $name . ':' . str_replace(':', '', $address);
            }
        }

        return null;
    }

    /**
     * 发送短信
     *
     * @param string $message
     * @param string $mobile
     * @return ?array 失败返回 null，成功返回数组（仅代表请求短信服务商成功，不代表成功发送）
     */
    public static function sendSms(string $message, string $mobile): ?array
    {
        if ('' === ($url = config('sms.url', ''))) {
            return null;
        }

        $mobile = self::mobileDecrypt($mobile);
        $postFields = [
            'account' => config('sms.account', ''),
            'password' => config('sms.password', ''),
            'msg' => urlencode($message),
            'phone' => $mobile,
            'report' => 'true'
        ];

        $httpClient = (new \Hyperf\Guzzle\ClientFactory(\Hyperf\Utils\ApplicationContext::getContainer()))->create();
        $httpResponse = $httpClient->post($url, [
            'json' => $postFields,
            'headers' => [
                'Accept' => 'application/json'
            ]
        ]);
        return @json_decode((string)$httpResponse->getBody(), true);
    }

    /**
     * 是否手机号
     *
     * @param int|string $mobile
     * @return bool
     */
    public static function isMobile($mobile): bool
    {
        if (!is_numeric($mobile) && !is_string($mobile)) {
            return false;
        }

        $mobile = trim((string)$mobile);
        if (1 === preg_match('/^1\d{10}$/', $mobile)) {
            return true;
        }

        // 国际号码
        return 1 === preg_match('/^\d{5,20}$/', str_replace(['+', '-', ' '], '', $mobile));
    }

    /**
     * 将 手机号 进行 AES 加密
     *
     * @param string $mobile 要加密的手机号
     * @return string 加密并 base64 encode 后的字符串
     */
    public static function mobileEncrypt($mobile): string
    {
        if (!$mobile || (!is_string($mobile) && !is_numeric($mobile))) {
            return '';
        }

        $config = config('encrypt.mobile', []);
        if (empty($config)) {
            return '';
        }

        $encrypt = \base64_encode(\openssl_encrypt(
            (string)$mobile,
            $config['method'],
            $config['key'],
            $config['padding'],
            $config['iv']
        ));
        return $encrypt ? $encrypt : '';
    }

    /**
     * 解密手机号
     *
     * @param string $base64Encrypt base64 encode 的手机号密文
     * @param bool   $desensitization 是否对解密后的手机号进行脱敏
     * @return string 手机号明文或脱敏后的手机号
     */
    public static function mobileDecrypt(string $base64Encrypt, bool $desensitization = false): string
    {
        $base64Encrypt = trim($base64Encrypt);
        if ('' === $base64Encrypt) {
            return '';
        }

        if (static::isMobile($base64Encrypt)) {
            // 未加密的手机号
            $mobile = $base64Encrypt;
        } else {
            $config = config('encrypt.mobile', []);
            if (empty($config)) {
                return '';
            }

            $mobile = \openssl_decrypt(
                \base64_decode($base64Encrypt),
                $config['method'],
                $config['key'],
                $config['padding'],
                $config['iv']
            );
        }

        if ($mobile) {
            if ($desensitization) {
                $mobile = self::mobileDesensitization($mobile);
            }

            return $mobile;
        }

        return '';
    }

    /**
     * 手机号脱敏
     *
     * @param string $mobile
     * @return string
     */
    public static function mobileDesensitization(string $mobile): string
    {
        $len = strlen($mobile);
        if ($len >= 11) {
            return substr_replace($mobile, '****', -8, 4);
        } else {
            $len = (int)ceil($len / 3);
            $offset = $len * -2;
            return substr_replace($mobile, str_repeat('*', $len), $offset, $len);
        }
    }

    /**
     * 开放平台aes加密
     * @param string $token
     * @param string $aesKey
     * @return string
     */
    public static function encryOpen(string $token, string $aesKey)
    {
        return base64_encode(openssl_encrypt($token, 'AES-128-ECB', $aesKey, OPENSSL_RAW_DATA));
    }

    /**
     * 开放平台aes解密
     * @param string $encryptionToken
     * @param string $aesKey
     * @return false|string
     */
    public static function decryOpen(string $encryptionToken, string $aesKey)
    {
        return openssl_decrypt(base64_decode($encryptionToken), 'AES-128-ECB', $aesKey, OPENSSL_RAW_DATA);
    }


    /**
     * @param int $channelId
     * @param int $force
     * @param array $field
     * @return array|bool|\Hyperf\Database\Model\Model|\Hyperf\Database\Query\Builder|object|null
     */
    public static function getChannel(int $channelId, int $force = 0, array $field = []){
        if(!$channelId){
            return false;
        }

        $defaultField = [
            "site_id",
            "Merchant_ID",
            "title",
            "user_id"
        ];
        if(!$field){
            $field = $defaultField;
        }else{
            $field = array_merge($field, $defaultField);
        }

        $key = 'center_open_channel_'.$channelId;
        $poolName = 'default';
        $redis = ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);
        $channel = $redis->get($key);
        $flag = 1;
        //无缓存或者键值不在缓存里面
        if($channel===false || $force){
            $flag = 0;
        }else{
            $channel = json_decode($channel, true);
            if(!is_array($channel)){
                $flag = 0;
            }else{
                foreach ($field as $v){
                    if(!array_key_exists($v, $channel)){
                        $flag = 0;
                        break;
                    }
                }
            }
        }


        if(!$flag){
            $where = [
                ['id', '=', $channelId],
                ['status', '=', 1]
            ];
            $channel = Db::connection("erp_base")->table('channel')->where($where)->select($field)->first();
            if(!$channel){
                return false;
            }
            $siteId = data_get($channel, 'site_id', 0);
            if($siteId){
                $where = [
                    'site_id' => $siteId
                ];
                $area = Db::connection("erp_base")->table("site_area")->where($where)->select("area_id")->first();
                $areaId = data_get($area, 'area_id', 0);
            }else{
                $areaId = 0;
            }

            $channel = get_object_vars($channel);
            $channel['area_id'] = $areaId;

            $redis->set($key, json_encode($channel), 3600);
        }

        return $channel;
    }

    /**
     * @param $clientId
     * @param int $force
     * @return array|bool|mixed|string
     */
    public static function getOpenClient($clientId, int $force = 0){
        $key = 'center_open_client_type_'.$clientId;
        $poolName = 'default';
        $redis = ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);
        $clientType = $redis->get($key);
        if($clientType===false || $force){
            $where = [
                ['client_id', '=', $clientId],
            ];
            $client = Db::table('open_client')->where($where)->select('client_type')->first();

            if(!$client){
                return false;
            }
            $clientType = data_get($client, 'client_type', '');

            $redis->set($key, $clientType, 86400);
        }
        return $clientType;
    }


    /**
     * @param $clientId
     * @param int $force
     * @return bool|mixed
     */
    public static function getOpenSelfClientUser($clientId, int $force = 0){
        $key = 'center_open_self_user_'.$clientId;
        $poolName = 'default';
        $redis = ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);
        $openUser = $redis->get($key);
        if($openUser===false || $force){
            $where = [
                ['client_id', '=', $clientId],
                ['is_delete', '=', 0],
            ];
            $clientUser = Db::table('open_client_user')->where($where)->select('user_id', 'is_disable')->first();
            if(!$clientUser){
                return false;
            }
            $userId = data_get($clientUser, 'user_id', 0);
            $isDisable = data_get($clientUser, 'is_disable', 0);
            $openUser = [
                'user_id' => $userId,
                'is_disable' => $isDisable,
            ];
            $redis->set($key, json_encode($openUser), 3600);
        }else{
            $openUser = json_decode($openUser, true);
        }

        return $openUser;
    }


    /**
     * @param $clientId
     * @param $userId
     * @param int $force
     * @return bool|mixed
     */
    public static function getOpenClientUserChannel($clientId, $userId, int $force = 0){
        $key = 'center_open_client_user_channel_'.$clientId."_".$userId;
        $poolName = 'default';
        $redis = ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);
        $openClientUserChannel = $redis->get($key);
        if($openClientUserChannel===false || $force){
            $where = [
                ['client_id', '=', $clientId],
                ['user_id', '=', $userId],
            ];
            $arr = Db::table('open_client_user_channel')->where($where)->select("channel_id")->get();
            if(!$arr){
                return false;
            }

            $openClientUserChannel = [];
            foreach ($arr as $k=>$v){
                $channelId = data_get($v, 'channel_id', '');
                if($channelId){
                    $channel = Functions::getChannel(intval($channelId));
                    if($channel){
                        $openClientUserChannel[] = $channelId;
                    }
                }

            }
            $redis->set($key, json_encode($openClientUserChannel), 3600);
        }else{
            $openClientUserChannel = json_decode($openClientUserChannel, true);
        }

        return $openClientUserChannel;
    }


    /**
     * @param $userId
     * @param int $force
     * @return bool|mixed|string
     */
    public static function getOpenPhone($userId, int $force = 0){
        $key = 'center_open_phone_'.$userId;
        $redis = new Redis();
        $redis = $redis->getClient();
        $phone = $redis->get($key);
        if($phone===false || $force){
            $where = [
                ['id', '=', $userId],
            ];
            $phone = Db::connection("erp_base")->table("user")->where($where)->select('mobile')->first();
            $phone = data_get($phone, 'mobile', '');
            $redis->set($key, $phone, 3600);
        }
        return $phone;
    }

}
