<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


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
     * 获取服务器ip
     * @return string
     */
    public static function getInternalIp(): string
    {
        $ips = swoole_get_local_ip();
        if (is_array($ips) && ! empty($ips)) {
            return current($ips);
        }
        /** @var mixed|string $ip */
        $ip = gethostbyname(gethostname());
        if (is_string($ip)) {
            return $ip;
        }
        throw new \RuntimeException('Can not get the internal IP.');
    }
}
