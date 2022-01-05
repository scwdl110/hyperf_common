<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

class BIApi
{
    public const ACTION_SEND_SMS = 'send_sms';

    protected static $url = '';

    protected static $apiKey = '';

    protected static $apiSecret = '';

    public function __construct()
    {
        if ('' === self::$url) {
            $config = config('biapi', []);
            if (!isset($config['url'], $config['api_key'], $config['api_secret'])) {
                throw new \RuntimeException('BI API 参数未配置');
            }

            if (!is_string($config['url']) || false ===  filter_var($config['url'], FILTER_VALIDATE_URL)) {
                throw new \RuntimeException("BI API url 参数格式不正确");
            }

            if (!is_string($config['api_key']) || 1 !== preg_match('/^[0-9a-z]{32}$/i', $config['api_key'])) {
                throw new \RuntimeException("BI API api_key 参数格式不正确");
            }

            if (!is_string($config['api_secret']) || 1 !== preg_match('/^[0-9a-z]{32}$/i', $config['api_secret'])) {
                throw new \RuntimeException("BI API api_secret 参数格式不正确");
            }

            self::$url = rtrim($config['url'], '/');
            self::$apiKey = $config['api_key'];
            self::$apiSecret = $config['api_secret'];
        }
    }

    public function request(string $action, array $param)
    {
        $param['api_key'] = self::$apiKey;
        $param['timestamp'] = time();
        $param['nonce_str'] = substr(md5(microtime()), 0, 16);
        $param['api_signkey'] = self::sign(self::$apiSecret, $param);

        $httpClient = (new \Hyperf\Guzzle\ClientFactory(\Hyperf\Utils\ApplicationContext::getContainer()))->create();
        return $httpClient->post(self::$url . "&a={$action}", ['form_params' => $param]);
    }

    public static function sign(string $secret, array $param): string
    {
        unset($param['m'], $param['c'], $param['a'], $param['api_signkey']);
        ksort($param);

        $str = '';
        foreach ($param as $k => $v) {
            $str .= "{$k}={$v}&";
        }

        return strtoupper(md5("{$str}api_secret={$secret}"));
    }
}
