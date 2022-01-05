<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * 输出结果的封装
 * 只要get不要set,进行更好的封装
 * @param <T>
 */

namespace Captainbi\Hyperf\Util;

use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\Context;
use Psr\Http\Message\ServerRequestInterface;

class OpenResult
{
    /**
     * @param $code
     * @param $msg
     * @param array $data
     * @param $next_token
     * @param $max_result
     * @return string
     */
    private static function data($code, $msg, $data = [], $next_token = '')
    {
        $code = $code ?: 0;
        $msg = $msg ?: 'success';

        //定制化只支持开放平台log
        $request = ApplicationContext::getContainer()->get(RequestInterface::class);
        $path = $request->getUri()->getPath();
        $context = Context::get(ServerRequestInterface::class);
        $userInfo = $context->getAttribute('userInfo');
        $time = time();
        $insertData = [
            'user_id' => $userInfo['user_id'],
            'client_id' => $userInfo['client_id'],
            'channel_id' => $userInfo['channel_id'],
            'path' => $path,
            'code' => $code,
            'msg' => $msg,
            'create_time' => $time,
            'modified_time' => $time,
        ];
        Db::connection("erp_report")->table("open_api_log")->insert($insertData);


        $is_open_next_token = ApplicationContext::getContainer()->get(ConfigInterface::class)->get('common.is_open_next_token');
        if ($is_open_next_token) {
            return json_encode(array(
                'code' => $code,
                'msg' => $msg,
                'next_token' => $next_token ?: '',
                'data' => (object)$data,
            ), JSON_UNESCAPED_UNICODE);
        } else {
            return json_encode(array(
                'code' => $code,
                'msg' => $msg,
                'data' => (object)$data,
            ), JSON_UNESCAPED_UNICODE);
        }
    }

    /**
     * 成功
     * @param array $data
     * @param string $msg
     * @param string $next_token
     * @param int $max_result
     * @return mixed
     */
    public static function success($data = [], $msg = 'success', $next_token = '')
    {
        return self::data(200, $msg, $data, $next_token);
    }

    /**
     * 失败
     * @param array $data
     * @param string $msg
     * @param int $code
     * @return mixed
     */
    public static function fail($data = [], $msg = 'error', $code = -1)
    {
        return self::data($code, $msg, $data);
    }

    /**
     * fail 的别名，一般错误都不需要 data 参数
     *
     * @param string $msg
     * @param int $code
     * @param array $data
     * @return string
     */
    public static function error(string $msg = 'error', int $code = -1, array $data = []): string
    {
        return self::data($code, $msg, $data);
    }
}
