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

class Result
{
    /**
     * @param $code
     * @param $msg
     * @param array $data
     * @return mixed
     */
    private static function data($code, $msg, $data = [])
    {
        return json_encode(array(
            'code' => $code ? $code : 0,
            'msg' => $msg ? $msg : 'success',
            'data' => $data
        ));
    }

    /**
     * 成功
     * @param array $data
     * @param string $msg
     * @return mixed
     */
    public static function success($data = [], $msg = 'success')
    {
        return self::data(200, $msg, $data);
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

}