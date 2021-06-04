<?php


namespace Captainbi\Hyperf\Util;

use Captainbi\Hyperf\Model\AccessLoggerModel;
use Captainbi\Hyperf\Model\ExecuteLoggerModel;
use Hyperf\DbConnection\Db;
use mysql_xdevapi\Exception;

class Logger
{
    /**
     * 记录访问日志
     * @param $admin_id
     * @param $user_id
     * @param $access_url
     * @param $query_string
     * @param $http_header
     * @param $http_method
     * @param $http_params
     * @return int
     */

    public static function access_log($admin_id, $user_id, $access_url, $query_string, $http_header, $http_method, $http_params)
    {
        $access_logger_model =  AccessLoggerModel::create(
            array(
                'admin_id' => $admin_id,
                'user_id' => $user_id,
                'access_url' => $access_url,
                'query_string' => $query_string,
                'http_header' => $http_header,
                'http_method' => $http_method,
                'http_params' => $http_params
            ));

        return $access_logger_model->id;
    }


    /**
     * 记录sql日志
     * @param $admin_id
     * @param $user_id
     * @param $execute_sql
     * @param $access_logger_id
     * @return int
     */

    public static function execute_log($admin_id, $user_id, $execute_sql)
    {
        $execute_logger_model =  ExecuteLoggerModel::create(
            array(
                'admin_id' => $admin_id,
                'user_id' => $user_id,
                'execute_sql' => $execute_sql
            ));
        return  $execute_logger_model->id;
    }
}