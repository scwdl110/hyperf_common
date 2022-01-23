<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Hyperf\Snowflake\IdGeneratorInterface;
use Hyperf\Utils\ApplicationContext;

class Unique {

    /**
     * 返回雪花id
     * @return string
     */
    public static function snowflake()
    {
        $container = ApplicationContext::getContainer();
        $generator = $container->get(IdGeneratorInterface::class);

        $snowflakeId = (string)$generator->generate();
        return $snowflakeId;
    }

    /***
     * 数据库获取数组
     * @param $items
     * @return array
     */

    public static function getArray($items)
    {
        if (empty($items)) {
            return array();
        } else {
            $data_arr = $items->toArray();
            $arr = array();
            foreach ($data_arr as &$value) {
                if (is_object($value)) {
                    $_arr = get_object_vars($value);
                    foreach ($_arr as $key => $val) {
                        $val = (is_array($val)) || is_object($val) ? object_to_array($val) : $val;
                        $arr[$key] = $val;
                    }
                    $value = $arr;
                }
            }
            return $data_arr;
        }
    }


    /**
     * 获取当前时间（可设置）
     * @return false|int
     */

    public static function getSetOrCurrentTime()
    {
        if(env("OPEN_TEST_TIME") != null){
            return strtotime(env("OPEN_TEST_TIME"));
        } else{
            return time();
        }
    }

}