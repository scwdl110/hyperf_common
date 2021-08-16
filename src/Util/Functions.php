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


    

}