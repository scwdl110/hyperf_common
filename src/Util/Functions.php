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
            if(isset($v[$column]) && isset($newArray[$v[$column]])){
                $newArray[$v[$column]] = $v;
            }
        }
        return $newArray;
    }

}