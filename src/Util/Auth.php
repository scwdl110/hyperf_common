<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


class Auth {

    /**
     * base64urlencode
     * @param string $string
     * @return mixed|string|string[]
     */
    public static function base64UrlEncode(string $string){
        $data = base64_encode($string);
        $data = str_replace(array('+','/','='),array('-','_',''),$data);
        return $data;
    }

    /**
     * base64urldecode
     * @param string $string
     * @return mixed|string|string[]
     */
    public static function base64UrlDecode(string $string){
        $data = str_replace(array('-','_'),array('+','/'),$string);
        $mod4 = strlen($data) % 4;
        if ($mod4) {
            $data .= substr('====', $mod4);
        }
        return base64_decode($data);
    }

}